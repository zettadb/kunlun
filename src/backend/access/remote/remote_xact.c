/*-------------------------------------------------------------------------
 *
 * remote_xact.c
 *      send transaction commands to remote storage nodes.
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/remote/remote_xact.c
 *
 *
 * INTERFACE ROUTINES
 * NOTES
 *	  This file contains the routines which implement
 *	  the POSTGRES remote access method used for remotely stored POSTGRES
 *	  relations.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/remote_xact.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "utils/memutils.h"
#include "sharding/sharding.h"
#include "sharding/sharding_conn.h"
#include "utils/builtins.h"
#include "sharding/cluster_meta.h"
#include "utils/snapmgr.h"
#include "storage/shmem.h"
#include "utils/catcache.h"
#include "utils/guc.h"
#include "miscadmin.h"
#include "executor/spi.h"
#include "storage/lwlock.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include <unistd.h>
#include <limits.h>
#include <time.h>

static char*get_storage_node_version(AsyncStmtInfo *asi);
static bool check_gdd_supported(AsyncStmtInfo *asi);
static void send_txn_cmd(enum enum_sql_command sqlcom, bool written_only, const char *fmt,...);
static void init_txn_cmd(void);
void downgrade_error(void);

// send 'savepoint xxx' to all accessed shards in current transaction.
char *g_txn_cmd = 0;
size_t g_txn_cmd_buflen = 256;

// GUI variables
bool trace_global_deadlock_detection = false;
int start_global_deadlock_detection_wait_timeout = 100;
bool enable_global_deadlock_detection = true;

inline static int gdd_log_level()
{
#ifdef ENABLE_DEBUG
	return trace_global_deadlock_detection ? LOG : DEBUG1;
#else
	return DEBUG2;
#endif
}

static void init_txn_cmd()
{
	if (!g_txn_cmd)
		g_txn_cmd = MemoryContextAlloc(TopMemoryContext, g_txn_cmd_buflen);
}

void StartSubTxnRemote(const char *name)
{
	Assert(name);
	send_txn_cmd(SQLCOM_SAVEPOINT, false, "SAVEPOINT %s", name);
}

void SendReleaseSavepointToRemote(const char *name)
{
	send_txn_cmd(SQLCOM_RELEASE_SAVEPOINT, false, "RELEASE SAVEPOINT %s", name);
}

void SendRollbackRemote(const char *txnid, bool xa_end, bool written_only)
{
	/*
	 * If top txn's name isn't set, no XA txn is started at any storage nodes,
	 * thus no need to execute an XA ROLLBACK.
	 * */
	if (!txnid)
		return;
	/*
	  Txn memcxt is already released, should not free stmts in case they are
	  allocated in txn memcxt, and they should be. Otherwise there would be
	  memory leaks here!
	*/
	CancelAllRemoteStmtsInQueue(false);

	int ihoc = InterruptHoldoffCount;
	PG_TRY();
	{
		if (xa_end)
			send_txn_cmd(SQLCOM_XA_ROLLBACK, written_only, "XA END '%s';XA ROLLBACK '%s'", txnid, txnid);
		else
			send_txn_cmd(SQLCOM_XA_ROLLBACK, written_only, "XA ROLLBACK '%s'", txnid);
	}
	PG_CATCH();
	{
		PG_TRY();
		{
		disconnect_storage_shards();
		request_topo_checks_used_shards();
		}
		PG_CATCH();
		{

		}
		PG_END_TRY();
	}
	PG_END_TRY();

	InterruptHoldoffCount = ihoc;
}

void SendRollbackSubToRemote(const char *name)
{
	send_txn_cmd(SQLCOM_ROLLBACK_TO_SAVEPOINT, false, "ROLLBACK TO %s", name);
}

static void send_txn_cmd(enum enum_sql_command esc, bool written_only,
	const char *fmt,...)
{
	init_txn_cmd();

	int len;
	va_list     args;

again:
	va_start(args, fmt);
	len = vsnprintf(g_txn_cmd, g_txn_cmd_buflen, fmt, args);
	va_end(args);

	if (len >= g_txn_cmd_buflen)
	{
		g_txn_cmd = repalloc(g_txn_cmd, g_txn_cmd_buflen *= 2);
		goto again;
	}

	/*
	  If conn already killed or broken, do not reconnect to send the txn
	  command because the txn in both computing node and the storage shards
	  is both already aborted, no need for such commands.
	  The only exception is when aborting a prepared XA txn where it would be
	  recovered after the connection was killed, and in this case the txn will
	  be aborted by cluster_mgr later.
	*/
	send_stmt_to_all_inuse(g_txn_cmd, len, CMD_TXN_MGMT, false, esc, written_only);
}

/*
 * Send 1st phase stmts to remote shards, return true if there is 2nd phase,
 * false if no 2nd phase needed.
 * */
bool Send1stPhaseRemote(const char *txnid)
{
	int num = GetAsyncStmtInfoUsed();
	int nr = 0, nw = 0, nddls = 0;
	if (num <= 0 || txnid == NULL)
		return false;

	static size_t slen = 0;
	static char *stmt = NULL;
	static char *stmt1 = NULL;
	static char *stmt2 = NULL;

	if (!stmt)
	{
		slen = 3 * (txnid ? strlen(txnid) : 0) + 64;
		stmt = MemoryContextAlloc(TopMemoryContext, slen);
		stmt1 = MemoryContextAlloc(TopMemoryContext, slen);
		stmt2 = MemoryContextAlloc(TopMemoryContext, slen);
	}
	else
	{
		// We need no more than this actually.
		Assert(slen >= 2*strlen(txnid) + 34);
	}

	/*
	 * This is a flag to let the allocated string belong to only one StmtElem,
	 * other StmtElem simply refer to it but don't own it. The stmts sent to
	 * all shards are the same so we don't want to alloc&fill it for every shard.
	 * */
	bool filled = false;

	int len = 0;
	bool abort_txn = false;

	for (int i = 0; i < num; i++)
	{
		AsyncStmtInfo *asi = GetAsyncStmtInfoByIndex(i);
		if (!ASIConnected(asi) || IsConnReset(asi))
		{
			abort_txn = true;
			continue;
		}

		if (asi->did_write && asi->txn_wrows > 0)
		{
			elog(DEBUG2, "Found written shard in transaction %s shard node (%u,%u) at %s:%u, %d rows written.",
				 txnid, asi->shard_id, asi->node_id, asi->conn->host, asi->conn->port, asi->txn_wrows);
			nw++;
		}
		if (asi->did_ddl)
			nddls++;
		if (ASIReadOnly(asi) || (ASIAccessed(asi) && asi->txn_wrows == 0))
		{
			if (!filled)
			{
				len = snprintf(stmt, slen, "XA END '%s';XA COMMIT '%s' ONE PHASE", txnid, txnid);
				Assert(len < slen);
			    filled = true;
			}
			/*
			  If we send an update to a shard but no maching rows updated, it's marked
			  written but txn_wrows is 0.
			*/
			elog(DEBUG2, "Found in transaction %s shard node (%u,%u) at %s:%u read only branch, doing 1pc to it.",
				 txnid, asi->shard_id, asi->node_id, asi->conn->host, asi->conn->port);
			// In MySQL, XA COMMIT  ... ONE PHASE is also a SQLCOM_XA_COMMIT.
			append_async_stmt(asi, stmt, len, CMD_TXN_MGMT, false, SQLCOM_XA_COMMIT);
			Assert(asi->result_pending == false);
			nr++;
		}

		if (ASIConnected(asi) && !ASIAccessed(asi))
		{
			/*
			  This happens when asi connection was found broken when stmts was
			  sent through the connection
			  and it's not an error, we can simply ignore the situation.
			*/
			ereport(LOG, (errcode(ERRCODE_INTERNAL_ERROR),
				    errmsg("A shard (%u) node (%u)'s connection not read or written in transaction %s is seen as used, probably because the connection was broken already.",
					asi->shard_id, asi->node_id, txnid)));
		}
	}

	if (nddls > 0)
	{
		if (nw > 0 || nr > 0 /*|| nddls > 1 it's possible that one DDL
			stmt takes actions on >1 tablets. MySQL XA doesn't support DDLs,
			for ACID properties of such a DDL stmt, we'd have to utilize the
			metadata DDL log.*/)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				    errmsg("State error for transaction %s: As required by MySQL, a DDL statement must be a single autocommit transaction, it should never be in an explicit transaction(nDDLs: %d, nWrittenShards: %d, nReadShards: %d).",
						  txnid, nddls, nw, nr)));
		}

		return false;
	}

	Assert(nw >= 0);
	enum enum_sql_command sqlcom;
	bool filled1 = false;
	bool filled2 = false;

	// store 2nd type of stmts into stmt2.
	for (int i = 0; i < num; i++)
	{
		AsyncStmtInfo *asi = GetAsyncStmtInfoByIndex(i);
		// skip broken channels here, nothing to do for them here.
		if (!ASIConnected(asi) || IsConnReset(asi)) continue;
		if (asi->did_write)
		{
			if (abort_txn)
			{
				if (!filled)
				{
			    	len = snprintf(stmt2, slen, "XA END '%s';XA ROLLBACK '%s'", txnid, txnid);
					filled = true;
				}
			    sqlcom = SQLCOM_XA_ROLLBACK;
				elog(DEBUG2, "Aborting transaction %s 's branch in shard node (%u,%u) at %s:%u",
				 	txnid, asi->shard_id, asi->node_id, asi->conn->host, asi->conn->port);
				append_async_stmt(asi, stmt2, len, CMD_TXN_MGMT, false, sqlcom);
			}
			else if (nw == 1 && ASIAccessed(asi) && asi->txn_wrows > 0)
			{
				if (filled1 == false)
				{
			    	len = snprintf(stmt1, slen, "XA END '%s';XA COMMIT '%s' ONE PHASE", txnid, txnid);
					filled1 = true;
				}
			    sqlcom = SQLCOM_XA_COMMIT;
				elog(DEBUG2, "Only 1 written shard found for transaction %s in shard node (%u,%u) at %s:%u, doing 1pc to it.",
				 	txnid, asi->shard_id, asi->node_id, asi->conn->host, asi->conn->port);
				append_async_stmt(asi, stmt1, len, CMD_TXN_MGMT, false, sqlcom);
			}
			else if (nw > 1 && asi->txn_wrows > 0)
			{
				if (filled2 == false)
				{
			    	len = snprintf(stmt2, slen, "XA END '%s';XA PREPARE '%s'", txnid, txnid);
					filled2 = true;
				}
			    sqlcom = SQLCOM_XA_PREPARE;
				elog(DEBUG2, "Found %d written shards for transaction %s, preparing in shard node (%u,%u) at %s:%u",
				 	nw, txnid, asi->shard_id, asi->node_id, asi->conn->host, asi->conn->port);
				append_async_stmt(asi, stmt2, len, CMD_TXN_MGMT, false, sqlcom);
			}

			Assert(len < slen);

			Assert(asi->result_pending == false);
		}
	}
	send_multi_stmts_to_multi();
	return nw > 1 && !abort_txn;
}

void Send2ndPhaseRemote(const char *txnid)
{
	int num = GetAsyncStmtInfoUsed();
	static size_t slen = 0;
	static char *stmt = NULL;
	int len = 0;
	bool filled = false;
	
	Assert(txnid && num > 1);

	if (!stmt)
	{
		slen = 2*strlen(txnid) + 32;
		stmt = MemoryContextAlloc(TopMemoryContext, slen);
	}
	else
	{
		// txnid strings are of similar lengths, make sure we allocated enough
		Assert(slen > strlen(txnid) + 14);
	}

	for (int i = 0; i < num; i++)
	{
		AsyncStmtInfo *asi = GetAsyncStmtInfoByIndex(i);
		/*
		  If any conn broken before this call, the txn is aborted. So when we
		  arrive here we definitely have all conns valid.
		 */
		Assert(ASIConnected(asi));

		if (asi->did_write && asi->txn_wrows > 0)
		{
			if (!filled)
			{
				len = snprintf(stmt, slen, "XA COMMIT '%s'", txnid);
				Assert(len < slen);
			    filled = true;
			}

			elog(DEBUG2, "For transaction %s doing 2pc commit in shard node (%u,%u) at %s:%u",
				 txnid, asi->shard_id, asi->node_id, asi->conn->host, asi->conn->port);
			append_async_stmt(asi, stmt, len, CMD_TXN_MGMT, false, SQLCOM_XA_COMMIT);
		}
	}

	send_multi_stmts_to_multi();
}

/*
  insert debug sync point to accessed storage shards certain debug_sync setting.
  
  where: so far always 1(before_execute_sql_command);
  what: so far always 1(wait)
  which: 1: all accessed shards; 2: all written shards
*/
void insert_debug_sync(int where, int what, int which)
{
#ifdef ENABLE_DEBUG_SYNC
	Assert(where == 1 && what == 1);
	Assert(which == 1 || which == 2);
	int num = GetAsyncStmtInfoUsed();
	char stmt[] = "set session debug_sync = 'before_execute_sql_command wait_for resume'";

	for (int i = 0; i < num; i++)
	{
		AsyncStmtInfo *asi = GetAsyncStmtInfoByIndex(i);
		if (asi->did_write || which == 1)
		{
			append_async_stmt(asi, stmt, sizeof(stmt) - 1, CMD_UTILITY, false,
				SQLCOM_SET_OPTION);
		}
	}
#endif
}

char *MakeTopTxnName(TransactionId txnid, time_t now)
{
	int idlen = 128, ret = 0;
	char *ptxnid = MemoryContextAlloc(TopTransactionContext, idlen);

again:
	ret = snprintf(ptxnid, idlen, "%u-%ld-%u", comp_node_id, now, txnid);
	if (ret >= idlen)
	{
		ptxnid = repalloc(ptxnid, idlen *= 2);
		goto again;
	}
	return ptxnid;
}


/****************************************************************************/


/*
 * Global deadlock detector.
 * Periodically perform deadlock detection, and can be activated by waits for
 * remote DML result.
 * Fetch each shard master's local txn wait-for relationships, to build a
 * global one.
 * */

int g_glob_txnmgr_deadlock_detector_victim_policy = KILL_MOST_ROWS_LOCKED;
static MemoryContext gdd_memcxt = NULL;
static MemoryContext gdd_stmts_memcxt = NULL;

/*
 * Let each backend request a round of global dd by sending gdd SIGUSR2.
 * they can do so if insert/update/delete stmts takes some time (N millisecs)
 * and not returned. N should be set less than lockwait timeout.
 * gdd can already respond to SIGUSR2 to start a round of gdd.
*/
typedef struct GDDState
{
	volatile sig_atomic_t num_reqs;
	pid_t gdd_pid;
	time_t when_last_gdd;
} GDDState;

static GDDState *g_gdd_state = NULL;

Size GDDShmemSize()
{ return sizeof(GDDState); }

void CreateGDDShmem()
{
	bool found = false;
	Size size = GDDShmemSize();
	g_gdd_state  = (GDDState*)ShmemInitStruct("Global deadlock detector state.", size, &found);

	if (!found)
	{
		/*
		 * We're the first - initialize.
		 */
		MemSet(g_gdd_state, 0, size);
	}
}

/*
  To be called only by the gdd process, the pid will be used by backend
  processes to notify the gdd to start a round of deadlock detection.
*/
void set_gdd_pid()
{
	g_gdd_state->gdd_pid = getpid();
}

void kick_start_gdd()
{
#ifdef ENABLE_DEBUG
	/*
	  This is called in SIGALRM handler, don't do IO or anything complex in
	  production build.
	*/
	elog(gdd_log_level(), "Waking up gdd after waiting %d ms for write results from shards.",
		 start_global_deadlock_detection_wait_timeout);
#endif
	if (g_gdd_state->gdd_pid != 0) kill(g_gdd_state->gdd_pid, SIGUSR2);
}

size_t increment_gdd_reqs()
{
	return ++g_gdd_state->num_reqs;
}

/*
 * This is only used to identify a global txn in deadlock detector code.
 * */
typedef struct GTxnId
{
	time_t ts;
	Oid compnodeid;
	TransactionId txnid;
} GTxnId;

typedef struct TxnBranchId
{
	GTxnId gtxnid;
	Shard_id_t shardid;
} TxnBranchId;

typedef struct GlobalTxn GlobalTxn;

typedef struct TxnBranch 
{
	TxnBranchId tbid;

	// This global txn's txn branch is currently waiting in this shard for lock.
	Shard_id_t waiting_shardid;
	// The target txn branch's connection id.
	uint32_t mysql_connid;
	GlobalTxn *owner;
	// whether this txn branch's query is chosen as victim by deadlock detector.
	bool killed_dd;
} TxnBranch;

typedef struct TxnBranchRef
{
	TxnBranchId txnid;
	TxnBranch *ptr;
} TxnBranchRef;

/*
 * Global txn wait-for graph node. Note that any global txn can be waiting
 * for at most one other global txn at any time.
 * */
typedef struct GlobalTxn
{
	GTxnId gtxnid;
	// min of all txn branches
	time_t start_ts;
	// In each round of sub-graph traverse, each visited graph node is given
	// this unique id to identify this round of traverse.
	uint64_t visit_id;
	// sum of all txn branches
	uint32_t nrows_changed;
	uint32_t nrows_locked;
	// NO. of txn branches killed by deadlock detector. if in a cycle there is such a
	// global txn whose txn branches killed most by DD when they were found in other cycles,
	// it's always chosen as the victim for this cycle, to minimize the NO. of 
	// global txns killed.
	uint32_t nbranches_killed;

	/*
	  NO. of txn branches blocked by txn branches of this global txn.
	*/
	uint32_t num_blocked;

	// The txn branches this global txn waits for, i.e. its branches wait for.
	// Given a TxnBranch tb1 in 'blockers' array, the TxnBranch object in 'branches'
	// array with the same shardid is the txn branch that's waiting for tb1.
	TxnBranch **blockers;
	uint32_t nblocker_slots;
	uint32_t nblockers;

	/*
	  This global txn's waiting txn branches in all shards accessed by the global txn.
	*/
	TxnBranch **branches;
	uint32_t nbranch_slots;
	uint32_t nbranches;
} GlobalTxn;

typedef struct GlobalTxnRef
{
	GTxnId gtxnid;
	GlobalTxn *ptr;
} GlobalTxnRef;

typedef struct GTxnBest
{
	GTxnDD_Victim_Policy policy;
	/*
	 * this is always the top priority.
	 * */
	uint32_t nbranches_killed;
	TxnBranch *most_branches_killed_gtxn;

	time_t min_start_ts;
	TxnBranch *min_start_gtxn;

	time_t max_start_ts;
	TxnBranch *max_start_gtxn;

	uint32_t min_nrows_changed;
	TxnBranch *min_nrows_chg_gtxn;

	uint32_t max_nrows_changed;
	TxnBranch *max_nrows_chg_gtxn;

	uint32_t max_nrows_locked;
	TxnBranch *max_nrows_locked_gtxn;

	/*
	  Kill the txn branch whose global txn has most waiting(blocked) txn branches.
	  Such a global can possibly be a bottleneck since it easy can be blocked
	  by other txns, if it is killed, many txns won't be blocked by it.
	*/
	uint32_t max_waiting_branches;
	TxnBranch *most_waiting_branches_gtxn;

	/*
	  Kill the txn branch whose global txn has most blocking txn branches, i.e.
	  it's blocking most NO. of other txns.
	*/
	uint32_t max_blocking_branches;
	TxnBranch *most_blocking_branches_gtxn;
} GTxnBest;

/*
 * Stack element. the stack stores alternative graph nodes to do DFS traverse
 * later. each batch of pushed PathNode share a best candidate found so far.
 * When the PathNode is poped, the best_so_far is used as best candidate in the traverse.
 * */
typedef struct PathNode {
	TxnBranch *blocker;
	GTxnBest *best_so_far;
	bool single_shard_cycle;
	Oid cur_shardid;
}PathNode;

typedef struct Stack
{
	PathNode *base;
	int top; // stack top element's index
	int capacity;
} Stack;

static void stack_push(Stack *stk, PathNode*pn)
{
	Assert(stk && pn);

	if (stk->base == NULL)
	{
		stk->top = 0;
		stk->capacity = 32;
		stk->base = (PathNode*)MemoryContextAllocZero(gdd_stmts_memcxt, sizeof(PathNode) * stk->capacity);
	}
	if (stk->top == stk->capacity)
	{
		Size stkspc = sizeof(PathNode) * stk->capacity;
		stk->base = repalloc(stk->base, stkspc * 2);
		memset(((char*)stk->base) + stkspc, 0, stkspc);
		stk->capacity *= 2;
	}
	stk->base[stk->top++] = *pn;
}

/*
 * pop top element to 'top'.
 * @retval true if there is top element, false if stack is empty.
 * */
static bool stack_pop(Stack *stk, PathNode *top)
{
	if (stk->top <= 0 || stk->base == NULL)
		return false;
	*top = stk->base[--stk->top];
	return true;
}

inline static void print_gtxnid(StringInfo buf, const GTxnId*gt)
{
	appendStringInfo(buf, "%u-%ld-%u", gt->compnodeid, gt->ts, gt->txnid);
}

static void InitBestCandidate(GTxnBest *best, GTxnDD_Victim_Policy policy)
{
	best->policy = policy;
	best->min_start_ts = 0xffffffff;
	best->max_start_ts = 0;
	best->min_nrows_changed = 0xffffffff;
	best->max_nrows_changed = 0;
	best->max_nrows_locked = 0;
	best->nbranches_killed = 0;

	best->most_branches_killed_gtxn = NULL;
	best->min_start_gtxn = best->max_start_gtxn = NULL;
	best->min_nrows_chg_gtxn = best->max_nrows_chg_gtxn = NULL;
	best->max_nrows_locked_gtxn = NULL;
}

// See if gn is a better choice than 'best' by 'vp' policy, if so note down gn.
static void UpdateCandidate(GTxnBest *best, TxnBranch *gn)
{
	if (best->nbranches_killed < gn->owner->nbranches_killed)
	{
		best->nbranches_killed = gn->owner->nbranches_killed;
		best->most_branches_killed_gtxn = gn;
	}

	switch (best->policy)
	{
	case KILL_OLDEST:
		if (best->min_start_ts > gn->owner->start_ts)
		{
			best->min_start_ts = gn->owner->start_ts;
			best->min_start_gtxn = gn;
		}
		break;
	case KILL_YOUNGEST:
		if (best->max_start_ts < gn->owner->start_ts)
		{
			best->max_start_ts = gn->owner->start_ts;
			best->max_start_gtxn = gn;
		}
		break;
	case KILL_MOST_ROWS_CHANGED:
		if (best->min_nrows_changed > gn->owner->nrows_changed)
		{
			best->min_nrows_changed = gn->owner->nrows_changed;
			best->min_nrows_chg_gtxn = gn;
		}
		break;
	case KILL_LEAST_ROWS_CHANGED:
		if (best->max_nrows_changed < gn->owner->nrows_changed)
		{
			best->max_nrows_changed = gn->owner->nrows_changed;
			best->max_nrows_chg_gtxn = gn;
		}
		break;
	case KILL_MOST_ROWS_LOCKED:
		if (best->max_nrows_changed < gn->owner->nrows_changed)
		{
			best->max_nrows_locked = gn->owner->nrows_locked;
			best->max_nrows_locked_gtxn = gn;
		}
		break;
	case KILL_MOST_WAITING_BRANCHES:
		if (best->max_waiting_branches < gn->owner->nblockers)
		{
			best->max_waiting_branches = gn->owner->nblockers;
			best->most_waiting_branches_gtxn = gn;
		}
		break;
	case KILL_MOST_BLOCKING_BRANCHES:
		if (best->max_blocking_branches < gn->owner->num_blocked)
		{
		    best->max_blocking_branches = gn->owner->num_blocked;
			best->most_blocking_branches_gtxn = gn;
		}
		break;
	default:
		// If g_gtxn_dd_victim_policy is modified (by client) while assigning
		// to 'vp' parameter, control could come here. We simply skip 'gn' for
		// candidate.
		best->policy = g_glob_txnmgr_deadlock_detector_victim_policy;
		break;
	}
}

static TxnBranch *GetGTDDVictim(GTxnBest *best)
{
	TxnBranch *gn = NULL;

	switch (best->policy)
	{
	case KILL_OLDEST:
		gn = best->min_start_gtxn;
		break;
	case KILL_YOUNGEST:
		gn = best->max_start_gtxn;
		break;
	case KILL_MOST_ROWS_CHANGED:
		gn = best->min_nrows_chg_gtxn;
		break;
	case KILL_LEAST_ROWS_CHANGED:
		gn = best->max_nrows_chg_gtxn;
		break;
	case KILL_MOST_ROWS_LOCKED:
		gn = best->max_nrows_locked_gtxn;
		break;
	case KILL_MOST_WAITING_BRANCHES:
		gn = best->most_waiting_branches_gtxn;
		break;
	case KILL_MOST_BLOCKING_BRANCHES:
		gn = best->most_blocking_branches_gtxn;
		break;
	default:
		gn = NULL;
		break;
	}
	/*
	 * Always kill the global txn whose most branches are killed, override any policy.
	 * */
	if (gn && gn->owner->nbranches_killed < best->nbranches_killed)
	{
		gn = best->most_branches_killed_gtxn;
	}
	return gn;
}

static HTAB *gtxn_waitgnodes = 0;
static HTAB *g_txn_branch_hashtbl = 0;

#define MAX_TXN_WAIT_GNODES 256
/*
 * if a global txn is used as victim, it should be used as victim in other cycles found.
 * */
typedef struct GlobalTxnSection
{
	struct GlobalTxnSection *next;
	int end;
	GlobalTxn nodes[MAX_TXN_WAIT_GNODES];
} GlobalTxnSection;

static GlobalTxnSection g_all_gnodes;

static GlobalTxn *Alloc_gtxn()
{
	GlobalTxn *ret = NULL;
	GlobalTxnSection *last_sect = NULL;
	for (GlobalTxnSection *sect = &g_all_gnodes; sect; sect = sect->next)
	{
		if (sect->end < MAX_TXN_WAIT_GNODES)
		{
			ret = sect->nodes + sect->end++;
			/*
			 * ret may hold alloced arrays, they can be reused.
			 * */
			GlobalTxn tmp = *ret;
			memset(ret, 0, sizeof(*ret));
			ret->blockers = tmp.blockers;
			ret->nblocker_slots = tmp.nblocker_slots;
			ret->branches = tmp.branches;
			ret->nbranch_slots = tmp.nbranch_slots;
			goto end;
		}
		last_sect = sect;
	}

	last_sect->next = MemoryContextAllocZero(gdd_memcxt, sizeof(GlobalTxnSection));
	last_sect = last_sect->next;
	ret = last_sect->nodes + last_sect->end++;
end:
	return ret;
}

#define MAX_TXN_BRANCHES 1024
typedef struct TxnBranchSection
{
	struct TxnBranchSection *next;
	int end;
	TxnBranch nodes[MAX_TXN_BRANCHES];
} TxnBranchSection;

static TxnBranchSection g_all_txn_branches;

/*
 * At start of each round of deadlock detect, we should first call this to
 * clear the data collected in previous round.
 * */
static void ResetDeadlockDetectorState()
{
	MemoryContextReset(gdd_stmts_memcxt);
	for (GlobalTxnSection *s = &g_all_gnodes; s; s = s->next)
	{
		s->end = 0;
	}

	hash_destroy(gtxn_waitgnodes);

	/*  
	 * create hashtable that indexes the gtxnid->
	 */
	HASHCTL     ctl;
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(GTxnId);
	ctl.entrysize = sizeof(GlobalTxnRef);
	gtxn_waitgnodes = hash_create("Global transaction wait-for graph node by global txn-id", 256,
				             &ctl, HASH_ELEM | HASH_BLOBS);

	for (TxnBranchSection *s = &g_all_txn_branches; s; s = s->next)
	{
		s->end = 0;
	}

	MemSet(&ctl, 0, sizeof(ctl));
	hash_destroy(g_txn_branch_hashtbl);
	ctl.keysize = sizeof(TxnBranchId);
	ctl.entrysize = sizeof(TxnBranchRef);
	g_txn_branch_hashtbl = hash_create("Global transaction wait-for graph txn branch nodes by txn-id", 1024,
				             &ctl, HASH_ELEM | HASH_BLOBS);
}


static TxnBranch *Alloc_txn_branch()
{
	TxnBranch *ret = NULL;
	TxnBranchSection *last_sect = NULL;
	for (TxnBranchSection *sect = &g_all_txn_branches; sect; sect = sect->next)
	{
		if (sect->end < MAX_TXN_BRANCHES)
		{
			ret = sect->nodes + sect->end++;
			memset(ret, 0, sizeof(*ret));
			goto end;
		}

		last_sect = sect;
	}

	last_sect->next = MemoryContextAllocZero(gdd_memcxt, sizeof(TxnBranchSection));
	last_sect = last_sect->next;
	ret = last_sect->nodes + last_sect->end++;
end:
	return ret;
}

static TxnBranch *FindTxnBranch(TxnBranchId *txnid)
{
	bool found = false;
	TxnBranchRef *tbref = hash_search(g_txn_branch_hashtbl, txnid, HASH_FIND, &found);
	if (!tbref)
	{
		Assert(!found);
		return NULL;
	}

	Assert(found && tbref->ptr);
	return tbref->ptr;
}

static GlobalTxn *FindGtxn(GTxnId *gtxnid)
{
	bool found = false;
	if (!gtxnid)
		return NULL;

	GlobalTxnRef *gnref = hash_search(gtxn_waitgnodes, gtxnid, HASH_FIND, &found);
	if (!gnref)
	{
		Assert(!found);
		return NULL;
	}

	Assert(found && gnref->ptr);
	return gnref->ptr;
}

/*
 * Add txn branch to its global txn owner.
 * If the global txn doesn't exist yet, add it first.
 * */
static TxnBranch *
add_global_txn_branch(GTxnId *gtxnid, Shard_id_t shardid, uint32_t mysql_connid,
				      time_t start_ts, uint32_t nrows_changed, uint32_t nrows_locked)
{
	GlobalTxn *gn = NULL;
	TxnBranchId txnid;
	Assert(gtxnid && shardid != 0 && mysql_connid != 0 && start_ts > 0 && nrows_changed > 0);

	gn = FindGtxn(gtxnid);
	if (gn)
		goto add_branch;

	gn = Alloc_gtxn();
	gn->gtxnid = *gtxnid;
	gn->nblockers = 0;
	gn->visit_id = 0;
	gn->start_ts = start_ts;
	gn->nrows_changed = 0; // will add nrows_changed below.
	gn->nrows_locked = 0;
	gn->nbranches_killed = 0;

	if (gn->blockers == NULL)
	{
		Assert(gn->nblocker_slots == 0 && gn->nblockers == 0);
		gn->nblocker_slots = 8;
		gn->blockers = MemoryContextAllocZero(gdd_memcxt, gn->nblocker_slots * sizeof(void*));
	}

	if (gn->branches == NULL)
	{
		Assert(gn->nbranch_slots == 0 && gn->nbranches == 0);
		gn->nbranch_slots = 8;
		gn->branches = MemoryContextAllocZero(gdd_memcxt, gn->nbranch_slots * sizeof(void*));
	}
	bool found = false;
	GlobalTxnRef *gnref = hash_search(gtxn_waitgnodes, gtxnid, HASH_ENTER, &found);
	Assert(!found && gnref);
	gnref->ptr = gn;

	elog(gdd_log_level(), "On shard(%u) found global txn (%u-%lu-%u)",
		 shardid, gtxnid->compnodeid, gtxnid->ts, gtxnid->txnid);
add_branch:

	memset(&txnid, 0, sizeof(txnid));
	txnid.gtxnid = *gtxnid;
	txnid.shardid = shardid;
	TxnBranch *tb = FindTxnBranch(&txnid);
	if (tb)
		return tb;

	if (gn->start_ts > start_ts)
		gn->start_ts = start_ts;
	gn->nrows_changed += nrows_changed;
	gn->nrows_locked += nrows_locked;

	tb = Alloc_txn_branch();
	tb->owner = gn;
	tb->waiting_shardid = shardid;
	tb->mysql_connid = mysql_connid;
	tb->killed_dd = false;

	TxnBranchRef *tbref = hash_search(g_txn_branch_hashtbl, &txnid, HASH_ENTER, &found);
	Assert(!found && tbref);
	tbref->ptr = tb;
	tb->tbid = txnid;

	/*
	 * Add new branch 'tb' into gn->branches array, expand the array if no enough space.
	 * */
	if (gn->nbranches == gn->nbranch_slots)
	{
		gn->branches = repalloc(gn->branches, gn->nbranch_slots * 2 * sizeof(void*));
		memset(gn->branches + gn->nbranch_slots, 0,
			   gn->nbranch_slots * sizeof(void*));
		gn->nbranch_slots *= 2;
	}

	gn->branches[gn->nbranches++] = tb;

	elog(gdd_log_level(), "On shard(%u) found txn branch(%d) of global txn (%u-%lu-%u)",
		 shardid, gn->nbranches, gtxnid->compnodeid, gtxnid->ts, gtxnid->txnid);
	return tb;
}

static void MakeWaitFor(GlobalTxn *waiter, TxnBranch *blocker)
{
	if (waiter->nblockers == waiter->nblocker_slots)
	{
		waiter->blockers = repalloc(waiter->blockers, waiter->nblocker_slots * 2 * sizeof(void*));
		memset(waiter->blockers + waiter->nblocker_slots, 0,
			   waiter->nblocker_slots * sizeof(void*));
		waiter->nblocker_slots *= 2;
	}
	blocker->owner->num_blocked++;
	waiter->blockers[waiter->nblockers++] = blocker;
}

static inline void set_gtxnid(const char *gtxnid_str, GTxnId *gtxnid)
{
	sscanf(gtxnid_str, "'%u-%lu-%u'", &gtxnid->compnodeid, &gtxnid->ts, &gtxnid->txnid);
}

void gdd_init()
{
	/*
	 * Initialization of needed modules and objects.
	 *
	 * make sure cache memory context exists
	 */
	if (!CacheMemoryContext)
		CreateCacheMemoryContext();

	if (!gdd_memcxt)
		gdd_memcxt =
			AllocSetContextCreate(TopMemoryContext,
				                  "Global Deadlock Detector Memory Context",
				                  ALLOCSET_DEFAULT_SIZES);
	if (!gdd_stmts_memcxt)
		gdd_stmts_memcxt = 
			AllocSetContextCreate(gdd_memcxt,
				                  "Global Deadlock Detector Memory Context for SQL Statements",
				                  ALLOCSET_DEFAULT_SIZES);
	ShardCacheInit();
	InitShardingSession();
}


/*
 * @retval true if need to traverse the wait-for graph for deadlocks and resolve them if any;
 *         false if no such need because there is only one shard used.
 * */
static bool build_wait_for_graph()
{
	static const char *query_wait = 
	 "SELECT waiter.trx_xid as waiter_xa_id, waiter.trx_mysql_thread_id as waiter_conn_id,"
	 "unix_timestamp(waiter.trx_started) as waiter_start_ts,"
	 "waiter.trx_rows_modified as waiter_nrows_changed, waiter.trx_rows_locked as waiter_nrows_locked,"
	 "blocker.trx_xid as blocker_xa_id, blocker.trx_mysql_thread_id as blocker_conn_id,"
	 "unix_timestamp(blocker.trx_started) as blocker_start_ts,"
	 "blocker.trx_rows_modified as blocker_nrows_changed, blocker.trx_rows_locked as blocker_nrows_locked "
	 "FROM performance_schema.data_lock_waits as lock_info JOIN "
	 "     information_schema.innodb_trx as waiter JOIN "
	 "     information_schema.innodb_trx as blocker "
	 "ON lock_info.REQUESTING_ENGINE_TRANSACTION_ID = waiter.trx_id AND "
	 "   lock_info.BLOCKING_ENGINE_TRANSACTION_ID = blocker.trx_id AND "
	 "   waiter.trx_xa_type = 'external' and blocker.trx_xa_type = 'external' AND "
	 "	waiter.trx_state='LOCK WAIT' AND (blocker.trx_state='RUNNING' OR blocker.trx_state='LOCK WAIT')";
	static int qlen_wait = 0;
	if (qlen_wait == 0) qlen_wait = strlen(query_wait);

	ResetCommunicationHub();
	
	List *pshards = GetAllShardIds();
	size_t nshards = list_length(pshards);
	if (nshards == 1) {
		return false;
	}

	/*
	 * build the wait-for graph.
	 * */
	int shardidx = 0;
	bool gdd_supported = true;
	ListCell *lc;

	foreach(lc, pshards)
	{
		AsyncStmtInfo *asi = NULL;
		Oid shardid = lfirst_oid(lc);

		PG_TRY(); {
			asi = GetAsyncStmtInfo(shardid);
			if (shardidx == 0 && !(gdd_supported = check_gdd_supported(asi)))
				break;
			shardidx++;
		} PG_CATCH(); {
			/*
			  Get rid of the exception, log it to server log only to free
			  error stack space.
			  Do not abort the current pg txn, keep running in it. it's not pg
			  error that the mysql node can't be reached.
			 */
			HOLD_INTERRUPTS();
			//EmitErrorReport();
			//AbortOutOfAnyTransaction();
			downgrade_error();
			errfinish(0);
			FlushErrorState();
			RESUME_INTERRUPTS();
			elog(DEBUG1, "GDD: Skipping shard(%u) when building wait-for graph because its master isn't available for now.", shardid);
		} PG_END_TRY();

		if (asi)
			append_async_stmt(asi, query_wait, qlen_wait, CMD_SELECT, false, SQLCOM_SELECT);
		else
			// This shard has no known master node, so it can't be written
			// anyway and it's safe to skip it for now.
			nshards--;
	}
	if (!gdd_supported)
		return false;

	enable_remote_timeout();
	send_multi_stmts_to_multi();

	size_t num_asis = GetAsyncStmtInfoUsed();
	//Assert(num_asis == nshards); this could fail if a shard suddenly has no master.
	/*
	 * Receive results and build graph nodes.
	 * */
	for (size_t i = 0; i < num_asis; i++)
	{
		CHECK_FOR_INTERRUPTS();
		
		AsyncStmtInfo *asi = GetAsyncStmtInfoByIndex(i);

		// If shard master unavailable, handle this shard in next round.
		if (!ASIConnected(asi)) continue;

		MYSQL_RES *mres = asi->mysql_res;
		GTxnId gtxnid;
		TxnBranchId txnid_blocker;
		memset(&txnid_blocker, 0, sizeof(txnid_blocker));

		if (mres == NULL)
		{
			elog(DEBUG1, "GDD: NULL results of wait-for relationship from shard.node(%u.%u).",
				 asi->shard_id, asi->node_id);
			free_mysql_result(asi);
			continue;
		}
		do {
			MYSQL_ROW row = mysql_fetch_row(mres);
			if (row == NULL)
			{
				check_mysql_fetch_row_status(asi);
				free_mysql_result(asi);
				break;
			}

			if (row[0] == NULL || row[1] == NULL || row[2] == NULL ||
				row[3] == NULL || row[4] == NULL ||
				row[5] == NULL || row[6] == NULL || row[7] == NULL ||
				row[8] == NULL || row[9] == NULL)
			{
				elog(WARNING, "GDD: Invalid rows fetched from i_s.innodb_trx of shard.node(%u.%u): NULL field(s) in NOT NULL column(s).",
					 asi->shard_id, asi->node_id);
				free_mysql_result(asi);
				break;
			}

			// store waiter info
			set_gtxnid(row[0], &gtxnid);
			char *endptr = NULL;
			/*
			 * Although the i_s.innodb_trx defines its connection_id column
			 * as bigint unsigned, in MySQL the my_thread_id type is uint32_t,
			 * so conn_id is in (0, UINT_MAX) for sure.
			 * */
			uint64_t conn_id = strtoull(row[1], &endptr, 10);
			uint64_t start_ts = strtoull(row[2], &endptr, 10);
			uint64_t nrows_locked = strtoull(row[3], &endptr, 10);
			uint64_t nrows_changed = strtoull(row[4], &endptr, 10);
			Assert(conn_id <= UINT_MAX && start_ts <= UINT_MAX &&
				   nrows_locked <= UINT_MAX && nrows_changed <= UINT_MAX);
			add_global_txn_branch(&gtxnid, asi->shard_id, conn_id, start_ts, nrows_changed, nrows_locked);

			// store blocker info
			set_gtxnid(row[5], &txnid_blocker.gtxnid);
			/*
			 * Although the i_s.innodb_trx defines its connection_id column
			 * as bigint unsigned, in MySQL the my_thread_id type is uint32_t,
			 * so conn_id is in (0, UINT_MAX) for sure.
			 * */
			conn_id = strtoull(row[6], &endptr, 10);
			start_ts = strtoull(row[7], &endptr, 10);
			nrows_locked = strtoull(row[8], &endptr, 10);
			nrows_changed = strtoull(row[9], &endptr, 10);
			Assert(conn_id <= UINT_MAX && start_ts <= UINT_MAX &&
				   nrows_locked <= UINT_MAX && nrows_changed <= UINT_MAX);
			add_global_txn_branch(&txnid_blocker.gtxnid, asi->shard_id, conn_id, start_ts, nrows_changed, nrows_locked);
			txnid_blocker.shardid = asi->shard_id;
			/*
			 * We have to fetch active xa txns before fetching wait-for relationship,
			 * otherwise we don't have global txns to set up the wait-for relationship.
			 * */
			GlobalTxn *gt_waiter = FindGtxn(&gtxnid);
			TxnBranch *tb_blocker = FindTxnBranch(&txnid_blocker);
			/*
			 * local txn branch wait-for can only happen on the same shard.
			 * */
			if (gt_waiter && tb_blocker)
			{
				elog(gdd_log_level(), "On shard(%u) found (%u-%lu-%u) waiting for (%u-%lu-%u)",
					txnid_blocker.shardid, gtxnid.compnodeid, gtxnid.ts, gtxnid.txnid,
					txnid_blocker.gtxnid.compnodeid, txnid_blocker.gtxnid.ts, txnid_blocker.gtxnid.txnid);
				MakeWaitFor(gt_waiter, tb_blocker);
			}
		} while (true);
	}

	disable_remote_timeout();

	return true/*GDD supported*/;
}


/* 
 * kill 'best' choice according to user set standards. Kill all branches
 * at once since a global txn have to be aborted when any of its branch is
 * aborted.
 */
static void kill_victim(GTxnBest *best)
{
	TxnBranch *tb = GetGTDDVictim(best);
	if (!tb) return;

	GlobalTxn *gt = tb->owner;
	StringInfoData str;
	initStringInfo2(&str, 256, gdd_stmts_memcxt);
	appendStringInfoString(&str, "Global deadlock detector found a deadlock and killed the victim(gtxnid: ");
	print_gtxnid(&str, &gt->gtxnid);
	appendStringInfoString(&str, "). Killed txn branches (shardid, connection-id):");
	/*
	 * TODO: Some of the txn branches may have finished the query, i.e. there is no
	 * blocked query to kill.
	 * */
	for (int i = 0; i < gt->nbranches; i++)
	{
		TxnBranch *gn = gt->branches[i];
		AsyncStmtInfo *asi = GetAsyncStmtInfo(gn->waiting_shardid);
		size_t stmtlen = 64;
		char *stmt = MemoryContextAlloc(gdd_stmts_memcxt, stmtlen);

		Assert(gn->mysql_connid != 0);
		int slen = snprintf(stmt, stmtlen, "kill query %d", gn->mysql_connid);
		Assert(slen < stmtlen);
		append_async_stmt(asi, stmt, slen, CMD_UTILITY, false, SQLCOM_KILL);
		gn->killed_dd = true;
		appendStringInfo(&str, " (%u, %u)", gn->waiting_shardid, gn->mysql_connid);
	}

	gt->nbranches_killed = gt->nbranches;

	elog(LOG, "Global deadlock detector: %s", str.data);
}


inline static uint64 GRF_TRVS_MASK(uint32 isect, uint32 i)
{	
	uint64 ret = isect;
	ret <<= 32;
	ret |= (i+1);
	return ret;
}

static void find_and_resolve_global_deadlock()
{
	uint32 isect = 0;
	GTxnDD_Victim_Policy victim_policy = g_glob_txnmgr_deadlock_detector_victim_policy;
	Stack stk;
	memset(&stk, 0, sizeof(stk));

	for (GlobalTxnSection *gs = &g_all_gnodes; gs; gs = gs->next, isect++)
	{
		Assert(isect < UINT_MAX && gs->end < UINT_MAX);

		for (int i = 0; i < gs->end; i++)
		{
			CHECK_FOR_INTERRUPTS();

			Shard_id_t cur_shardid = Invalid_shard_id;
			bool single_shard_cycle = true;
			GlobalTxn *gn = gs->nodes + i;
			GTxnBest best, *pbest = NULL;
			InitBestCandidate(&best, victim_policy);
			StringInfoData gddlog;
			initStringInfo2(&gddlog, 1024, gdd_stmts_memcxt);
			pbest = &best;

			/*
			 * One round of traverse, starting from gn. All nodes accessible
			 * will all be visited if they are not yet so. Since the
			 * wait-for graph is a directed graph, part of the nodes may not
			 * be accessible in this round of traverse, and they will be visited in
			 * next round with a different starting point 'gn'.
			 * */
			do {
				PathNode pnode;
				bool hasit = false;

				CHECK_FOR_INTERRUPTS();

				/*
				 * all wait-for cycles having a gn node that was killed already,
				 * are no longer cyclic, such deadlocks are all resolved, so
				 * stop traversing.
				 * */
				if (gn->nbranches_killed > 0)
				    goto dfs_next;

				if (gn->visit_id == 0)
				{
				    /*
				     * Consider whether any of the txn branches gn waits for
				     * should be killed, i.e. to visit it.
				     * */
				    gn->visit_id = GRF_TRVS_MASK(isect, i);
// LOG level is special, it's high for elog/ereport, but has a low number.
#define SHOULD_LOG (log_min_messages <= gdd_log_level() || gdd_log_level() == LOG)
					if (SHOULD_LOG)
						appendStringInfo(&gddlog, "gdd visit (%lu) gtxn(%u-%ld-%u) %s", gn->visit_id,
							gn->gtxnid.compnodeid, gn->gtxnid.ts, gn->gtxnid.txnid,
							gn->nblockers > 1 ? "push alt paths to stack:": "");

				    /* 
					 * Push the rest blockers if any to stack for DFS traverse later.
					 * dup the best candidate found so far for the blockers, we
					 * will use the cached best
					 * when we pop a blocker out to resume our search.
					 */
					GTxnBest *pbest_dup = NULL;
					if (gn->nblockers > 1)
					{
						pbest_dup = (GTxnBest *)MemoryContextAllocZero(gdd_stmts_memcxt, sizeof(GTxnBest));
						*pbest_dup = *pbest;
					}

				    for (int i = 1; i < gn->nblockers; i++)
				    {
						PathNode pnd;
						pnd.blocker = gn->blockers[i];
						Assert(pnd.blocker);
						pnd.best_so_far = pbest_dup;
						pnd.cur_shardid = cur_shardid;
						pnd.single_shard_cycle = single_shard_cycle;

						stack_push(&stk, &pnd);

						if (SHOULD_LOG)
							appendStringInfo(&gddlog, "(%u, %u) of (%u-%ld-%u); ",
											pnd.blocker->waiting_shardid,
											pnd.blocker->mysql_connid,
											pnd.blocker->owner->gtxnid.compnodeid,
											pnd.blocker->owner->gtxnid.ts,
											pnd.blocker->owner->gtxnid.txnid);
				    }

					elog(gdd_log_level(), "%s", gddlog.data);
					resetStringInfo(&gddlog);

				    if (gn->nblockers > 0)
				    {
				        UpdateCandidate(pbest, gn->blockers[0]);
						if (cur_shardid == Invalid_shard_id)
							cur_shardid = gn->blockers[0]->tbid.shardid;
						else if (cur_shardid != gn->blockers[0]->tbid.shardid)
							single_shard_cycle = false;
				        gn = gn->blockers[0]->owner;
				        continue;
				    }
				    // else pop nodes in stack to continue the DFS traverse.
				}
				else if (gn->visit_id == GRF_TRVS_MASK(isect, i))
				{
				    /* 
				     * A cycle is found, if it's not in a single shard node,
					 * kill the chosen victim gn. note that gn can be a global
				     * txn started by another computing node, so we have no stats
				     * like 'startup time' or 'NO. of rows changed' here,
				     * thus do such a simple kill.
				     */
					elog(gdd_log_level(), "gdd visit (%lu) : at gtxn(%u-%ld-%u) found a %s wait-for cycle%s.",
						 gn->visit_id, gn->gtxnid.compnodeid, gn->gtxnid.ts, gn->gtxnid.txnid,
						 single_shard_cycle ? "single shard" : "",
						 single_shard_cycle ? ", it is left for local shard node to resolve" : ", victim will be killed");
					if (!single_shard_cycle)
				    	kill_victim(pbest);
				    //goto dfs_next;
				}
				else
				{
				    //goto dfs_next;
				}
dfs_next:

				hasit = stack_pop(&stk, &pnode);
				if (!hasit)
				    break;
				TxnBranch *tb = pnode.blocker;
				gn = tb->owner;
				elog(gdd_log_level(), "gdd visit (%lu) : pop stack: (%u, %u) branch of gtxn(%u-%ld-%u)",
					gn->visit_id, tb->waiting_shardid, tb->mysql_connid,
					gn->gtxnid.compnodeid, gn->gtxnid.ts, gn->gtxnid.txnid);
				// We are starting another candidate path from the diverging
				// point. use the cached best candidate, it's the best up to
				// the diverging point.
				pbest = pnode.best_so_far;
				cur_shardid = pnode.cur_shardid;
				single_shard_cycle = pnode.single_shard_cycle;
				if (cur_shardid == Invalid_shard_id)
					cur_shardid = tb->waiting_shardid;
				else if (cur_shardid != tb->waiting_shardid)
					single_shard_cycle = false;
				UpdateCandidate(pbest, tb);
			} while (gn);
		}
	}

	enable_remote_timeout();
	send_multi_stmts_to_multi();
	disable_remote_timeout();

	if (stk.base)
		pfree(stk.base);
}

/*
 * Do one round of deadlock detection.
 * */
void perform_deadlock_detect()
{
	if (!enable_global_deadlock_detection)
		return;
	elog(gdd_log_level(), "Performing one round of global deadlock detection on %d requests.", g_gdd_state->num_reqs);
	g_gdd_state->num_reqs = 0;
	g_gdd_state->when_last_gdd = time(0);

	sigjmp_buf	pdd_local_sigjmp_buf, *old_sjb = NULL;

	if (sigsetjmp(pdd_local_sigjmp_buf, 1) != 0)
	{
		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log 
		EmitErrorReport(); done in errfinish(0) */

		/*
		 * Need to downgrade the elevel to WARNING for errfinish(0) to pop
		 * error stack top. NOTE that we can do this because all functions
		 * called in this function don't touch shared memory or any pg tables,
		 * all operations are purely performed in current process's private
		 * address space. If shared memory is accessed and error occurs in the
		 * while, all other pg processes can be impacted.
		 * */
		downgrade_error();

		errfinish(0);
		AbortCurrentTransaction();
		LWLockReleaseAll();

		AbortBufferIO();
		UnlockBuffers();
		if (CurrentResourceOwner)
		{
			ResourceOwnerRelease(CurrentResourceOwner,
								 RESOURCE_RELEASE_BEFORE_LOCKS,
								 false, true);
			/* we needn't bother with the other ResourceOwnerRelease phases */
		}
		AtEOXact_Buffers(false);
		AtEOXact_SMgr();
		AtEOXact_Files(false);
		AtEOXact_HashTables(false);

		MemoryContextSwitchTo(gdd_stmts_memcxt);
		FlushErrorState();

		PG_exception_stack = old_sjb;

		/*
		 * In a background process, we can't rethrow here, it must keep running.
		 * So we must do proper cleanup and get rid of the error.
		 * */
		ResetCommunicationHubStmt(false);
		MemoryContextResetAndDeleteChildren(gdd_stmts_memcxt);
		RESUME_INTERRUPTS();

		/*
		 * Sleep at least 1 second after any error.  We don't want to be
		 * filling the error logs as fast as we can.
		 */
		pg_usleep(1000000L);

		return;
	}

	/* We can now handle ereport(ERROR) */
	old_sjb = PG_exception_stack;
	PG_exception_stack = &pdd_local_sigjmp_buf;

	ResetDeadlockDetectorState();
	SetCurrentStatementStartTimestamp();

	/*
	  Both build_wait_for_graph() and kill_victim() needs master info.
	  they should be in a txn, or in seperate txns (more prompt).
	*/
	Assert(!IsTransactionState());
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	bool gdd_supported;
	if ((gdd_supported = build_wait_for_graph()))
	{
		CHECK_FOR_INTERRUPTS();
	    find_and_resolve_global_deadlock();
	}
	/*
	  Can't do this before the scan finishes.
	*/
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	if (!gdd_supported)
		sleep(10000);
}

static bool check_gdd_supported(AsyncStmtInfo *asi)
{
	bool ret = false;
	char *verstr = get_storage_node_version(asi);
	if (!verstr)
	{
		elog(WARNING, "GDD: Can't get storage node version from shard.node(%u.%u), won't perform GDD.",
			 asi->shard_id, asi->node_id);
		return false;
	}

	if (strcasestr(verstr, "kunlun-storage"))
		ret = true;
	else
		elog(LOG, "GDD: Not using kunlun-storage as storage nodes, won't perform GDD.");
	pfree(verstr);
	return ret;
}

/*
  All storage nodes in a Kunlun cluster must be of same version, so it's OK
  to connect to any shard node for such a check.
*/
static char*get_storage_node_version(AsyncStmtInfo *asi)
{
	const char *stmt = "select version()";
	append_async_stmt(asi, stmt, strlen(stmt), CMD_SELECT, false, SQLCOM_SELECT);
	enable_remote_timeout();
	send_multi_stmts_to_multi();

	MYSQL_RES *mres = asi->mysql_res;
	char *verstr = NULL;

	if (mres == NULL)
		goto end;

	MYSQL_ROW row = mysql_fetch_row(mres);
	if (row == NULL)
	{
		check_mysql_fetch_row_status(asi);
		goto end;
	}
	verstr = pstrdup(row[0]);
end:
	free_mysql_result(asi);
	return verstr;
}
