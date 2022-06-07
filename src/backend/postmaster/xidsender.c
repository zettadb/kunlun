/* ----------
 * xidsender.c
 *      send accumulated xids to cluster primary node as one insert stmt, then
 *      resume the waiting backends.
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 *	src/backend/postmaster/xidsender.c
 * ----------
 */
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/param.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <string.h>
#include <time.h>
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

#include "pgstat.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/remote_meta.h"
#include "access/xact.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "catalog/pg_cluster_meta_nodes.h"
#include "catalog/pg_cluster_meta.h"
#include "executor/spi.h"
#include "postmaster/xidsender.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "sharding/cluster_meta.h"
#include "storage/bufmgr.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/latch.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/pg_shmem.h"
#include "utils/ascii.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"
#include "sharding/mysql/mysql.h"
#include "fmgr.h"
#include "tcop/utility.h"
#include "tcop/debug_injection.h"
#include "storage/ipc.h"
#include "sharding/sharding.h"

#define SHARDS_BITMAP_ELEM_BITS (sizeof(ulong) * 8)
#define SHARDS_BITMAP_LEN (MAX_SHARDS/ SHARDS_BITMAP_ELEM_BITS)

typedef struct GlobalXid
{
	Oid comp_nodeid;
	time_t deadline;
	GlobalTrxId gtrxid;
	PGPROC *proc;
	char txn_action; // 1: commit; 0: abort;
	int nshards;
	ulong shards_bitmap[SHARDS_BITMAP_LEN]; /* participants */

	/**
	 * Used to indicate which proc is waiting for which
	 *  object to avoid false notifications.
	 */
	pid_t pid;
	long fenceNo;
} GlobalXid;

typedef struct XidGlobalInfo
{
	int num_used_slots;
	TransactionId max_trxid;
	pid_t procid;
	sig_atomic_t counter;
}XidGlobalInfo;


static GlobalXid *XidSlots = NULL;
static XidGlobalInfo *g_xgi = NULL;
static MYSQL_CONN cluster_conn;
/*
  Do not retry connection, let upper level caller repeat its operation instead,
  so that we can handle topo checks ASAP.
*/
const static int MAX_METASHARD_MASTER_CONN_RETRIES = 1;

// GUC
int cluster_commitlog_group_size = 8;
int cluster_commitlog_delay_ms = 0;
bool skip_tidsync = false;

/* Signal handler flags */
static volatile bool got_SIGHUP = false;
static volatile bool got_SIGUSR2 = false;
static volatile bool got_SIGTERM = false;

extern int get_topo_service_pid(void);
extern void BackgroundWorkerInitializeConnection(const char *dbname, const char *username, uint32 flags);
static MYSQL_CONN* connect_to_metadata_cluster(bool isbg);
extern const char *GetClusterName2();
static void recover_nextXid_global(void);

uint32_t get_cluster_conn_thread_id()
{
	return mysql_thread_id(&cluster_conn.conn);
}

void disconnect_metadata_shard()
{
	close_metadata_cluster_conn(&cluster_conn);
}


MYSQL_CONN* get_metadata_cluster_conn(bool isbg)
{
	/*
	 * Return an established mysql connection to the metadata master node. Our
	 caller expect the returned connection to be always valid and established.
	 If call mysql_real_query() with an invalid mysql connection, the mysql
	 client lib could cause memory corrupt and failure to connect at next attempt.

	 If this is called in a bg process, in each retry in connect_to_metadata_cluster()
	 a new txn will be created so latest metadata is used; if called in a backend
	 process(user session), error will be thrown so client will retry in a new txn,
	 thus metadata is also updated and used.
	 * */
	 while (!cluster_conn.connected && !got_SIGTERM)
	 {
		CHECK_FOR_INTERRUPTS();
		connect_to_metadata_cluster(isbg);
		if (!cluster_conn.connected)
		{
			if (get_topo_service_pid() != MyProcPid)
				pg_usleep(1000000);
			else
			{
				UpdateCurrentMetaShardMasterNodeId();
				// when metashard master gone, we would loop like crazy without such a sleep.
				// this proc won't be frequently wakenup by SIGUSR2.
				if (!cluster_conn.connected) wait_latch(1000);
			}
		}
	 }
	 return &cluster_conn;
}

/*
  Connect metadata master node, and optionally recover current computing node's nextXid.
*/
static MYSQL_CONN* connect_to_metadata_cluster(bool isbg)
{
	bool need_txn = false;
	bool do_retry = false;
	int nretries = 0;
	int conn_fail = 0;
	uint16_t master_port;
	char master_ip[256];

	if (!IsTransactionState())
	{
		need_txn = true;
	}

	PG_TRY();
	{
retry:
		do_retry = false;
		/*
			TODO: if there is an txn already, then in the loop are we able to see
			updates to below 2 meta tables? to do so we must use SnapshotNow rather
			than SnapshotMVCC!
		*/
		CHECK_FOR_INTERRUPTS();
		if (need_txn)
		{
			SetCurrentStatementStartTimestamp();
			StartTransactionCommand();
			SPI_connect();
			PushActiveSnapshot(GetTransactionSnapshot());
		}

		HeapTuple ctup = SearchSysCache1(CLUSTER_META, comp_node_id);
		if (!HeapTupleIsValid(ctup))
		{
			/*
			 * This computing node was just created, it has not been initialized
			 * yet, or it can't see the tuples with current snapshot, so wait for
			 * a while and retry in a new txn&snapshot.
			 * */
			if (nretries++ < MAX_METASHARD_MASTER_CONN_RETRIES && !got_SIGTERM)
			{
				wait_latch(1000);
				do_retry = true;
				goto end;
			}

			ereport(ERROR, 
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Kunlun-db: cache lookup failed for cluster meta (pg_cluster_meta) by computing node id(comp_node_id) %u", comp_node_id),
					 errhint("comp_node_id variable must equal to pg_cluster_meta's single row's comp_node_id field.")));
		}

		Form_pg_cluster_meta cmeta = (Form_pg_cluster_meta)GETSTRUCT(ctup);
		cluster_id = cmeta->cluster_id;
		g_cluster_name = cmeta->cluster_name;

		Oid cmid = cmeta->cluster_master_id;
		HeapTuple cmtup = SearchSysCache1(CLUSTER_META_NODES, cmid);
		if (!HeapTupleIsValid(cmtup))
			elog(ERROR, "cache lookup failed for cluster meta primary node (pg_cluster_meta_node) by field server_id %u", cmid);
		Form_pg_cluster_meta_nodes cmnode = (Form_pg_cluster_meta_nodes)GETSTRUCT(cmtup);

		bool isnull = false;
		Datum pwdfld = SysCacheGetAttr(CLUSTER_META_NODES, cmtup, Anum_pg_cluster_meta_nodes_passwd, &isnull);
		if (isnull)
			elog(ERROR, "Non-nullable field 'passwd' in table pg_cluster_meta_nodes has NULL value.");

		Datum hostaddr_dat = SysCacheGetAttr(CLUSTER_META_NODES, cmtup, Anum_pg_cluster_meta_nodes_hostaddr, &isnull);
		if (isnull)
			elog(ERROR, "Non-nullable field 'hostaddr' in table pg_cluster_meta_nodes has NULL value.");
		char *pwd = TextDatumGetCString(pwdfld);
		char *hostaddr = TextDatumGetCString(hostaddr_dat);

		int rcm = 0;
		/*
		   The cmnode->is_master must be true, otherwise, pg_cluster_meta and
		   pg_cluster_meta_nodes tables have inconsistent data --- the former
		   says a node is master but in latter's corresponding row it is not stored
		   so. We must update the 2 tables in the same txn to achive such consistency.
		   */
		if (!cmnode->is_master)
		{
			ereport(ERROR, 
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("Kunlun-db: Meta data inconsistent in pg_cluster_meta and pg_cluster_meta_nodes tables, pg_cluster_meta.cluster_master_id is %u but this row M in pg_cluster_meta_nodes contains inconsistent fact: M.is_master is false.", cmid)));
		}
		Assert(cmnode->is_master);

		conn_fail = 0;
		strncpy(master_ip, hostaddr, sizeof(master_ip) - 1);
		master_port = cmnode->port;

		if ((rcm = connect_meta_master(&cluster_conn, hostaddr, cmnode->port, cmnode->user_name.data, pwd, isbg)))
		{
			if (cmnode->is_master && rcm == -2)
			{
				RequestShardingTopoCheck(METADATA_SHARDID);
				conn_fail = 2;
				/*
				 * Connected to an old master, it's no longer master now. Wait for
				 * master switch and meta table update to complete.
				 * */
				elog(LOG, "Connected to a former primary(now replica). Waiting for primary switch and meta table update to complete.");
			}
			else if (rcm == -1)
			{
				RequestShardingTopoCheck(METADATA_SHARDID);
				conn_fail = 1;
				/*
				 * Master db instance doesn't exist or gone, wait longer for it to
				 * become available.
				 * */
				elog(LOG, "Primary mysql instance doesn't exist or gone, waiting for a primary node to become available and reconnect.");
			}

			/*
			   For fg stmts, throw error and let client retry the user txn/stmt;
			   For bg ops, retry the operation in a brandnew txn in order to update the syscaches
			   if (conn_fail && isbg && nretries++ < MAX_METASHARD_MASTER_CONN_RETRIES)
			   {
			   if (!wait_latch(1000))
			   nretries--; // if waken by others, the wait isn't counted
			   do_retry = true;
			   }
			   */
		}

		ReleaseSysCache(cmtup);
		ReleaseSysCache(ctup);
end:
		/*
		 * And finish our transaction.
		 */
		if (need_txn)
		{
			SPI_finish();
			PopActiveSnapshot();
			CommitTransactionCommand();
		}

		if (do_retry)
			goto retry;
	}
	PG_CATCH();
	{
		if (need_txn && IsTransactionState())
		{
			AbortCurrentTransaction();
		}
		PG_RE_THROW();
	}
	PG_END_TRY();
	
	if (conn_fail)
	{
		ereport(isbg ? WARNING : ERROR, 
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("Kunlun-db: Can not connect to metadata shard primary node(%s, %u): %s", master_ip, master_port,
				 conn_fail == 1 ? "node unavailable" : "node isn't primary.")));
		return NULL;
	}

	return &cluster_conn;
}

/* True if recovering next xid for global txn */
static bool RecoveringGlobalNextXid = false;
static void recover_nextXid_global()
{
	if (XidSyncDone())
		return;
	/*
	 * It's likely that a previous crash of all postgres backends leaked
	 * transaction IDs that are already used and logged into metadata cluster's
	 * commit-log, but not logged in local CLog. All backends will make sure
	 * to reject transaction execution before ShmemVariableCache->nextXid is
	 * updated here when necessary.
	 * */
	Assert(g_xgi);
	Assert(MaxBackends > 0);
	Assert(IsTransactionBlock() == false);
	Assert(InRecovery == false);
	TransactionId old_nextxid = ShmemVariableCache->nextXid;
	TransactionId new_start = old_nextxid + MaxBackends;

	/* update next xid to the max xid recovered from redo log plus the max number of process */
	{
		LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
		advance_nextxid(new_start);
		LWLockRelease(XidGenLock);
	}

	/* alloc the first xid after pg startup, and flush redo log */
	PG_TRY();
	{
		RecoveringGlobalNextXid = true;
		StartTransactionCommand();
		GetTopTransactionId();
		CommitTransactionCommand();
		RecoveringGlobalNextXid = false;
	}
	PG_CATCH();
	{
		RecoveringGlobalNextXid = false;
		AbortCurrentTransaction();
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* mark the next xid available */
	g_xgi->max_trxid = new_start;

	elog(LOG, "Recovered ShmemVariableCache->nextXid from %u to %u.", old_nextxid, new_start);
}

bool XidSyncDone()
{
	// Caller must have acquired XidGenLock already!
	return g_xgi->max_trxid != InvalidTransactionId || RecoveringGlobalNextXid;
}

static int
qsort_oid_compare(const void *a, const void *b)
{
	return (*(Oid*)a - *(Oid*)b);
}

/**
 * @brief Get the index of corresponding shard in asc order
 * 
 * @return return -1 if the shardid is invalid
 */
static int
get_shardid_index(Oid shardid)
{
	static Oid sortedShards[MAX_SHARDS];
	static int numShards = 0;
	List *shardList;
	ListCell *lc;

	/* Reload sorted shards */
	int count = 0;
	bool starttxn = false;
	
	while (numShards <= 0 ||
	       sortedShards[numShards - 1] < shardid)
	{
		/* GetAllshardids() need called in transaction */
		if (!IsTransactionState()) {
			starttxn = true;
			StartTransactionCommand();
		}

		numShards = 0;
		shardList = GetAllShardIds();
		foreach (lc, shardList)
		{
			sortedShards[numShards++] = lfirst_oid(lc);
			Assert(numShards <= MAX_SHARDS);
		}

		qsort(sortedShards, numShards, sizeof(Oid), qsort_oid_compare);
		if (++count > 4)
			break;
	}

	if (starttxn)
		CommitTransactionCommand();

	/* binary search */
	if (numShards > 64)
	{
		Oid *addr = bsearch(&shardid,
				    sortedShards,
				    numShards,
				    sizeof(Oid),
				    qsort_oid_compare);
		if (addr)
			return (addr - sortedShards) / sizeof(Oid);
	}
	/* linear search */
	else
	{
		for (int i = 0; i < numShards; ++i)
		{
			if (sortedShards[i] == shardid)
				return i;
		}
	}

	elog(WARNING, "Get index of shard(%u) failed, number of shards:%d", shardid, numShards);
	return -1;
}

/**
 * @brief Get the Oid at the given index in a asc order
 * 
 */
static Oid
get_shardid_by_index(int index)
{
	static Oid sortedShards[MAX_SHARDS];
	static int numShards = 0;
	List *shardList;
	ListCell *lc;

	/* Reload sorted shards */
	int count = 0;
	bool starttxn = false;

	while (index >= numShards)
	{
		if (!IsTransactionState())
		{
			starttxn = true;
			StartTransactionCommand();
		}

		numShards = 0;
		shardList = GetAllShardIds();
		foreach (lc, shardList)
		{
			sortedShards[numShards++] = lfirst_oid(lc);
			Assert(numShards <= MAX_SHARDS);
		}

		qsort(sortedShards, numShards, sizeof(Oid), qsort_oid_compare);
		if (++count > 4)
			break;
	}

	if (starttxn)
		CommitTransactionCommand();

	if (index < numShards)
		return sortedShards[index];
	
	elog(WARNING, "Get shard by index(%d) failed, number of shards:%d", index, numShards);
	return InvalidOid;
}

/**
 * @brief Encode the shardid into bitmap
 */
static void
encode_shards_bitmap(Oid shardid, ulong *bitmap)
{
	int idx;

	if ((idx = get_shardid_index(shardid)) < 0)
	{
		ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("Kunlun-db: Geting index of shard(%u) failed", shardid)));
	}

	bitmap[idx / SHARDS_BITMAP_ELEM_BITS] |= (1ul << (idx % SHARDS_BITMAP_ELEM_BITS));
}

static void
decode_shards_bitmap(ulong *bitmap, int nshards, void (*fn)(void*, Oid), void *context)
{
	ulong elem;
	int idx = 0;
	int offset;
	Oid shardid;

	for (int i = 0; nshards && i < SHARDS_BITMAP_LEN; ++i)
	{
		elem = bitmap[i];
		while (elem && nshards)
		{
			/* last bit position */
			offset = ffsl(elem) - 1;
			elem &= (elem - 1);
			{
				shardid = get_shardid_by_index(idx + offset);
				if (shardid == InvalidOid)
				{
					ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Kunlun-db: Geting shard at index(%d) failed", idx)));
				}
				fn(context, shardid);

				--nshards;
			}
		}
		idx += SHARDS_BITMAP_ELEM_BITS;
	}
}

static void
dump_shards_bitmap_worker(void *context, Oid shardid)
{
	appendStringInfo(((StringInfo)context), "%u,", shardid);
}

/**
 * @brief dump shardid in json array format
 */
static void
dump_shards_bitmap(int nshards, ulong *bitmap, StringInfo str)
{
	appendStringInfoChar(str, '[');
	if (nshards)
	{
		decode_shards_bitmap(bitmap,
				     nshards,
				     dump_shards_bitmap_worker,
				     (void *)str);
		--str->len;
	}
	appendStringInfoChar(str, ']');
}

/*
 * Called by backends to wait for commit log write completion.
 * @retval 1: successful; 0: failure; -1: timeout
 * */
char WaitForXidCommitLogWrite(Oid comp_nodeid, GlobalTrxId xid, time_t deadline, List *prepared_shards, bool commit_it)
{
	LWLockAcquire(GlobalXidSenderLock, LW_EXCLUSIVE);
	int slot_idx = g_xgi->num_used_slots++;
	GlobalXid *slot = XidSlots + slot_idx;
	slot->comp_nodeid = comp_nodeid;
	slot->deadline = deadline;
	slot->gtrxid = xid;
	slot->proc = MyProc;
	slot->txn_action = (commit_it ? 1 : 0);
	slot->pid = MyProc->pid;
	slot->fenceNo = MyProc->fence.fence_no;
	
	/* Encode prepared shards */
	slot->nshards = list_length(prepared_shards);
	memset(slot->shards_bitmap, 0, sizeof(slot->shards_bitmap));
	ListCell *lc;
	foreach(lc, prepared_shards)
	{
		encode_shards_bitmap(lfirst_oid(lc), slot->shards_bitmap);
	}

	uint64_t counter = g_xgi->counter;
	if (g_xgi->num_used_slots >= cluster_commitlog_group_size && g_xgi->procid != 0)
		kill(g_xgi->procid, SIGUSR2);
	LWLockRelease(GlobalXidSenderLock);

	/* 
	 * Wait for completion if committing, no need to wait if aborting, because
	 * prepared txn branches will be aborted after timeouts.
	 * Only wait if our task has not been reap'ed yet otherwise this process
	 * won't be waken up ever.
	 */
	int ret = 0;

	if (commit_it && g_xgi->counter == counter)
	{
		/*
		  We read the non-atomic g_xgi->counter without holding GlobalXidSenderLock,
		  this isn't a problem --- the worst case is that the proc isn't notified
		  by PGSemaphoreUnlock(), and timeout mechanism will make sure the user
		  backend won't block forever but return correctly after statement timeout.
		  OTOH, we have 100% guarantee that if we don't wait here, the commit log
		  has definitely been received by metadata shard.
		*/
		ret = PGSemaphoreTimedLockFence(MyProc->sem, StatementTimeout, &MyProc->fence);
		Assert(ret == 0 || ret == 1);
		if (ret == 1) // the wait timed out
		{
			MyProc->commit_log_append_done = -1;
			/*
			  Do not make/append kill meta conn req here because in backend
			  process there is no established connection to metadata shard,
			  and the req is made&appended in gtss process in this case already.
			*/
			RequestShardingTopoCheck(METADATA_SHARDID);
		}
	}

	return MyProc->commit_log_append_done;
}

void disconnect_request_kill_meta_conn(int c, Datum d)
{
	disconnect_metadata_shard();
	ShardConnKillReq *req = makeMetaConnKillReq(1/*conn*/, mysql_thread_id(&cluster_conn.conn));
	if (req)
	{
		appendShardConnKillReq(req);
		pfree(req);
	}
}

/*
 * Quickly copy slots to local buffer to assemble insert stmt to send to
 * remote meta server. 'slots' is assumed big enough, it has MaxBackends
 * slots just as big as XidSlots.
 * */
static int reapXids(GlobalXid *slots)
{
	int ret = 0;
	LWLockAcquire(GlobalXidSenderLock, LW_EXCLUSIVE);
	memcpy(slots, XidSlots, g_xgi->num_used_slots * sizeof(GlobalXid));
	ret = g_xgi->num_used_slots;
	g_xgi->num_used_slots = 0;
	LWLockRelease(GlobalXidSenderLock);
	return ret;
}


/* ----------
 * Max number of concurrently running transactions.
 *
 * ----------
 */
#define MaxConcurrentTxns (MaxBackends)

static MemoryContext xidSenderLocalContext = NULL;


/* ----------
 * Local function forward declarations
 * ----------
 */
#ifdef EXEC_BACKEND
static pid_t xidsender_forkexec(void);
#endif

static void xidsender_exit(SIGNAL_ARGS);
static void xidsender_quickexit(SIGNAL_ARGS);
static void xidsender_sighup_handler(SIGNAL_ARGS);
static void xidsender_sigusr2_handler(SIGNAL_ARGS);


static void xidsender_setup_memcxt(void);

/* ----------
 * xidsender_setup_memcxt() -
 *
 *	Create xidSenderLocalContext, if not already done.
 * ----------
 */
static void
xidsender_setup_memcxt(void)
{
	if (!xidSenderLocalContext)
		xidSenderLocalContext = AllocSetContextCreate(TopMemoryContext,
												   "Xid Sender Memory Context",
												   ALLOCSET_DEFAULT_SIZES);
}
#ifdef EXEC_BACKEND

/*
 * xidsender_forkexec() -
 *
 * Format up the arglist for xid sender process, then fork and exec.
 */
static pid_t
xidsender_forkexec(void)
{
	char	   *av[10];
	int			ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--fork_xidsender";
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */

	av[ac] = NULL;
	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}
#endif							/* EXEC_BACKEND */
/* SIGQUIT signal handler for xidsender process */
static void
xidsender_exit(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGTERM = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

static void
xidsender_quickexit(SIGNAL_ARGS)
{
	_exit(2);
}

/* SIGHUP handler for xidsender process */
static void
xidsender_sighup_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGHUP = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/* SIGUSR2 handler for xidsender process */
static void
xidsender_sigusr2_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGUSR2 = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * xidsender_start() -
 *
 *	Called from postmaster at startup or after an existing sender dies.
 *
 *	Returns PID of child process, or 0 if fail.
 *
 *	Note: if fail, we will be called again from the postmaster main loop.
 */
int
xidsender_start(void)
{
	pid_t		xidSenderPid;

	MyPMChildSlot = AssignPostmasterChildSlot();
	/*
	 * Okay, fork off the xid sender.
	 */
#ifdef EXEC_BACKEND
	switch ((xidSenderPid = xidsender_forkexec()))
#else
	switch ((xidSenderPid = fork_process()))
#endif
	{
		case -1:
			(void) ReleasePostmasterChildSlot(MyPMChildSlot);
			ereport(LOG,
					(errmsg("Kunlun-db: could not fork global transaction ID sender: %m")));
			return 0;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			InitPostmasterChild();

			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			/* Drop our connection to postmaster's shared memory, as well
			 * we need shmem access.
			dsm_detach_all();
			PGSharedMemoryDetach();*/

			XidSenderMain(0, NULL);
			break;
#endif

		default:
			return (int) xidSenderPid;
	}

	return 0;
}


static inline const char *txn_action(char action)
{
	if (action == 1)
		return "commit";
	else if (action == 0)
		return "abort";
	else
		return NULL;
}

static void
check_commit_log_decision(GlobalXid *slotbuff, int *nslots)
{
	GlobalXid *slot;
	GlobalTrxId gtrxid;
	Oid comp_nodeid;
	const char *action;
	char *end;
	int num = *nslots;
	StringInfoData stmt;

	/* Query the metadata server about the states of the transactions */
	initStringInfo(&stmt);
	appendStringInfo(&stmt, "select comp_node_id, txn_id, next_txn_cmd  from " KUNLUN_METADATA_DBNAME ".commit_log_%s where (comp_node_id, txn_id) in ",
			 GetClusterName2());
	{
		appendStringInfoChar(&stmt, '(');
		for (int i = 0; i < num; i++)
		{
			GlobalXid *slot = slotbuff + i;
			appendStringInfo(&stmt, "(%u, %lu),", slot->comp_nodeid, slot->gtrxid);
		}
		--stmt.len;
		appendStringInfoChar(&stmt, ')');
	}

	if (send_stmt_to_cluster_meta(&cluster_conn, stmt.data, stmt.len, CMD_SELECT, true))
		goto end;

	if (cluster_conn.result)
	{
		MYSQL_ROW row;
		while ((row = mysql_fetch_row(cluster_conn.result)))
		{
			comp_nodeid = strtoul(row[0], &end, 10);
			gtrxid = strtoull(row[1], &end, 10);
			action = row[2];
			for (int i = 0; i < num; ++i)
			{
				slot = slotbuff + i;
				if (slot->comp_nodeid == comp_nodeid && slot->gtrxid == gtrxid)
				{
					if (strcasecmp(txn_action(slot->txn_action), action) == 0)
					{
						elog(LOG, "Check transaction commit log, transaction '%u-%lu' is %sed", comp_nodeid, gtrxid, action);
						slot->proc->commit_log_append_done = 1;
					}
					else
					{
						elog(LOG, "Check transaction commit log, transcation '%u-%lu' is %sed by cluster manager", comp_nodeid, gtrxid, action);
						slot->proc->commit_log_append_done = 0;
					}

					/* move the completed slot to the tail*/
					if (i != num - 1)
					{
						GlobalXid tmp = slotbuff[num - 1];
						slotbuff[num - 1] = *slot;
						*slot = tmp;
					}
					--num;
					break;
				}
			}
		}
		free_metadata_cluster_result(&cluster_conn);

		/* Notify the waiting proc */
		for (int i = num; i < *nslots; ++i)
		{
			slot = slotbuff + i;
			if (slot->txn_action == 1 && // only commit waits
			    slot->proc->pid == slot->pid)
			{
				PGSemaphoreUnlockFence(slot->proc->sem,
						       &slot->proc->fence,
						       slot->fenceNo);
			}
		}

		*nslots = num;
	}

	/* check the deadline */
	{
		time_t now = time(NULL);
		for (int i = 0; i < num; ++i)
		{
			slot = slotbuff + i;
			if (slot->deadline < now)
			{
				/* mark timeout */
				slot->proc->commit_log_append_done = -1;
				if (i < num - 1)
				{
					GlobalXid tmp = slotbuff[num - 1];
					slotbuff[num - 1] = *slot;
					*slot = tmp;
				}
				--num;
			}
		}

		/* Notify the proc ? */
		for (int i = num; i < *nslots; ++i)
		{
			slot = slotbuff + i;
			if (slot->txn_action == 1 && // only commit waits
			    slot->proc->pid == slot->pid)
			{
				PGSemaphoreUnlockFence(slot->proc->sem,
						       &slot->proc->fence,
						       slot->fenceNo);
			}
		}

		*nslots = num;
	}
end:
	if (stmt.data)
		pfree(stmt.data);
}

static uint64_t MAX_SIGATOMIC = 0;

void XidSenderMain(int argc, char **argv)
{
	sigjmp_buf	local_sigjmp_buf;

	if (sizeof(sig_atomic_t) == 4)
		MAX_SIGATOMIC = UINT32_MAX;
	else if (sizeof(sig_atomic_t) == 8)
		MAX_SIGATOMIC = UINT64_MAX;
	else
		Assert(false);
	/*
	 * Identify myself via ps
	 */
	init_ps_display("global transaction state synchronizer(GTSS)", "", "", "");
	/*
	 * Ignore all signals usually bound to some action in the postmaster,
	 * except SIGHUP and SIGQUIT.  Note we don't need a SIGUSR1 handler to
	 * support latch operations, because we only use a local latch.
	 */
	SetProcessingMode(InitProcessing);
	pqsignal(SIGHUP, xidsender_sighup_handler);
	pqsignal(SIGINT, StatementCancelHandler);
	pqsignal(SIGTERM, xidsender_exit);
	pqsignal(SIGQUIT, xidsender_quickexit);
	InitializeTimeouts();
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, SIG_IGN);
	pqsignal(SIGUSR2, xidsender_sigusr2_handler);
	pqsignal(SIGCHLD, SIG_DFL);
	pqsignal(SIGTTIN, SIG_DFL);
	pqsignal(SIGTTOU, SIG_DFL);
	pqsignal(SIGCONT, SIG_DFL);
	pqsignal(SIGWINCH, SIG_DFL);
	PG_SETMASK(&UnBlockSig);
	
	IsBackgroundWorker = true;
	g_xgi->procid = getpid();

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * See notes in postgres.c about the design of this coding.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		/* Abort the current transaction in order to recover */
		AbortCurrentTransaction();

		/*
		 * Release any other resources, for the case where we were not in a
		 * transaction.
		 */
		LWLockReleaseAll();
		pgstat_report_wait_end();
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

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		//MemoryContextSwitchTo(xidSenderLocalContext);
		 */
		FlushErrorState();

		/* 
		  All memory alloced in xidSenderLocalContext live throughout lifetime
		  of this proc, don't release them here.
		  MemoryContextResetAndDeleteChildren(xidSenderLocalContext);
		*/

		/*
		 * Make sure pgstat also considers our stat data as gone.
		 */
		pgstat_clear_snapshot();

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();

		/* if in shutdown mode, no need for anything further; just go away */
		if (got_SIGTERM)
			goto out;

		/*
		 * Sleep at least 1 second after any error.  We don't want to be
		 * filling the error logs as fast as we can.
		 */
		pg_usleep(1000000L);
		goto start;
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/* Early initialization */
	BaseInit();

	/*
	 * Create a per-backend PGPROC struct in shared memory, except in the
	 * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
	 * this before we can use LWLocks (and in the EXEC_BACKEND case we already
	 * had to do some stuff with LWLocks).
	 */
#ifndef EXEC_BACKEND
	InitProcess();
#endif

	InitPostgres("postgres", InvalidOid, NULL, InvalidOid, NULL, false);

	SetProcessingMode(NormalProcessing);

	xidsender_setup_memcxt();
	GlobalXid *localbuf =
		MemoryContextAlloc(xidSenderLocalContext,
						   MaxBackends * sizeof(GlobalXid));
	StringInfoData stmt, json_shards;
        initStringInfo2(&stmt, 8192, xidSenderLocalContext);
	initStringInfo2(&json_shards, 8192, xidSenderLocalContext);
	time_t now = 0, when_last_send = 0;

	recover_nextXid_global();
start:

	now = time(0);
	if (!cluster_conn.connected)
	{
		connect_to_metadata_cluster(true);
	}

	while (!got_SIGTERM)
	{
		/*
		 * When we have configs to use, reread config file here and update the
		 * config variables.
		 * */
		if (got_SIGHUP)
		{

		}

		if (!cluster_conn.connected)
		{
			connect_to_metadata_cluster(true);
		}

		// Do an inaccurate unsynced read, wait 1s if no much work accumulated yet.
		if (g_xgi->num_used_slots < cluster_commitlog_group_size)
		{
			wait_latch(cluster_commitlog_delay_ms);
		}

		CHECK_FOR_INTERRUPTS();

		if (when_last_send == 0 || (now = time(0)) - when_last_send > 60)
		{
			check_mysql_instance_status(&cluster_conn, CHECK_KEEP_ALIVE, true);
			when_last_send = now;
		}

		int nslots = reapXids(localbuf);

		if (nslots == 0)
			continue;

		
		// send the stmt.
		int retry_count = 0;
retry_send:
		resetStringInfo(&stmt);

		/*
		  Let metadata cluster primary node wait for 'resume' signal which
		  can be signaled later in test scripts.
		*/
		DEBUG_INJECT_IF("test_metadata_svr_commit_log_append_timeout",
			appendStringInfo(&stmt, "set session debug_sync='before_execute_sql_command wait_for resume';"););

		appendStringInfo(&stmt, "insert into " KUNLUN_METADATA_DBNAME ".commit_log_%s (comp_node_id, txn_id, next_txn_cmd, written_shards) values ", GetClusterName2());
		for (int i = 0; i < nslots; i++)
		{
			GlobalXid *slot = localbuf + i;
			/* dump participant shards in json format */
			resetStringInfo(&json_shards);
			dump_shards_bitmap(slot->nshards, slot->shards_bitmap, &json_shards);

			appendStringInfo(&stmt, "(%u, %lu, (if (unix_timestamp() > %ld,  NULL, '%s')), '%s'),",
					 slot->comp_nodeid, slot->gtrxid, slot->deadline,
					 txn_action(slot->txn_action),
					 json_shards.data);
		}

		stmt.data[stmt.len-1] = '\0';// remove the last comma.
		stmt.len--;

		{
			CHECK_FOR_INTERRUPTS();

			bool done = !send_stmt_to_cluster_meta(&cluster_conn, stmt.data, stmt.len, CMD_INSERT, true);
			if (!done)
			{
				elog(WARNING, "Failed to execute commit log insert stmt (%s) on cluster primary , retrying.", stmt.data);

				if (!cluster_conn.connected)
				{
					pg_usleep(100000); // when metashard master gone, we would loop like crazy without such a sleep.
					connect_to_metadata_cluster(true);
				}
				if (got_SIGTERM)
					break;

				/* Check the commit log */
				check_commit_log_decision(localbuf, &nslots);
				++ retry_count;

				if (nslots > 0)
				{
					if (retry_count > 4)
						pg_usleep(100000);

					goto retry_send;
				}
			}

			when_last_send = time(0);
			LWLockAcquire(GlobalXidSenderLock, LW_EXCLUSIVE);
			g_xgi->counter++;
			if (g_xgi->counter == MAX_SIGATOMIC)
				g_xgi->counter = 0;
			LWLockRelease(GlobalXidSenderLock);

			for (int i = 0; i < nslots; i++)
			{
				GlobalXid *slot = localbuf + i;
				slot->proc->commit_log_append_done = (done ? 1 : 0);
				if (slot->txn_action == 1 && // only commit waits
				    slot->proc->pid == slot->pid)
				{
					PGSemaphoreUnlockFence(slot->proc->sem,
							       &slot->proc->fence,
							       slot->fenceNo);
				}
			}
		}
	}
out:
	proc_exit(0);
}

/*
 * Report shared-memory space needed by CreateSharedBackendXidSlots.
 */
Size
BackendXidSenderShmemSize(void)
{
	Size		size;

	/* XidSlots: */
	size = mul_size(sizeof(GlobalXid), MaxConcurrentTxns);
	size += MAXALIGN(sizeof(XidGlobalInfo));
	return size;
}


/*
 * Initialize the shared status array and several string buffers
 * during postmaster startup.
 */
void
CreateSharedBackendXidSlots(void)
{
	Size		size;
	bool		found;

	/* Create or attach to the shared array */
	size = BackendXidSenderShmemSize();
	g_xgi = (XidGlobalInfo*)ShmemInitStruct("Backend Global XID Info and Slots Array", size, &found);
	XidSlots = (GlobalXid *) ((char*)g_xgi + MAXALIGN(sizeof(XidGlobalInfo)));

	if (!found)
	{
		/*
		 * We're the first - initialize.
		 */
		MemSet(g_xgi, 0, size);
		g_xgi->max_trxid = InvalidTransactionId;
	}
}


/* ----------
 * xidsender_initialize() -
 *
 *	Initialize xidsenders state, and set up our on-proc-exit hook.
 *	Called from InitPostgres .
 * ----------
 */
void
xidsender_initialize(void)
{
	before_shmem_exit(disconnect_request_kill_meta_conn, 0);
	/* Set up a process-exit hook to clean up */
	//on_shmem_exit(xidsender_beshutdown_hook, 0);
}

/*
  Wait for latch by 'millisecs' milli-seconds optionally.
  @retval true if timed out; false otherwise
*/
bool wait_latch(int millisecs)
{
	int rc = WaitLatch(MyLatch,
				   WL_LATCH_SET | (millisecs > 0 ? WL_TIMEOUT : 0) | WL_POSTMASTER_DEATH,
				   millisecs,
				   PG_WAIT_EXTENSION);
	if (rc & WL_LATCH_SET) ResetLatch(MyLatch);
	/* emergency bailout if postmaster has died */
	if (rc & WL_POSTMASTER_DEATH)
		proc_exit(1);
	return rc&WL_TIMEOUT;
}

