/*-------------------------------------------------------------------------
 *
 * sharding_conn.c
 *		routines managing sharding connections and session states.
 *
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * IDENTIFICATION
 *	  src/backend/sharding/sharding_conn.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "pgstat.h"

#include "sharding/sharding_conn.h"
#include "sharding/sharding.h"
#include "sharding/mysql_vars.h"
#include "sharding/mat_cache.h"
#include "access/parallel.h"
#include "access/remote_meta.h"
#include "access/remotetup.h"
#include "access/remote_xact.h"
#include "utils/algos.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "nodes/nodes.h"
#include "sharding/mysql/mysql.h"
#include "sharding/mysql/errmsg.h"
#include "sharding/mysql/mysqld_error.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "utils/timeout.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"

#include <arpa/inet.h>
#include <limits.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

extern Oid comp_node_id;

#define IS_MYSQL_CLIENT_ERROR(err) \
	(((err) >= CR_MIN_ERROR && (err) <= CR_MAX_ERROR) || ((err) >= CER_MIN_ERROR && (err) <= CER_MAX_ERROR))

#define SAFE_HANDLE(h) ((StmtSafeHandle){h, handle_epoch})

#define HANDLE_EXPIRED(h) (h.epoch != handle_epoch)

#define CHECK_HANDLE_EPOCH(h) \
	do {	\
		if (h.epoch != handle_epoch) \
			elog(ERROR, "A fatal error may have occurred, the handle used has expired."); \
	} while (false);

// GUC vars.
// seconds waiting for connect done.
int mysql_connect_timeout = 10;
/*
 * Seconds waiting for read/write done, real wait time is 3/2 times this value
 * according to doc because of read/write retries.
 * CR_SERVER_LOST ('Lost connection...') is returned if connect/read/write times out;
 * if the other side closed connection already, read/write returns
 * CR_SERVER_GONE_ERROR('Server gone away...').
 */
int mysql_read_timeout = 10;
int mysql_write_timeout = 10;
int mysql_max_packet_size = 16384;
bool mysql_transmit_compress = false;
static int32_t handle_epoch = 0;

static void ResetASI(AsyncStmtInfo *asi);
static bool async_connect(MYSQL *mysql, const char *host, uint16_t port, const char *user, const char *password);
static ShardConnection *GetConnShard(Oid shardid);
static ShardConnection *AllocShardConnSlot(Oid shardid, int inspos);
static int AllocShardConnNodeSlot(ShardConnection *sconn, Oid nodeid, int *newconn, bool req_chk_onfail);
static void handle_backend_disconnect(AsyncStmtInfo *asi);
static MYSQL *GetConnShardNode(Oid shardid, Oid nodeid, int *newconn, bool req_chk_onfail);
static MYSQL *GetConnShardMaster(Oid shardid, int *newconn);
static bool ConnHasFlag(AsyncStmtInfo *asi, int flagbit);
static bool MarkConnFlag(AsyncStmtInfo *asi, int flagbit, bool b);

static void check_mysql_node_status(AsyncStmtInfo *asi, bool want_master);

static void work_on_stmt(AsyncStmtInfo *asi, StmtHandle *handle);
static bool send_stmt_impl(AsyncStmtInfo *asi, StmtHandle *handle);
static bool recv_stmt_result_impl(AsyncStmtInfo *asi, StmtHandle *handle);
static bool fetch_stmt_remote_next(AsyncStmtInfo *asi, StmtHandle *handle);
static bool handle_stmt_remote_result(AsyncStmtInfo *asi, StmtHandle *handle);
static StmtHandle* poll_remote_events_any(StmtHandle *handles[], int count, int timeout_ms);
static bool process_preceding_stmts(AsyncStmtInfo *asi, StmtHandle *cur);
static void flush_invalid_stmts(AsyncStmtInfo *asi);
static void flush_all_stmts_impl(AsyncStmtInfo **asi, int count, bool cancel);
static void cancel_all_stmts_impl(AsyncStmtInfo *asi[], int cnt);

static void materialize_current_tuple(AsyncStmtInfo *asi, StmtHandle *handle);
static MYSQL_ROW next_row_from_matcache(StmtHandle *handle);

static void handle_stmt_error(AsyncStmtInfo *asi, StmtHandle *handle, int eno);

static void CurrentStatementsShards(StringInfo str);
static void disconnect_request_kill_shard_conns(int i, Datum d);

bool IsConnReset(AsyncStmtInfo *asi)
{
	return ConnHasFlag(asi, CONN_RESET);
}

inline static bool IsConnValid(AsyncStmtInfo *asi)
{
	return ConnHasFlag(asi, CONN_VALID);
}

/*
 * If MYSQL connection is reset, we need to mark it so that before next stmt
 * sent we can send the SET NAMES and cached session vars ahead.
 * */
inline static bool MarkConnReset(AsyncStmtInfo *asi, bool b)
{
	return MarkConnFlag(asi, CONN_RESET, b);
}

inline static bool MarkConnValid(AsyncStmtInfo *asi, bool valid)
{
	if (!valid)
		asi->conn = NULL;
	return MarkConnFlag(asi, CONN_VALID, valid);
}

/*
 * The mapping of postgreSQL database name and schema name to mysql database name:
 * In pg, these facts are true, besides those in doc:
 * 1. A connection can't switch database, it sticks to one database.
 * 2. A schema is processed as a namespace, it's logical and virtual.
 * 3. each object handle(Relation, Proc, etc) has a namespace field.
 *
 * so use dbname-schemaname as mysql db name. the max length is 192 bytes in mysql side.
 * all names sent to mysql must be qualified (dbname.objname)
 * */

/*
 * Make a qualified name for an object(relation, proc, etc) using its
 * namespace id and object name. *plen takes back the length of the fully
 * qualified name if plen isn't NULL. 'objname' can be NULL, and then we
 * produce a dbname-schemaname string which can be used as a mysql db name.
 * whenever a pg db or schema is created we need to create a db in mysql
 * with such a db name.
 * */
const char *make_qualified_name(Oid nspid, const char *objname, int *plen)
{
	static StringInfoData qname;
	initStringInfo(&qname);

	get_database_name2(MyDatabaseId, &qname);
	appendStringInfoString(&qname, "_$$_");

	get_namespace_name2(nspid, &qname);
	if (!objname)
	{
		goto end;
	}

	appendStringInfo(&qname, ".%s", objname);
end:
	if (plen)
		*plen = lengthStringInfo(&qname);

	return qname.data;
}

#define SHARD_SECTION_NCONNS 16

typedef struct ShardConnIdxSection
{
	/*
	 * shard ids in this array are inserted in order (asc), it's a quick index
	 * to locate the corresponding ShardConnection object. This always hold:
	 *      shard_ids[i] == conns[i]->shard_id
	 *
	 * */
	Oid shard_ids[SHARD_SECTION_NCONNS];
	ShardConnection *conns[SHARD_SECTION_NCONNS];
} ShardConnIdxSection;

typedef struct ShardConnSection
{
	/*
	 * NO. of valid ShardConnection objects in 'shards' array and idx.shard_ids
	 * and idx.conns arrays.
	 */
	int nconns;
	ShardConnIdxSection idx;
	ShardConnection shards[SHARD_SECTION_NCONNS];
	struct ShardConnSection *next;
} ShardConnSection;

typedef struct ShardingSession
{
	/*
	 * Mysql session variables set by user/client, cached during session lifetime.
	 * Whenever a new shard node connection is made, computing node must set all
	 * these session variables to mysql.
	 *
	 * Every variable can exist once in this list.
	 * */
	// Var_section *set_vars;// alloc in TopMemcxt. this is defined in mysql_vars.c as a global object.
	ShardConnSection all_shard_conns; // 1st section
	ShardConnSection *last_section;	  // ptr to last section.

	/*
	 * remote stmt send&result recv facility.
	 * */

	/*
	 * dzw: async send 'ports' for execution of remote statements.
	 * each shard to be written to has such a slot, each relation to be
	 * written/read is wired to its owner shard's slot.
	 * Whenver a remote stmt is formed, it's appended to the target
	 * AsyncStmtInfo's stmt queue. To send stmts, we first check whether there
	 * is pending result to recv,
	 * if so wait&recv it via send_stmt_to_multi_wait, then send its stmt via
	 * send_stmt_to_multi_start.
	 *
	 * At end of an stmt(e.g. insert), each end_remote_insert_stmt() call appends
	 * the relation's stmts to its ri_pasi port's stmt queue. then finally in
	 * ExecEndModifyTable(), send each shard's accumulated stmts this way:
	 * do above check&wait&recv&send for every slot's every stmt, one iteration
	 * after another. each iteration sends one stmt of every port.
	 *
	 * TODO: do vector write in mysql_real_query/mysql_real_query_start/_cont
	 * in order to send multiple stmts to one shard using one mysql_real_query_start call.
	 * */
	AsyncStmtInfo *asis;
	int num_asis_used; // NO. of slots used in current stmt in 'asis'.
	int num_asis;	   // total NO. of slots in 'asis'.
} ShardingSession;

static ShardingSession cur_session;

/**
 * @brief Cleanup stmt handle allocated in subtransaction after transaction committed/rollback
 */
static void
remote_conn_subxact_cb(SubXactEvent event, SubTransactionId mySubid,
		       SubTransactionId parentSubid, void *arg)
{
	if (event == SUBXACT_EVENT_ABORT_SUB || event == SUBXACT_EVENT_COMMIT_SUB)
	{
		const size_t count = cur_session.num_asis_used;
		AsyncStmtInfo *pasi = cur_session.asis;
		ListCell *lc;
		StmtHandle *handle;
		// scan all connections currently in use
		for (int i = 0; i < count; ++i, ++pasi)
		{
			do
			{
				handle = NULL;
				/* Find out stmt handle allocated in current subtransaction */
				foreach (lc, pasi->stmt_inuse)
				{
					handle = (StmtHandle *)lfirst(lc);
					/* For currently running stmt, make sure its refcount bigger than zero */
					if (handle->subxactid == mySubid &&
					    (pasi->curr_stmt != handle || handle->refcount > 1))
						break;
					handle = NULL;
				}
				if (!handle)
					break;
				/* Cancel statement currently running */
				if (pasi->curr_stmt == handle)
					cancel_stmt_async(SAFE_HANDLE(handle));
				release_stmt_handle(SAFE_HANDLE(handle));
			} while (true);
		}
	}
}

void InitShardingSession()
{
	cur_session.last_section = &cur_session.all_shard_conns;
	init_var_cache();
	/*
	  Make sure conns are always closed.
	*/
	before_shmem_exit(disconnect_request_kill_shard_conns, 0);
				
        RegisterSubXactCallback(remote_conn_subxact_cb, NULL);
}

static void disconnect_request_kill_shard_conns(int i, Datum arg)
{
	disconnect_storage_shards();
	ShardConnKillReq *req = makeShardConnKillReq(1 /*kill conn*/);
	if (req)
	{
		/*
		  If shared memory isn't corrupt, i.e. this backend proc is exiting
		  gracefully(i.e. client ends/kills its connection), the request can be
		  appended and all mysql conns will be closed/killed reliably.
		  Otherwise, there could be mysql conns that are stuck in query
		  execution left open until it has a chance to read next network packet.
		*/
		appendShardConnKillReq(req);
		pfree(req);
	}
}

AsyncStmtInfo *GetAsyncStmtInfo(Oid shardid)
{
	return GetAsyncStmtInfoNode(shardid, InvalidOid, true);
}

/**
 * @brief Check the connection is still up
 * 
 */
static
bool check_conn_alive(MYSQL *conn)
{
	struct tcp_info info;
	int len = sizeof(info);
	int socket = mysql_get_socket(conn);
	if (getsockopt(socket, IPPROTO_TCP, TCP_INFO, &info, (socklen_t *)&len) == 0)
	{
		return (info.tcpi_state == TCP_ESTABLISHED);
	}
	return true;
}
/*
  Get communication port to (shardid, shardNodeId) storage node.
  If shardNodeId is InvalidOid, it's current master node.
*/
AsyncStmtInfo *GetAsyncStmtInfoNode(Oid shardid, Oid shardNodeId, bool req_chk_onfail)
{
	if (cur_session.asis == NULL)
	{
		cur_session.num_asis = 32;
		cur_session.asis = MemoryContextAllocZero(TopMemoryContext, sizeof(AsyncStmtInfo) * cur_session.num_asis);
	}

	bool want_master = false;
	int newconn = 0;
	Storage_HA_Mode ha_mode = storage_ha_mode();
	AsyncStmtInfo *asi;

	if (shardNodeId == InvalidOid)
	{
		shardNodeId = GetShardMasterNodeId(shardid);
		/*
		  Iff shardNodeId == InvalidOid do caller want to connect to the
		  shard's master node. Otherwise caller simply want to connect to
		  the node, so don't check it's a master.
		 */
		if (ha_mode != HA_NO_REP)
			want_master = true;
	}

	for (int i = 0; i < cur_session.num_asis_used; i++)
	{
		AsyncStmtInfo *pasi = cur_session.asis + i;
		if (pasi->shard_id == shardid && pasi->node_id == shardNodeId)
		{
			/* return what we have, do not support reconnect in transaction */
			return pasi;
		}
	}

	if (cur_session.num_asis_used == cur_session.num_asis)
	{
		cur_session.asis = repalloc(cur_session.asis, sizeof(AsyncStmtInfo) * cur_session.num_asis * 2);
		memset(cur_session.asis + cur_session.num_asis, 0, cur_session.num_asis * sizeof(AsyncStmtInfo));
		cur_session.num_asis *= 2;
	}

	asi = cur_session.asis + cur_session.num_asis_used++;
	// Assert(asi->conn == NULL && asi->shard_id == InvalidOid && asi->node_id == InvalidOid);
make_conn:
	asi->conn = GetConnShardNode(shardid, shardNodeId, &newconn, req_chk_onfail);
	asi->shard_id = shardid;
	asi->node_id = shardNodeId;

	/* Check if the connection is still established */
	if (!newconn && !check_conn_alive(asi->conn))
	{
		mysql_close(asi->conn);
		MarkConnValid(asi, false);
		goto make_conn;
	}

	/*
	 * Set session status to the new connection, including all cached mysql var
	 * values, and SET NAMES. they will be sent to target shard ahead of
	 * the initial DML stmts(which are preceded by XA START).
	 * */
	if (newconn || IsConnReset(asi))
	{
		int setvar_stmtlen = 0;

		char *setvar_stmt = produce_set_var_stmts(&setvar_stmtlen);
		if (setvar_stmt && setvar_stmtlen > 0)
		{
			send_stmt_async_nowarn(asi, setvar_stmt, setvar_stmtlen, CMD_UTILITY, true, SQLCOM_SET_OPTION);
		}

		char cmdbuf[256];
		int cmdlen = 0;
		if (ha_mode != HA_NO_REP)
			cmdlen = snprintf(cmdbuf, sizeof(cmdbuf),
					  "SET NAMES 'utf8'; set session autocommit = true; set computing_node_id=%u; set global_conn_id=%u",
					  comp_node_id, getpid());
		else
			cmdlen = snprintf(cmdbuf, sizeof(cmdbuf),
					  "SET NAMES 'utf8'; set session autocommit = true ");
		Assert(cmdlen < sizeof(cmdbuf));

		send_stmt_async_nowarn(asi, cmdbuf, cmdlen, CMD_UTILITY, false, SQLCOM_SET_OPTION);

		// must append this last in one packet of sql stmts.
		if (want_master)
		{
			check_mysql_node_status(asi, want_master);
		}
		flush_all_stmts_impl(&asi, 1, false);

		/*
		 * Make the communication port brandnew. this is crucial, without this
		 * operation, XA START won't be correctly generated and sent at the
		 * start of a txn branch.
		 * */
		ResetASI(asi);
		/*
		 * Now we've set/sent session status to the mysql connection, we can
		 * clear the CONN_RESET bit.
		 * */
		MarkConnReset(asi, false);
	}

	Assert(IsConnValid(asi) && !IsConnReset(asi));

	return asi;
}

/*
 * Reset asi at end(or start) of a stmt.
 * */
static void ResetASI(AsyncStmtInfo *asi)
{
	/*
	 * We don't release memory here, it's been released when the stmt's result
	 * has been received.
	 * */
	cancel_all_stmts_impl(&asi, 1);
	
	asi->stmt_queue = NIL;
	asi->stmt_wrows = 0;
	asi->nwarnings = 0;
	asi->executed_stmts = 0;
	asi->did_write = asi->did_read = asi->did_ddl = false;
	asi->txn_in_progress = false;
	// asi->shard_id = InvalidOid;
	// asi->node_id = InvalidOid;
	// asi->conn = NULL;
	asi->txn_wrows = 0;
	{
		while(list_length(asi->stmt_inuse))
		{
			StmtHandle *handle = (StmtHandle*)linitial(asi->stmt_inuse);
			release_stmt_handle(SAFE_HANDLE(handle));
		}
	}
}
/*
 * Called at start of each txn to totally cleanup all channels used by prev txn.
 * */
void ResetCommunicationHub()
{
	for (int i = 0; i < cur_session.num_asis_used; i++)
	{
		ResetASI(cur_session.asis + i);
	}
	cur_session.num_asis_used = 0;
	/* increment handle_epoch to invalid handles out of module */
	++ handle_epoch;
}

// called at end(or start) of a stmt to reset certain states but keep some other states.
void ResetCommunicationHubStmt(bool ended_clean)
{
	for (int i = 0; i < cur_session.num_asis_used; i++)
	{
		AsyncStmtInfo *asi = cur_session.asis + i;
		asi->stmt_wrows = 0;
		asi->nwarnings = 0;
	}
}

/*
 * Alloc a ShardConnection object in cur_session.last_section, if no space,
 * alloc a new ShardConnSection first.
 * */
static ShardConnection *AllocShardConnSlot(Oid shardid, int inspos)
{
	if (cur_session.last_section->nconns == SHARD_SECTION_NCONNS)
	{
		ShardConnSection *psect = (ShardConnSection *)MemoryContextAllocZero(
		    TopMemoryContext, sizeof(ShardConnSection));

		cur_session.last_section->next = psect;
		cur_session.last_section = psect;
		inspos = -1;
	}

	ShardConnSection *psect = cur_session.last_section;
	ShardConnection *pconn = psect->shards + psect->nconns; // simply append the object at end of array.

	Assert(inspos < psect->nconns + 1);

	// make idx.shardids/conns in shardid asc order.
	if (inspos >= 0 && inspos < psect->nconns)
	{
		memmove(psect->idx.shard_ids + inspos + 1, psect->idx.shard_ids + inspos, (psect->nconns - inspos) * sizeof(Oid));
		memmove(psect->idx.conns + inspos + 1, psect->idx.conns + inspos, (psect->nconns - inspos) * sizeof(void *));
	}

	if (inspos < 0)
		inspos = 0;

	psect->idx.shard_ids[inspos] = shardid;
	psect->idx.conns[inspos] = pconn;

	cur_session.last_section->nconns++;
	pconn->shard_id = shardid;

	return pconn;
}

static inline bool
ShardBackendConnValid(ShardConnection *sconn, int pos)
{
	return sconn->conns[pos] != NULL && (sconn->conn_flags[pos] & CONN_VALID);
}

static int AllocShardConnNodeSlot(ShardConnection *sconn, Oid nodeid, int *newconn, bool req_chk_onfail)
{
	Oid *ids = sconn->nodeids;
	int inspos = -1;
	int pos = bin_search(&nodeid, ids, sconn->num_nodes, sizeof(nodeid), oid_cmp, &inspos);
	MYSQL *mysql_conn;

	// Most likely an already existing mysql connection.
	*newconn = 0;
	// found existing valid connection, return it.
	if (likely(pos >= 0 && ShardBackendConnValid(sconn, pos)))
		return pos;

	Shard_node_t snode;
	bool found_shard_node;
	/*
	  If the new connection isn't valid, establish it. Especially, the MYSQL slot
	  could be unallocated because we failed to connect to the target mysql
	  instance, and in this case we need to allocate it to retry the connection.
	*/
	if (pos >= 0 && !ShardBackendConnValid(sconn, pos))
	{
		inspos = pos;
		mysql_conn = sconn->conns[pos];
		Assert(mysql_conn);
		mysql_init(mysql_conn);
		goto make_conn;
	}

	if (sconn->num_nodes >= MAX_NODES_PER_SHARD)
		ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("Kunlun-db: A shard can have at most %d nodes, shard %u is given more.",
				MAX_NODES_PER_SHARD, sconn->shard_id)));

	// alloc slot and make new connection.
	if (inspos >= 0 && inspos < sconn->num_nodes)
	{
		memmove(ids + inspos + 1, ids + inspos, (sconn->num_nodes - inspos) * sizeof(Oid));
		memmove(sconn->conns + inspos + 1, sconn->conns + inspos, (sconn->num_nodes - inspos) * sizeof(void *));
		memmove(sconn->conn_flags + inspos + 1, sconn->conn_flags + inspos, (sconn->num_nodes - inspos) * sizeof(char));
	}

	if (inspos < 0) // allocing&inserting 1st element.
	{
		inspos = 0;
	}

	ids[inspos] = nodeid;

	// Use last MYSQL slot.
	mysql_conn = sconn->conn_objs + sconn->num_nodes;
	sconn->num_nodes++;

	sconn->conns[inspos] = mysql_conn;
	sconn->conn_flags[inspos] = 0;
make_conn:
	found_shard_node = FindCachedShardNode(sconn->shard_id, nodeid, &snode);

	if (found_shard_node &&
	    !async_connect(mysql_conn, snode.hostaddr, snode.port,
			   snode.user_name.data, snode.passwd))
	{
		if (req_chk_onfail)
			RequestShardingTopoCheck(sconn->shard_id);
		/*
		  Although connection fails this time, the storge slots allocated to
		  'nodeid' belong to it, we won't revoke them.
		*/
		ereport(ERROR,
			(errcode(ERRCODE_CONNECTION_FAILURE),
			 errmsg("Kunlun-db: Failed to connect to mysql storage node at (%s, %u): %d, %s",
				snode.hostaddr, snode.port, mysql_errno(mysql_conn),
				mysql_error(mysql_conn))));
	}
	sconn->conn_flags[inspos] |= CONN_VALID;

	// This is a newly established mysql connection.
	*newconn = 1;

	return inspos;
}

static bool MarkConnFlag(AsyncStmtInfo *asi, int flagbit, bool b)
{
	Oid shardid = asi->shard_id;
	Oid nodeid = asi->node_id;
	Assert(nodeid != 0);

	ShardConnection *sconn = GetConnShard(shardid);
	Oid *ids = sconn->nodeids;
	int inspos;
	int pos = bin_search(&nodeid, ids, sconn->num_nodes, sizeof(nodeid), oid_cmp, &inspos);
	if (pos < 0)
		return false;
	if (b)
		sconn->conn_flags[pos] |= flagbit;
	else
		sconn->conn_flags[pos] &= ~flagbit;
	return true;
}

static bool ConnHasFlag(AsyncStmtInfo *asi, int flagbit)
{
	Oid shardid = asi->shard_id;
	Oid nodeid = asi->node_id;
	Assert(nodeid != 0);

	ShardConnection *sconn = GetConnShard(shardid);
	Oid *ids = sconn->nodeids;
	int inspos;
	int pos = bin_search(&nodeid, ids, sconn->num_nodes, sizeof(nodeid), oid_cmp, &inspos);
	if (pos < 0)
		return false;
	return sconn->conn_flags[pos] & flagbit;
}

/*
 * Find a ShardConnection object by (shardid, nodeid) from cur_session.
 * If not found, create a new one and cache it in cur_session.
 * */
static ShardConnection *GetConnShard(Oid shardid)
{
	int inspos = -1, pos;
	ShardConnSection *psect = NULL;

	/*
	 * Find from each section one by one.
	 * */
	for (psect = &cur_session.all_shard_conns; psect; psect = psect->next)
	{
		const Oid *sids = psect->idx.shard_ids;
		pos = bin_search(&shardid, sids, psect->nconns, sizeof(shardid), oid_cmp, &inspos);
		Assert(pos != -2); // no argument error.
		if (pos >= 0)
			break;
	}

	ShardConnection *pconn = NULL;

	if (pos < 0)
	{
		Assert(psect == NULL);
		pconn = AllocShardConnSlot(shardid, inspos);
	}
	else
		pconn = psect->idx.conns[pos];

	return pconn;
}

/*
 * Find MYSQL connection object by (shardid, nodeid) from cur_session.
 * If not found, start a new connection and cache it in cur_session.
 *
 * A non-master node of a shard can be needed when we want to dispatch a read
 * only query(select stmt, etc) to a slave node when currently there is no
 * active transaction.
 *
 * Also when we already know the master node's id we can call this function
 * instead of GetConnShardMaster(). Never cache master node object or id
 * elsewhere than the ShardCache, otherwise when master switches the cached
 * value will be obsolete.
 @param req_chk_onfail: if connect to node fails, request master switch check.
 * */
static MYSQL *GetConnShardNode(Oid shardid, Oid nodeid, int *newconn, bool req_chk_onfail)
{
	ShardConnection *sconn = GetConnShard(shardid);
	int slot = AllocShardConnNodeSlot(sconn, nodeid, newconn, req_chk_onfail);
	return sconn->conns[slot];
}

/*
 * Find a ShardConnection object by (shardid, nodeid) from cur_session.
 * If not found, create a new one and cache it.
 * */
static MYSQL *GetConnShardMaster(Oid shardid, int *newconn)
{
	ShardConnection *sconn = GetConnShard(shardid);
	int slot = AllocShardConnNodeSlot(sconn, GetShardMasterNodeId(shardid), newconn, true);
	return sconn->conns[slot];
}
static int
wait_for_mysql(MYSQL *mysql, int status, int timeout_ms)
{
	struct pollfd pfd;
	int timeout;
	int res;
	pfd.fd = mysql_get_socket(mysql);
	pfd.events =
	    (status & MYSQL_WAIT_READ ? POLLIN : 0) |
	    (status & MYSQL_WAIT_WRITE ? POLLOUT : 0) |
	    (status & MYSQL_WAIT_EXCEPT ? POLLPRI : 0);

	if (timeout_ms != -1)
		timeout = timeout_ms;
	else if (status & MYSQL_WAIT_TIMEOUT)
		timeout = mysql_get_timeout_value_ms(mysql);
	else
		timeout = -1;
	
	while (true)
	{
		res = poll(&pfd, 1, timeout);
		if (res == 0)
			return MYSQL_WAIT_TIMEOUT;
		else if (res < 0)
		{
			// handle EINTR and ENOMEM
			if (errno == ENOMEM)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
						 errmsg("Kunlun-db: poll() failed with ENOMEM(%d : %s)", errno, strerror(errno))));
				break;
			}
			else if (errno == EINTR)
			{
				CHECK_FOR_INTERRUPTS();
				continue;
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Kunlun-db: poll() unexpectedly failed with (%d : %s)", errno, strerror(errno))));
			}
		}
		else
		{
			int status = 0;
			if (pfd.revents & POLLIN)
				status |= MYSQL_WAIT_READ;
			if (pfd.revents & POLLOUT)
				status |= MYSQL_WAIT_WRITE;
			if (pfd.revents & POLLPRI)
				status |= MYSQL_WAIT_EXCEPT;
			return status;
		}
	}
}

/**
 * Make connection to target mysql instance, return true of successful, false on failure.
 * TCP_NODELAY: 0
 * SO_SNDBUF: 64KB or more
 * SO_RCVBUF:64KB or more
 * no need to set, can set default at linux system level, and the max value
 varies among system settings and an out-of-range setting will not take effect,
 so better leave as default so that DBA can set it at Linux system level.
 *
 * */
static bool async_connect(MYSQL *mysql, const char *host, uint16_t port, const char *user, const char *password)
{
	Assert(mysql != NULL);
	mysql_init(mysql);
	mysql_options(mysql, MYSQL_OPT_NONBLOCK, 0);
	mysql_options(mysql, MYSQL_OPT_CONNECT_TIMEOUT, &mysql_connect_timeout);
	mysql_options(mysql, MYSQL_OPT_READ_TIMEOUT, &mysql_read_timeout);
	mysql_options(mysql, MYSQL_OPT_WRITE_TIMEOUT, &mysql_write_timeout);
	/*
	 * USE result, no difference from STORE result for small result set, but
	 * for large result set, USE can use the result rows before all are recved,
	 * and also, extra copying of result data is avoided.
	 * */
	int use_res = 1;
	mysql_options(mysql, MYSQL_OPT_USE_RESULT, &use_res);
	mysql_options(mysql, MYSQL_OPT_MAX_ALLOWED_PACKET, &mysql_max_packet_size);

	/**
	 * more options to investigate,
	 *
	 * these are supported by both mariadb&mysql:
	 *    MYSQL_OPT_USE_RESULT,  MYSQL_OPT_PROTOCOL,
	 * these are supported by latest mysql8.0.15 but not latest mariadb, may be useful in future:
	 *      MYSQL_OPT_RETRY_COUNT, MYSQL_OPT_OPTIONAL_RESULTSET_METADATA,
	  * SSL options, useful if we need to connect to storage nodes using ssl.
	  * MYSQL_OPT_TLS_VERSION,
	  * MYSQL_OPT_SSL_KEY,
	  * MYSQL_OPT_SSL_CERT,
	  * MYSQL_OPT_SSL_CA,
	  * MYSQL_OPT_SSL_CAPATH,
	  * MYSQL_OPT_SSL_CIPHER,
	  * MYSQL_OPT_SSL_CRL,
	  * MYSQL_OPT_SSL_CRLPATH,
	  * These SSL options are not in latest mariadb:
	  * MYSQL_OPT_SSL_MODE,
	  * MYSQL_OPT_GET_SERVER_PUBLIC_KEY,
	  * MYSQL_OPT_SSL_FIPS_MODE
	  * For all option bits not existing in latest mariadb, if we really need them
	  * we can modify mariadb client lib code to add these option bits.
	  */
	if (mysql_transmit_compress)
		mysql_options(mysql, MYSQL_OPT_COMPRESS, NULL);

	// Never reconnect, because that messes up txnal status.
	my_bool reconnect = 0;
	mysql_options(mysql, MYSQL_OPT_RECONNECT, &reconnect);

	/* Returns 0 when done, else flag for what to wait for when need to block. */
	int status = 0;
	MYSQL *ret = NULL;
	status = mysql_real_connect_start(&ret,
					  mysql,
					  host,
					  user,
					  password,
					  NULL,
					  port,
					  NULL,
					  CLIENT_MULTI_STATEMENTS | (mysql_transmit_compress ? MYSQL_OPT_COMPRESS : 0));
	while (status)
	{
		status = wait_for_mysql(mysql, status, -1);
		status = mysql_real_connect_cont(&ret, mysql, status);
	}

	if (!ret)
	{
		/**
		 * TODO:
		 * If we are in an active transaction now, we have to abort the user
		 * connection and all its connections to storage nodes, because we can't
		 * resume current stmt&txn execution without this new connection. If we
		 * have no active txn state now, we can keep current fore-end and
		 * its backend connections, and the stmt(mostly auto-commit stmt, or a
		 * set/show stmt to be sent to backends) execution fails.
		 * */
		return false;
	}

	elog(LOG, "Connected to mysql instance at %s:%u", host, port);
	return true;
}

int GetAsyncStmtInfoUsed()
{
	return cur_session.num_asis_used;
}

AsyncStmtInfo *GetAsyncStmtInfoByIndex(int i)
{
	AsyncStmtInfo *res = (i < cur_session.num_asis_used && i >= 0) ? cur_session.asis + i : NULL;

	// when res->conn is NULL, this assert fails
	// Assert(res->shard_id != InvalidOid && res->node_id != InvalidOid);

	// if (!IsConnValid(res))
	{
		/*
		  In future we may want to reconnect when the conn is invalid, but
		  not now.
		 */
		// if (conn_invalid) // res marked valid inside it.
		//   GetAsyncStmtInfoNode(res->shard_id, res->node_id, true);
		// else
		// res = NULL;
	}

	return res;
}

uint64_t GetRemoteAffectedRows()
{
	uint64_t num = 0;

	for (int i = 0; i < cur_session.num_asis_used; i++)
	{
		AsyncStmtInfo *pasi = cur_session.asis + i;
		num += pasi->stmt_wrows;
	}

	return num;
}

/**
 * @brief Sets the ignored errors for subsequent statements added by calling add;
 */
static int stmt_ignored_eno;
int set_stmt_ignored_error(int eno)
{
	int old = stmt_ignored_eno;
	stmt_ignored_eno = eno;
	return old;
}

/**
 * Append 'stmt' into asi's job queue. 'stmt' will be sent later when its
 * turn comes, then it will be pfree'd.
 */
StmtSafeHandle
send_stmt_async(AsyncStmtInfo *asi, char *stmt, size_t stmt_len,
		CmdType cmd, bool owns_stmt_mem, enum enum_sql_command sqlcom, bool materialize)
{
	/*
	  If the shard node isn't connected, don't append stmt to it. This could happen
	  when a shard master is down but the fact is not yet detected in this computing
	  node. bg processes will catch the connection failure exception.
	*/
	if (!ASIConnected(asi))
	{
		ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("Kunlun-db: Can not append remote queries to a broken channel to shard %u node %u.",
				asi ? asi->shard_id : 0, asi ? asi->node_id : 0)));
		return INVALID_STMT_HANLE;
	}

	Assert(cmd == CMD_INSERT || cmd == CMD_UPDATE || cmd == CMD_DELETE ||
	       cmd == CMD_SELECT || cmd == CMD_UTILITY || cmd == CMD_DDL || cmd == CMD_TXN_MGMT);

	if (cmd == CMD_DDL && IsExplicitTxn())
	{
		ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("Kunlun-db: As demanded by MySQL, a DDL statement can NOT be executed in an explicit transaction.")));
	}

	/* Make sure the memory allocated from top transaction context */
	if (owns_stmt_mem)
	{
		Assert(GetMemoryChunkContext(stmt) == TopTransactionContext);
	}

	MemoryContext mem_ctx = MemoryContextSwitchTo(TopMemoryContext);

	StmtHandle *handle = (StmtHandle *)palloc0(sizeof(StmtHandle));
	handle->asi = asi;
        handle->subxactid = GetCurrentSubTransactionId();
	handle->refcount = 2;
	handle->stmt = stmt;
	handle->stmt_len = stmt_len;
	handle->owns_stmt_mem = owns_stmt_mem;
	handle->cmd = cmd;
	handle->sqlcom = sqlcom;
	handle->support_rewind = materialize;
	handle->ignore_errno = stmt_ignored_eno;
	handle->is_dml_write =
	    (cmd == CMD_INSERT || cmd == CMD_DELETE || cmd == CMD_UPDATE);
	asi->stmt_queue = lappend(asi->stmt_queue, handle);
	asi->stmt_inuse = lappend(asi->stmt_inuse, handle);

	MemoryContextSwitchTo(mem_ctx);

	/**
	 *  urge processing of the pending statements if :
	 *  (1) No runing statements
	 *  (2) The running statement does not return a tuple, which avoids expensive materialization.
	 *  (3) Too many pending statements
	 */
	if (asi->curr_stmt == NULL ||
	    asi->curr_stmt->cmd != CMD_SELECT ||
	    list_length(asi->stmt_queue) > 10)
	{
		process_preceding_stmts(asi, NULL);
	}

	return SAFE_HANDLE(handle);
}

/**
 * @brief Before sending the statement, update the inner status of asi, and add extra info to the handle
 */
static void
work_on_stmt(AsyncStmtInfo *asi, StmtHandle *handle)
{
	/*
	 * If this shard is accessed for the 1st time, send the txn start stmts
	 * to it first, then send DML stmts.
	 * A DDL stmt is always sent as an autocommit txn without XA cmds wrapped.
	 * Note that if in future we need to send non DML stmts, such as SHOW
	 * commands, set var commands, etc, we won't need to send XA START before
	 * them. It will not be an error to send XA START in such situations though.
	 * If user statement only access one shard with only one interactions, and not 
	 * in an explicit transaction, than no need to wrap the statement in a xa transaction.
	 *
	 * In a txn the 1st stmt might be a subtxn and it might fail and be aborted,
	 * and in this case we should avoid generating&sending another XA START stmt.
	 * */

	if (!asi->did_write &&
	    !asi->did_read &&
	    handle->cmd != CMD_DDL &&
	    handle->sqlcom != SQLCOM_SET_OPTION &&
	    IsTransactionState())
	{
		/*
		  Need wrap the statement into a xa transaction if :
		  (1) in a explicit transaction
		  (2) need multi interactions with remote shards
		  (3) invoke non internal language function which may introduce extra interaction with shards
		  (4) background process which have no valid estimate of the number of interactions
		 */
		if (asi->txn_in_progress)
		{
			/* Already in xa transaction, do nothing */
		}
		else 
		{
			StringInfoData txnstart;
			int tslen = Max(512, 256 + handle->stmt_len);

			initStringInfo2(&txnstart, tslen, TopTransactionContext);
			StartTxnRemote(&txnstart);
			appendStringInfoChar(&txnstart, ';');
			appendBinaryStringInfo(&txnstart, handle->stmt, handle->stmt_len);
			if (handle->owns_stmt_mem)
				pfree(handle->stmt);
			handle->stmt_len = lengthStringInfo(&txnstart);
			handle->stmt = donateStringInfo(&txnstart);
			handle->owns_stmt_mem = true;
			asi->txn_in_progress = true;
		}
	}

	CmdType cmd = handle->cmd;
	if (!asi->did_write)
		asi->did_write = (cmd == CMD_INSERT || cmd == CMD_UPDATE || cmd == CMD_DELETE);
	if (!asi->did_ddl)
		asi->did_ddl = (cmd == CMD_DDL);
	if (!asi->did_read)
		asi->did_read = (cmd == CMD_SELECT || cmd == CMD_UTILITY);
	asi->executed_stmts++;
}

static bool
send_stmt_impl(AsyncStmtInfo *asi, StmtHandle *handle)
{
	Assert(!asi->curr_stmt);
	Assert(handle->asi == asi);
	Assert(ASIConnected(asi));
	int ret = 0;
	/* update asi status, and add extra info to current statment */
	work_on_stmt(asi, handle);
	
	asi->curr_stmt = handle;
	asi->stmt_queue = list_delete_ptr(asi->stmt_queue, handle);
	/* send it */
	handle->status_req = mysql_real_query_start(&ret,
											asi->conn,
											handle->stmt,
											handle->stmt_len);

	elog(DEBUG1, "sent query to [%u, %u:%ld]: %s",
	     asi->shard_id,
	     asi->node_id,
	     mysql_thread_id(asi->conn),
	     handle->stmt);

	if (handle->status_req == 0)
	{
		/* No need to call mysql_real_query_cont() */
		handle->first_packet = true;

		/** 
		 * Free the stmts memory here, otherwise, it is automatically 
		 * released depending on the corresponding memory context.
		 */
		if (handle->owns_stmt_mem)
		{
			pfree(handle->stmt);
			handle->stmt = NULL;
			handle->owns_stmt_mem = false;
		}
	}

	return handle->status_req == 0;
}

/**
 * @brief Recv next result of the statement
 *
 * @return true 	EOF or a new result
 * @return false 	the result is on the way
 */
static bool
recv_stmt_result_impl(AsyncStmtInfo *asi, StmtHandle *handle)
{
	Assert(asi == handle->asi);
	Assert(asi->curr_stmt == handle);
	Assert(ASIConnected(asi));
	int err = 0;
	bool ret = false;

	if (handle->finished)
	{
		ret = true;
		goto end;
	}

	if (handle->status_req &&
	    (handle->status_req & handle->status) == 0)
		return false;

	/* Waiting for the first packet of the first result */
	if (!handle->first_packet)
	{
		if ((handle->status_req =
			 mysql_real_query_cont(&err, asi->conn, handle->status)))
			return false;

		handle->status = 0;
		
		/* We have got the response packet now, mark it */
		handle->first_packet = true;

		/* Free memory */
		if (handle->owns_stmt_mem)
		{
			pfree(handle->stmt);
			handle->stmt = NULL;
			handle->owns_stmt_mem = false;
		}
		
		if (err)
			handle_stmt_error(asi, handle, mysql_errno(asi->conn));
	}

	/* Waiting for the first packet of the next result */
	if (handle->nextres)
	{
		if ((handle->status_req =
			 mysql_next_result_cont(&err, asi->conn, handle->status)))
			return false;

		handle->status = 0;
		handle->nextres = false;
	}

	PG_TRY();
	{
		ret = handle_stmt_remote_result(asi, handle);
	}
	PG_CATCH();
	{
		if (handle->finished)
		{
			asi->curr_stmt = NULL;
			release_stmt_handle(SAFE_HANDLE(handle));
		}
		PG_RE_THROW();
	}
	PG_END_TRY();

end:
	if (handle->finished)
	{
		asi->curr_stmt = NULL;
		release_stmt_handle(SAFE_HANDLE(handle));
	}

	return ret;
}

/**
 * @brief Fetch the next tuple of Select/Returning's result
 *
 * @return true 	EOF or a new tuple
 * @return false 	need wait
 */
static inline bool
fetch_stmt_remote_next(AsyncStmtInfo *asi, StmtHandle *handle)
{
	if (handle->status_req &&
		(handle->status_req & handle->status) == 0)
		return false;

	if (handle->status_req == 0)
		handle->status_req = mysql_fetch_row_start(&handle->row,
												   handle->res);
	else
		handle->status_req = mysql_fetch_row_cont(&handle->row,
												  handle->res,
												  handle->status);
	if (handle->status_req)
		return false;

	handle->status = 0;

	/* empty, maybe error or EOF */
	if (!handle->row)
	{
		int eno;
		if ((eno = mysql_errno(asi->conn)))
		{
			handle_stmt_error(asi, handle, eno);
			Assert(handle->finished);
			/* This is an ignored error, reaching the EOF */
			return true;
		}

		mysql_free_result(handle->res);
		handle->res = NULL;
		if (!mysql_more_results(asi->conn))
		{
			handle->finished = true;
		}
		else
		{
			int ret;
			handle->status_req = mysql_next_result_start(&ret, asi->conn);
			handle->nextres = (handle->status_req != 0);
		}
	}
	/* it's returning, so count for the affected rows */
	else
	{
		handle->lengths = mysql_fetch_lengths(handle->res);
		if (handle->cmd != CMD_SELECT)
		{
			++handle->affected_rows;
			++asi->stmt_wrows;
			++asi->txn_wrows;
		}
	}

	return true;
}

/**
 * @brief Handle the result of the given statment
 *
 * @return true 	EOF or a new tuple
 * @return false 	Need wait
 */
static bool
handle_stmt_remote_result(AsyncStmtInfo *asi, StmtHandle *handle)
{
	int eno;
	Assert(ASIConnected(asi));

	/* it is finished, no more results */
	if (handle->finished)
		return true;

	if (handle->status_req &&
		(handle->status_req & handle->status) == 0)
		return false;
	
	if (!handle->res)
	{
		/* Check if it is 'select/returning' based on the returned metadata. ? */
		int field_count = 0;
		if ((field_count = mysql_field_count(asi->conn)) > 0)
		{
			if (handle->fetch) // fetch is true, means we handle select/returning before
			{
				if (!handle->cancel) // Don't complain abort canceled statement
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Kunlun-db: More than one SELECT statements were sent at once.")));
				}
			}

			handle->field_count = field_count;

			/* Mark we are going to fetch the result tuple */
			handle->fetch = true;
			if (!(handle->res = mysql_use_result(asi->conn)))
			{
				if ((eno = mysql_errno(asi->conn)))
				{
					handle_stmt_error(asi, handle, eno);
					// This is an ignored error, reaching the EOF of the result
					Assert(handle->finished);
					return true;
				}
				else
				{
					ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Kunlun-db: A %s statement returned no results.",
							handle->cmd == CMD_SELECT ? "SELECT" : "RETURNING")));
				}
			}
			
			// fetch the field type
			MYSQL_FIELD *fields = mysql_fetch_field(handle->res);
			handle->types = (enum enum_field_types *)
			    MemoryContextAlloc(TopMemoryContext, field_count * sizeof(enum enum_field_types));
			for (int i = 0; i < field_count; ++i)
				handle->types[i] = fields->type;
		}
	}

	if (!handle->res)
	{
		uint64_t n = mysql_affected_rows(asi->conn);
		if (n == (uint64_t)-1)
		{
			handle_stmt_error(asi, handle, mysql_errno(asi->conn));
			// it's a ignore error, reach the EOF of the result
			return true;
		}
		handle->affected_rows += n;
		asi->stmt_wrows += n;
		asi->txn_wrows += n;
		asi->nwarnings += mysql_warning_count(asi->conn);

		/* No more results? it's EOF ? */	
		if (!mysql_more_results(asi->conn))
		{
			handle->finished = true;
		}
		else
		{
			int ret;
			handle->status_req = mysql_next_result_start(&ret, asi->conn);
			handle->nextres = (handle->status_req != 0);
		}

		return true;
	}
	else
	{
		/* free result if it is canceled  */
		if (handle->cancel)
		{
			handle->row = NULL;
			if (handle->status_req == 0)
				handle->status_req = mysql_free_result_start(handle->res);
			else
				handle->status_req = mysql_free_result_cont(handle->res, handle->status);
			if (handle->status_req)
				return false;

			handle->status = 0;
			handle->res = NULL;
			if (!mysql_more_results(asi->conn))
			{
				handle->finished = true;
			}
			else
			{
				int ret;
				handle->status = mysql_next_result_start(&ret, asi->conn);
				handle->nextres = (handle->status != 0);
			}
			return true;
		}
		else
		{
			return fetch_stmt_remote_next(asi, handle);
		}
	}
}

bool is_stmt_eof(StmtSafeHandle h)
{
	CHECK_HANDLE_EPOCH(h);
	StmtHandle *handle = RAW_HANDLE(h);
	return handle->finished &&
	       (!handle->read_cache || !handle->cache || matcache_eof(handle->cache));
}

static StmtHandle*
poll_remote_events_any(StmtHandle *handles[], int size, int timeout_ms)
{
	struct pollfd pfds[size];
	memset(pfds, 0, sizeof(pfds[0]) * size);

	int num = 0;
	for (int i=0; i<size; ++i)
	{
		StmtHandle *handle = handles[i];
		if (handle->finished || !handle->status_req || (handle->status_req & handle->status))
			return handle;

		struct pollfd *pfd = pfds + num;
		pfd->fd = mysql_get_socket(handle->asi->conn);
		pfd->revents = 0;
		pfd->events =
			(handle->status_req & MYSQL_WAIT_READ ? POLLIN : 0) |
			(handle->status_req & MYSQL_WAIT_WRITE ? POLLOUT : 0) |
			(handle->status_req & MYSQL_WAIT_EXCEPT ? POLLPRI : 0);
		++num;
	}

	StmtHandle *active_handle = NULL;
	while (num)
	{
		int res = poll(pfds, num, timeout_ms);

                /* Timeout, break  */
                if (res == 0)
                        break;
		if (res < 0)
		{
			// handle EINTR and ENOMEM
			if (errno == ENOMEM)
			{
				pgstat_report_wait_end();
				ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					 errmsg("Kunlun-db: poll() failed with ENOMEM(%d : %s)", errno, strerror(errno))));
				break;
			}
			else if (errno == EINTR)
			{
				CHECK_FOR_INTERRUPTS();
				continue;
			}
			else
			{
				pgstat_report_wait_end();
				ereport(ERROR,
					(errcode(ERRCODE_SYSTEM_ERROR),
					 errmsg("Kunlun-db: poll() unexpectedly failed with system error (%d : %s)",
						errno, strerror(errno))));
			}
		}
		else if (res > 0)
		{
			for (size_t i = 0; i < num; i++)
			{
				struct pollfd *pfd = pfds + i;
				StmtHandle *handle = handles[i];
				handle->status = 0;
				if (pfd->revents & POLLIN)
					handle->status |= MYSQL_WAIT_READ;
				if (pfd->revents & POLLOUT)
					handle->status |= MYSQL_WAIT_WRITE;
				if (pfd->revents & POLLPRI)
					handle->status |= MYSQL_WAIT_EXCEPT;
				active_handle = handle;
			}
			break;
		}
	}

	return active_handle;
}

/**
 * @brief Urges processing of statements preceding cur .
 *
 * 	if cur = null, then all statements in the queue are urged to be processed
 *
 * @return true	 	All of the statements preceding cur has been processed
 * @return false 	A preceding statement is in progress, need wait for a while
 */
static bool
process_preceding_stmts(AsyncStmtInfo *asi, StmtHandle *cur)
{
	StmtHandle *handle;
	Assert(ASIConnected(asi));
	if (cur && cur == asi->curr_stmt)
		return true;
	while (true)
	{
		if (asi->curr_stmt)
		{
			handle = asi->curr_stmt;
			/* mark should read the cache to get the next tuple  */
			if (!handle->read_cache)
			{
				handle->read_cache = true;
				/* Read from current write position */
				if (handle->cache)
				{
					matcache_get_write_pos(handle->cache, &handle->cache_pos);
					handle->reset_cache_pos = true;
				}
			}

			/* materialize results of previous statment */
			++handle->refcount;
			while (recv_stmt_result_impl(asi, handle))
			{
				if (handle->finished)
					break;

				if (handle->row && !handle->cancel)
				{
					materialize_current_tuple(asi, handle);
				}
			}

			if (!handle->cache)
				handle->read_cache = false;

			bool finished = handle->finished;
			release_stmt_handle(SAFE_HANDLE(handle));
			if (!finished)
				return false;
		}

		do
		{
			if (list_length(asi->stmt_queue) == 0 ||
			    (handle = (StmtHandle*)linitial(asi->stmt_queue)) == cur)
				return true;

			if (handle->cancel)
			{
				asi->stmt_queue = list_delete_ptr(asi->stmt_queue, handle);
				release_stmt_handle(SAFE_HANDLE(handle));
				continue;
			}
		} while (0);

		/* send preceding statement */
		if (!send_stmt_impl(asi, handle))
			return false;
	}
}

static void
flush_invalid_stmts(AsyncStmtInfo *pasi)
{
	Assert(ASIConnected(pasi) == false);
	ListCell *lc;
	foreach (lc, pasi->stmt_queue)
	{
		StmtHandle *handle = (StmtHandle *)lfirst(lc);
		handle->cancel = true;
		handle->finished = true;
		release_stmt_handle(SAFE_HANDLE(handle));
	}
	pasi->stmt_queue = NIL;

	if (pasi->curr_stmt)
	{
		pasi->curr_stmt->cancel = true;
		pasi->curr_stmt->finished = true;
		release_stmt_handle(SAFE_HANDLE(pasi->curr_stmt));
		pasi->curr_stmt = NULL;
	}
}

/**
 * @brief Urge processing of the statements in asi[], and wait for them to complete.
 *
 *  Because this is an internally used helper function, if an asi is invalid, all the statements
 *  in it will be canceled and no error will be thrown, and the caller needs to do the
 *  corresponding checks before calling it.
 *  
 *  @param cancel	True if we are waiting for canceled stmts to be complete
 */
static void
flush_all_stmts_impl(AsyncStmtInfo **asi, int count, bool cancel)
{
	StmtHandle *handle;
	StmtHandle *blocking[count];
	int num_blocking = 0;
	bool is_write = false;
	bool is_ddl = false;
	bool enable_timeout = false;
	int checkCount = 0;

	PG_TRY();
	{
		while (true)
		{
			for (int i = 0; i < count; ++i)
			{
				AsyncStmtInfo *pasi = asi[i];

				/* if the the connection is invalid, just release all the stmts */
				if ( !ASIConnected(pasi))
				{
					flush_invalid_stmts(pasi);
					continue;
				}
			recheck:
				/* any pending stmts? */
				if (!pasi->curr_stmt && list_length(pasi->stmt_queue) == 0)
					continue;

				/* urges processing of  the statements in queue */
				if (process_preceding_stmts(pasi, NULL))
					goto recheck;

				handle = pasi->curr_stmt;
				is_write |= handle->is_dml_write;
				is_ddl  |= handle->cmd == CMD_DDL;
				blocking[num_blocking++] = pasi->curr_stmt;
			}

			if (num_blocking == 0)
				break;

			/* set timeout for distributed deadlock detect */
			if (is_write && !enable_timeout)
			{
				enable_timeout = true;
				enable_timeout_after(WRITE_SHARD_RESULT_TIMEOUT,
						     start_global_deadlock_detection_wait_timeout);
			}

			/* wait for the results from any of the currently blocking stmts */
			poll_remote_events_any(blocking, num_blocking, 1000);
			num_blocking = 0;

			CHECK_FOR_INTERRUPTS();
			/* interrupt may be hold,*/
			if (cancel && QueryCancelPending && InterruptHoldoffCount > 0)
			{
				if (checkCount == 0)
				{
					ShardConnKillReq *req = makeShardConnKillReq(2 /*kill query*/);
					if (req)
					{
						appendShardConnKillReq(req);
						pfree(req);
					}
					checkCount = 1000;
				}
				checkCount -= 1;
			}
		}
		if (enable_timeout)
			disable_timeout(WRITE_SHARD_RESULT_TIMEOUT, false);
	}
	PG_CATCH();
	{
		if (geterrcode() == ERRCODE_QUERY_CANCELED)
		{
			/* No need to send kill here, it is done in ProecessInterrupts() */

			/* Maybe the network to shard is broken, and topo is changed */
			request_topo_checks_used_shards();

			/*
			  if statement_timeout set smaller than start_global_deadlock_detection_wait_timeout,
			  this helps to inform gdd in time.
			*/
			if (is_write)
				kick_start_gdd();

			if (is_ddl)
			{
				StringInfoData stmt_str;
				initStringInfo2(&stmt_str, 512, TopTransactionContext);
				CurrentStatementsShards(&stmt_str);
				ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Kunlun-db: Current timed out statement is a DDL, so there could be"
						"unrevokable effects(i.e. leftover tables/databases, etc) in "
						"target storage shards."),
					 errhint("DBAs should manually check all target shards and "
						 "clear leftover effects. DDL statements in all target shards: %*s.",
						 stmt_str.len, stmt_str.data)));
			}
		}

		/* Cancel all statements in the queue */
		cancel_all_stmts();
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/**
 * @brief The common logic of getting next row of the statement
 */
static MYSQL_ROW
get_stmt_next_row_common(StmtHandle *handle, bool wait)
{
	Assert(handle->asi);
	AsyncStmtInfo *asi = handle->asi;
	if (is_stmt_eof(SAFE_HANDLE(handle)))
		return NULL;

	/* read from materialize cache */
	if (handle->read_cache)
	{
		if (handle->reset_cache_pos)
		{
			handle->reset_cache_pos = false;
			matcache_set_read_pos(handle->cache, handle->cache_pos);
		}
		if (!matcache_eof(handle->cache))
			return next_row_from_matcache(handle);
		/* Reach the EOF of matcache, try read from socket again ? */
		handle->read_cache = false;

		/* This is a temporary one time materializtion, reset it*/
		if (handle->support_rewind == false)
			matcache_reset(handle->cache);
		
		/* Try read socket again, maybe some there are tuples not materialized yet */
		return get_stmt_next_row_common(handle, wait);
	}

	/* if the connection is broken, just return NULL */
	if (!ASIConnected(handle->asi))
	{
		AsyncStmtInfo *asi = handle->asi;
		flush_invalid_stmts(asi);
		ereport(ERROR,
			(errcode(ERRCODE_CONNECTION_EXCEPTION),
			 errmsg("Kunlun-db: Connection with MySQL storage node (%u, %u) is gone. Resend the statement.",
				asi->shard_id, asi->node_id)));
	}

	/* urge processing of  preceding statments */
	if (asi->curr_stmt != handle)
	{
		while (process_preceding_stmts(asi, handle) == false)
		{
			/* return if not wait */
			if (!wait)
				return NULL;

			poll_remote_events_any(&asi->curr_stmt, 1, 1000);
			CHECK_FOR_INTERRUPTS();
		}
	}

	/* Send statement if not send it yet */
	if (!asi->curr_stmt)
		send_stmt_impl(asi, handle);

	Assert(asi->curr_stmt == handle);
	/* wait until recv next tuple from the socket */
	while (true)
	{
		CHECK_FOR_INTERRUPTS();
		if (recv_stmt_result_impl(asi, handle) == false)
		{
			/* just return if not no wait*/
			if (!wait)
				return NULL;
			poll_remote_events_any(&asi->curr_stmt, 1, 1000);
			continue;
		}

		/* EOF */
		if (handle->finished)
			return NULL;

		/* Non-select/returning cmd */
		if (handle->row == NULL)
			continue;

		if (handle->support_rewind)
			materialize_current_tuple(asi, handle);
		return handle->row;
	}

	return NULL;
}

MYSQL_ROW
get_stmt_next_row(StmtSafeHandle handle)
{
	CHECK_HANDLE_EPOCH(handle);
	return get_stmt_next_row_common(handle.handle, true);
}

MYSQL_ROW
try_get_stmt_next_row(StmtSafeHandle handle)
{
	CHECK_HANDLE_EPOCH(handle);
	return get_stmt_next_row_common(handle.handle, false);
}

enum enum_field_types *
get_stmt_field_types(StmtSafeHandle handle)
{
	CHECK_HANDLE_EPOCH(handle);
	return handle.handle->types;
}

size_t *get_stmt_row_lengths(StmtSafeHandle handle)
{
	CHECK_HANDLE_EPOCH(handle);
	return handle.handle->lengths;
}

int32 get_stmt_affected_rows(StmtSafeHandle handle)
{
	CHECK_HANDLE_EPOCH(handle);
	return handle.handle->affected_rows;
}

static bool
stmt_handles_member(StmtSafeHandle *handles, int size, StmtHandle *handle)
{
	for (int i = 0; i < size; ++i)
		if (handles[i].handle == handle)
			return true;
	return false;
}

StmtSafeHandle
wait_for_readable_stmt(StmtSafeHandle *handles, int size)
{
	StmtHandle *handle, *best = NULL;
	StmtHandle *runing_stmts[size];
	StmtHandle *pending_stmts[size];
	int num_runing_stmts = 0, num_pending_stmts = 0;
	ListCell *lc;

	for(int i=0; i<size; ++i)
	{
		CHECK_HANDLE_EPOCH(handles[i]);	
		if (is_stmt_eof(handles[i]))
			continue;
		
		handle = handles[i].handle;
		/* it has been materialized, read it ! */
		if (handle->read_cache &&
		    handle->cache &&
		    !matcache_eof(handle->cache))
		{
			best = handle;
		}
		/* it is stmts currently runing and waiting for no events */
		else if (handle == handle->asi->curr_stmt)
		{
			if (!best && (handle->status_req == 0 || (handle->status_req & handle->status)))
				best = handle;

			if (handle->status_req)
				runing_stmts[num_runing_stmts++] = handle;
		}
		else if (handle->asi->curr_stmt == 0 ||
			 !stmt_handles_member(handles, size, handle->asi->curr_stmt))
		{
			foreach (lc, handle->asi->stmt_queue)
			{
				if (lfirst(lc) == handle)
				{
					pending_stmts[num_pending_stmts++] = handle;
					break;
				}
				/* prevent expensive materialization */
				if (stmt_handles_member(handles, size, lfirst(lc)))
					break;
			}
		}
	}

	/* Send the stmts if connection is idle */
	int i = 0;
	while (i < num_pending_stmts)
	{
		handle = pending_stmts[i];
		if (handle->asi->curr_stmt == NULL &&
		    linitial(handle->asi->stmt_queue) == handle)
		{
			send_stmt_impl(handle->asi, handle);
			runing_stmts[num_runing_stmts++] = handle;
			pending_stmts[i] = pending_stmts[--num_pending_stmts];
		}
		else
		{
			++i;
		}
	}

	while (!best)
	{
		/* waiting for result from any runing statments */
		if (num_runing_stmts > 0)
		{
			while (!(best = poll_remote_events_any(runing_stmts, num_runing_stmts, 1000)))
			{
				/* check for pending interruption */
			}
			break;
		}
		else if (num_pending_stmts > 0)
		{
			StmtHandle *blocking[size];
			i = 0;
			while (i < num_pending_stmts)
			{
				handle = pending_stmts[i];
				/* process the precding stmts to get a idle connection*/
				if (process_preceding_stmts(handle->asi, handle))
				{
					send_stmt_impl(handle->asi, handle);
					runing_stmts[num_runing_stmts++] = handle;
					pending_stmts[i] = pending_stmts[--num_pending_stmts];
				}
				else
				{
					blocking[i] = handle->asi->curr_stmt;
					++i;
				}
			}

			if (num_runing_stmts == 0)
			{
				poll_remote_events_any(blocking, num_pending_stmts, 1000);
			}
		}
		else
		{
			break;
		}
	}

	return SAFE_HANDLE(best);
}

/**
 * @brief Materialize ulong value
 */
static void
matcache_store_ulong(MatCache *cache, size_t value)
{
	uint8_t u8 = value;
	if (value < 251) /* 1 byte */
	{
		matcache_write(cache, (uchar*)&u8, sizeof(u8));
	}
	else if (value < 0xffff) /* 3 byte */
	{
		uint16_t u16 = value;
		u8 = 251;
		matcache_write(cache, (uchar*)&u8, sizeof(u8));
		matcache_write(cache, (uchar*)&u16, sizeof(u16));
	}
	else
	{
		u8 = 252;
		matcache_write(cache, (uchar*)&u8, sizeof(u8));
		matcache_write(cache, (uchar*)&value, sizeof(value));
	}
}

static size_t
matcache_read_ulong(MatCache *cache)
{
	uint8_t u8;
	matcache_read(cache, (uchar*)&u8, sizeof(u8));
	if (u8 < 251)
	{
		return u8;
	}
	else if (u8 == 251)
	{
		uint16_t u16;
		matcache_read(cache, (uchar*)&u16, sizeof(u16));
		return u16;
	}
	else
	{
		size_t value;
		Assert(u8 == 252);
		matcache_read(cache, (uchar*)&value, sizeof(value));
		return value;
	}
}

static int
serialize_ulong(size_t value, char *buff)
{
	if (value < 251)
	{
		buff[0] = (char)value;
		return 1;
	}
	else if (value < 0xffff)
	{
		buff[0] = 251;
		*(uint16_t *)(buff + 1) = (uint16_t)value;
		return 3;
	}
	else
	{
		buff[0] = 252;
		*(size_t *)(buff + 1) = value;
		return 1 + sizeof(value);
	}
}

static size_t
deserialize_ulong(char **pptr)
{
	size_t ret = 0;
	char *ptr = *pptr;
	if (*ptr < 251)
	{
		ret = *(uint8_t *)ptr++;
	}
	else if (*ptr == 251)
	{
		++ptr;
		ret = *(uint16_t *)ptr;
		ptr += 2;
	}
	else
	{
		Assert(*ptr == 252);
		++ptr;
		ret = *(size_t *)ptr;
		ptr += sizeof(size_t);
	}
	*pptr = ptr;
	return ret;
}

/**
 * @brief Get the next row from the materialized cache
 */
static MYSQL_ROW
next_row_from_matcache(StmtHandle *handle)
{
	size_t len, lenlen, rowlen;
	StringInfo buff = &handle->read_buff;
	resetStringInfo(buff);

	len = matcache_read_ulong(handle->cache);

	/* alloc enough memory */
	lenlen = sizeof(size_t) * handle->field_count;
	rowlen = sizeof(char*) * handle->field_count;
	enlargeStringInfo(buff, len + lenlen + rowlen);

	matcache_read(handle->cache, (uchar*)buff->data, len);
	handle->lengths = (size_t*)(buff->data + len);
	handle->row = (char**)(buff->data + len  + lenlen);

	char *ptr = buff->data;
	for (int i = 0; i < handle->field_count; ++i)
	{
		len = deserialize_ulong(&ptr);
		handle->row[i] = (len == 0 ? NULL : ptr);
		ptr += len;
		handle->lengths[i] = (len == 0 ? len : len - 1);
	}
	return handle->row;
}

/**
 * @brief Materialize currently fetched tuple
 */
static void
materialize_current_tuple(AsyncStmtInfo *asi, StmtHandle *handle)
{
	Assert(asi->curr_stmt == handle);
	Assert(handle->res && handle->row);
	if (!handle->cache)
	{
		handle->cache = matcache_create();
		Assert(matcache_mode(handle->cache) == WRITE_MODE);
		initStringInfo2(&handle->read_buff, 128, TopMemoryContext);
		initStringInfo2(&handle->buff, 128, TopMemoryContext);
	}
	resetStringInfo(&handle->buff);

	size_t len;
	StringInfo buff = &handle->buff;
	char lenBuff[sizeof(size_t) + 1];
	int lenBuffSize;
	
	for (int i = 0; i < handle->field_count; ++i)
	{
		len = handle->lengths[i] + (handle->row[i] ? 1 : 0);
		lenBuffSize = serialize_ulong(len, lenBuff);
		appendBinaryStringInfo(buff, lenBuff, lenBuffSize);
		if (len == 0)
			continue;
		appendBinaryStringInfo(buff, handle->row[i], len);
	}

	len = buff->len;
	matcache_store_ulong(handle->cache, len);
	matcache_write(handle->cache, (uchar *)buff->data, buff->len);
}

bool is_stmt_rewindable(StmtSafeHandle handle)
{
	CHECK_HANDLE_EPOCH(handle);
	return RAW_HANDLE(handle)->support_rewind;
}

void stmt_rewind(StmtSafeHandle h)
{
	CHECK_HANDLE_EPOCH(h);
	StmtHandle *handle = h.handle;
	Assert(handle->support_rewind);
	if (handle->cache)
	{
		handle->read_cache = true;
		handle->reset_cache_pos = true;
		handle->cache_pos.fileno = 0;
		handle->cache_pos.offset = 0;
		matcache_set_read_pos(handle->cache, handle->cache_pos);
	}
}

void release_stmt_handle(StmtSafeHandle h)
{
	if (HANDLE_EXPIRED(h))
		return;
	StmtHandle *handle = h.handle;
	--handle->refcount;
	if (handle->refcount == 0)
	{
		handle->asi->stmt_inuse =  list_delete_ptr(handle->asi->stmt_inuse, handle);
		Assert(handle->finished);
		if (handle->cache)
			matcache_close(handle->cache);
		if (handle->types)
			pfree(handle->types);
		// The memory context may have been released 
		// if (handle->owns_stmt_mem)
		// 	pfree(handle->stmt);
		pfree(handle);
	}
}

/**
 * @brief Same as send_stmt_async(), but don't  care about the result unless some error occurs
 */
void send_stmt_async_nowarn(AsyncStmtInfo *asi, char *stmt,
			    size_t stmt_len, CmdType cmd, bool ownsit, enum enum_sql_command sqlcom)
{
	StmtSafeHandle handle = send_stmt_async(asi, stmt, stmt_len, cmd, ownsit, sqlcom, false);
	release_stmt_handle(handle);

	/* throw error if the connection is invalid*/
	if (ASIConnected(asi) == false)
	{
		flush_invalid_stmts(asi);
		ereport(ERROR,
			(errcode(ERRCODE_CONNECTION_EXCEPTION),
			 errmsg("Kunlun-db: Connection with MySQL storage node (%u, %u) is gone. Resend the statement.",
				asi->shard_id, asi->node_id)));
	}
}

/**
 * @brief Send statement and waiting for the result synchronously
 *
 *  throw error if connection is invalid or other errors happened
 */
void send_remote_stmt_sync(AsyncStmtInfo *asi, char *stmt, size_t len,
			   CmdType cmdtype, bool owns_it, enum enum_sql_command sqlcom, int ignore_err)
{
	StmtSafeHandle handle = INVALID_STMT_HANLE;
	int old = set_stmt_ignored_error(ignore_err);
	PG_TRY();
	{
		handle = send_stmt_async(asi, stmt, len, cmdtype, owns_it, sqlcom, false);
		set_stmt_ignored_error(old);
	}
	PG_CATCH();
	{
		set_stmt_ignored_error(old);
		if (stmt_handle_valid(handle))
			release_stmt_handle(handle);

		PG_RE_THROW();
	}
	PG_END_TRY();
	release_stmt_handle(handle);

	/* throw error if the connection is invalid*/
	if (ASIConnected(asi) == false)
	{
		flush_invalid_stmts(asi);
		ereport(ERROR,
			(errcode(ERRCODE_CONNECTION_EXCEPTION),
			 errmsg("Kunlun-db: Connection with MySQL storage node (%u, %u) is gone. Resend the statement.",
				asi->shard_id, asi->node_id)));
	}

	flush_all_stmts_impl(&asi, 1, false);
}

/**
 * @brief Send statement to all of the connections in use and waiting for the result synchronously
 *
 *  throw error if a connection is invalid or other errors happened
 */
void send_stmt_to_all_inuse_sync(char *stmt, size_t len, CmdType cmdtype, bool owns_it,
				 enum enum_sql_command sqlcom, bool written_only)
{
	size_t cnt = cur_session.num_asis_used, num = 0;
	AsyncStmtInfo *asi = cur_session.asis;
	AsyncStmtInfo *used_asis[cnt];

	for (int i = 0; i < cnt; ++i, ++asi)
	{
		/*
		  if the connection to storage shard is closed, no need to abort the txn;
		  if no stmt was yet executed, we have no txn to abort --- this could
		  happen if another channel got error and we need to abort current txn.
		  All of these two cases can be detected by check asi->executed_stmts,
		  because we always reset asi when got disconnected error , which will
		  set executed_stmts to zero.
		*/
		if (sqlcom == SQLCOM_XA_ROLLBACK && asi->executed_stmts == 0)
			continue;

		if (written_only && !asi->did_write)
			continue;

		if (cmdtype == CMD_TXN_MGMT && ASIConnected(asi)  && !asi->txn_in_progress)
			continue;

		/* add it to asi's stmt queue  */
		send_stmt_async_nowarn(asi, stmt, len, cmdtype, owns_it, sqlcom);

		used_asis[num++] = asi;
	}

	/* Do synchronously wait*/
	flush_all_stmts_impl(used_asis, num, false);
}

/**
 * @brief Send statement to all of the shards and waiting for the result synchronously
 *
 *  throw error if a connection is invalid or other errors happened
 */
void send_stmt_to_all_shards_sync(char *stmt, size_t len, CmdType cmdtype, bool owns_it,
				  enum enum_sql_command sqlcom)
{
	ListCell *lc;
	List *shards = GetAllShardIds();
	const int cnt = list_length(shards);
	AsyncStmtInfo *used_asis[cnt];

	int i = 0;
	foreach (lc, shards)
	{

		AsyncStmtInfo *asi = GetAsyncStmtInfo(lfirst_oid(lc));
		send_stmt_async_nowarn(asi, stmt, len, cmdtype, owns_it, sqlcom);
		used_asis[i++] = asi;
	}

	flush_all_stmts_impl(used_asis, cnt, false);
}

/**
 * @brief Send all of the statements in queue and  waiting for the result synchronously
 *
 *  throw error if a connection is invalid or other errors happened
 */
void flush_all_stmts()
{
	const size_t count = cur_session.num_asis_used;
	AsyncStmtInfo *pasi = cur_session.asis;
	AsyncStmtInfo *asi_vec[count];

	int len = 0;
	for (int i = 0; i < count; ++i, ++pasi)
	{
		if (pasi->curr_stmt || list_length(pasi->stmt_queue) > 0)
		{
			if (ASIConnected(pasi) == false)
			{
				flush_invalid_stmts(pasi);
				ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("Kunlun-db: Connection with MySQL storage node (%u, %u) is gone. Resend the statement.",
						pasi->shard_id, pasi->node_id)));
			}
			asi_vec[len++] = pasi;
		}
	}

	if (len > 0)
		flush_all_stmts_impl(asi_vec, len, false);
}

/**
 * @brief Marked statement to be canceled, and no longer cares whether it executes
 *    or its returned result (which may still be executed).
 *
 * @param handle
 */
void cancel_stmt_async(StmtSafeHandle h)
{
	CHECK_HANDLE_EPOCH(h);
	StmtHandle *handle = RAW_HANDLE(h);
	Assert(handle->refcount > 0);
	handle->cancel = true;
}

/**
 * @brief Marked statement to be canceled, and free all the results remained in the sockets
 * 
 * @param handle 
 */
void cancel_stmt(StmtSafeHandle h)
{
	if (HANDLE_EXPIRED(h))
		return;
	StmtHandle *handle = RAW_HANDLE(h);
	handle->cancel = true;

	/* free cache */
	handle->read_cache = false;
	handle->support_rewind = false;
	if (handle->cache)
	{
		matcache_close(handle->cache);
		handle->cache = NULL;
	}

	/* Check if it is currently running statment */
	if (is_stmt_eof(h) || handle->asi->curr_stmt != handle)
		return;

	MYSQL_ROW row = get_stmt_next_row_common(handle, true);

	Assert(row == NULL);
}

/**
 * @brief Cancels all statements waiting in the queue and waits for the currently running
 *  statement to complete.
 *
 *  This is used internally and is mainly used when the asi is finally cleaned up, so even if
 *  the running stmt eventually reports an error, or even if the connection is disconnected,
 *  no exception will be thrown.
 */
static void
cancel_all_stmts_impl(AsyncStmtInfo *asi[], int cnt)
{
	static int deep = 0;
	++deep;
	if (deep > 1)
	{
		/*Avoid recursive call*/
		--deep;
		return;
	}

	AsyncStmtInfo *active[cnt];
	int num = 0;
	for (int i = 0; i < cnt; ++i)
	{
		AsyncStmtInfo *pasi = asi[i];
		ListCell *lc;
		foreach (lc, pasi->stmt_queue)
		{
			StmtHandle *handle = lfirst(lc);
			handle->cancel = true;
			handle->finished = true;
			release_stmt_handle(SAFE_HANDLE(handle));
		}
		list_free(pasi->stmt_queue);
		pasi->stmt_queue = NIL;

		if (pasi->curr_stmt)
		{
			pasi->curr_stmt->cancel = true;
			active[num++] = pasi;
		}
	}

	/* make a copy to top error before loop */
	ErrorData *errdata = NULL;
	MemoryContext saved;
	if (top_errcode())
	{
		saved = MemoryContextSwitchTo(TopMemoryContext);
		errdata = CopyErrorData();
		MemoryContextSwitchTo(saved);
	}

	bool retry = false;
	do
	{
		retry = false;
		PG_TRY();
		{
			if (num > 0)
				flush_all_stmts_impl(active, num, true);
		}
		PG_CATCH();
		{
			FlushErrorState();
			retry = true;
		}
		PG_END_TRY();
	} while (retry);

	--deep;

	/* Restore the top error information */
	if (errdata)
	{
		PG_TRY();
		{
			FlushErrorState();
			ReThrowError(errdata);
		}
		PG_END_TRY();
		FreeErrorData(errdata);
	}
}

/**
 * @brief Like cancel_all_stmts_impl(), cancels all the statements in asi currently in use,
 *  but does not need to pass in the asi array.
 */
void cancel_all_stmts()
{
	const size_t count = cur_session.num_asis_used;
	AsyncStmtInfo *pasi = cur_session.asis;
	AsyncStmtInfo *asis[count];

	for (int i = 0; i < count; ++i, ++pasi)
	{
		asis[i] = pasi;
	}

	cancel_all_stmts_impl(asis, count);
}

static void
close_remote_conn(AsyncStmtInfo *asi)
{
	if (asi->curr_stmt && asi->curr_stmt->res)
	{
		Assert(asi->conn);
		mysql_free_result(asi->curr_stmt->res);
		asi->curr_stmt->res = NULL;
	}

	if (asi->conn)
		mysql_close(asi->conn);
	MarkConnValid(asi, false);
	flush_invalid_stmts(asi);
}
/*
 * Abort txns on all connections to storage node, and clear the MYSQL* ptr in
 * ShardConnection.
 * */
static void handle_backend_disconnect(AsyncStmtInfo *asi0)
{
	AsyncStmtInfo *asi;
	/*
	 * Close all in-use connections to storage nodes, and mark them invalid.
	 * */
	for (int i = 0; i < cur_session.num_asis_used; i++)
	{
		asi = cur_session.asis + i;
		if (asi->conn)
		{
			close_remote_conn(asi);
		}

		/*
		 * Disconnection auto aborts current txn, except if current stmt just
		 * executed is XA PREPARE.
		 * If we send XA ROLLBACK when last executed
		 * isn't 'XA PREPARE', we'd make new connection and the XA ROLLBACK
		 * will cause error: unknown xid.

		 * If we sent XA PREPARE to some conns, here we don't know whether they
		 * have succeeded, and since we've either reset or disconnected such
		 * conns, it's safe to skip the abort and let cluster_manager abort.

		 * For the broken channel, let cluster_manager abort the prepared txn
		 * branch if it's already prepared. we don't know here whethe it's
		 * prepared or not.
		 */
		ResetASI(asi);
	}
	/* invalid all handles not released yet*/
	++ handle_epoch;
}

static void
handle_stmt_error(AsyncStmtInfo *asi, StmtHandle *handle, int eno)
{
	MYSQL *conn = asi->conn;
	Oid shardid = asi->shard_id;
	Oid nodeid = asi->node_id;

	if (eno == 0)
		return;

	static char errmsg_buf[512];
	errmsg_buf[0] = '\0';
	strncat(errmsg_buf, mysql_error(conn), sizeof(errmsg_buf) - 1);

	/* Mark the EOF of the statement */
	handle->finished = true;

	/*
	 * Only break the connection for client errors. Errors returned by server
	 * caused by sql stmt simply report to client, those are not caused by
	 * the connection.
	 * */
	if (IS_MYSQL_CLIENT_ERROR(eno))
	{
		if (eno == CR_SERVER_LOST || eno == CR_SERVER_GONE_ERROR)
		{
			ShardConnKillReq *req = makeShardConnKillReq(1 /*kill conn*/);
			if (req)
			{
				appendShardConnKillReq(req);
				pfree(req);
			}
		}

		/* Close all of the connections used in txn */
		handle_backend_disconnect(asi);
	}

	if (eno == CR_SERVER_LOST ||
	    eno == CR_SERVER_GONE_ERROR ||
	    eno == CR_SERVER_HANDSHAKE_ERR)
	{
		if (GetShardMasterNodeId(asi->shard_id) == asi->node_id)
			RequestShardingTopoCheck(asi->shard_id);

		ereport(ERROR,
			(errcode(ERRCODE_CONNECTION_EXCEPTION),
			 errmsg("Kunlun-db: Connection with MySQL storage node (%u, %u) is gone: %d, %s. Resend the statement.",
				shardid, nodeid, eno, errmsg_buf),
			 errdetail_internal("Disconnected all connections to MySQL storage nodes.")));
	}
	else if (eno == CR_UNKNOWN_ERROR)
		ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("Kunlun-db: Unknown MySQL client error from MySQL storage node (%u, %u) : %d, %s. Resend the statement.",
				shardid, nodeid, eno, errmsg_buf),
			 errdetail_internal("Disconnected all connections to MySQL storage nodes.")));
	else if (eno == CR_COMMANDS_OUT_OF_SYNC)
		ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("Kunlun-db: Command out of sync from MySQL storage node (%u, %u) : %d, %s. Resend the statement.",
				shardid, nodeid, eno, errmsg_buf),
			 errdetail_internal("Disconnected all connections to MySQL storage nodes.")));
	else if (IS_MYSQL_CLIENT_ERROR(eno))
		ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("Kunlun-db: MySQL client error from MySQL storage node (%u, %u) : %d, %s. Resend the statement.",
				shardid, nodeid, eno, errmsg_buf),
			 errdetail_internal("Disconnected all connections to MySQL storage nodes.")));
	else if (eno == ER_OPTION_PREVENTS_STATEMENT &&
		 (strcasestr(errmsg_buf, "--read-only") ||
		  strcasestr(errmsg_buf, "--super-read-only")))
	{
		/* It's fragile to rely on error message text for precise error
		 * conditions, but this is all we have from mysql client. If in future
		 * the error message changes, we must respond to that. */
		RequestShardingTopoCheck(asi->shard_id);
		handle_backend_disconnect(asi);

		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: MySQL storage node (%u, %u) is no longer a primary(%d, %s), . Retry the transaction.",
						shardid, nodeid, eno, errmsg_buf),
				 errhint("Primary info will be refreshed automatically very soon. "),
				 errdetail_internal("Disconnected all connections to MySQL storage nodes.")));
	}
	else if (eno != handle->ignore_errno)
		ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("Kunlun-db: MySQL storage node (%u, %u) returned error: %d, %s.",
				shardid, nodeid, eno, errmsg_buf)));
	else
		elog(WARNING, "MySQL storage node (%u, %u) returned error: %d, %s, and it's taken as a WARNING.",
		     shardid, nodeid, eno, errmsg_buf);
}

/*
 * Whether a mysql query has been executed in current transaction.
 * */
bool MySQLQueryExecuted()
{
	AsyncStmtInfo *asi = cur_session.asis;
	for (int i = 0; i < cur_session.num_asis_used; i++, asi++)
	{
		if (asi->executed_stmts || asi->did_read || asi->did_write || asi->did_ddl)
			return true;
	}
	return false;
}

static void
check_mysql_node_status(AsyncStmtInfo *asi, bool want_master)
{
	Storage_HA_Mode ha_mode = storage_ha_mode();
	Assert(ha_mode != HA_NO_REP);

	const char *stmt_mgr = "SELECT MEMBER_HOST, MEMBER_PORT  FROM performance_schema.replication_group_members "
			       " WHERE channel_name = 'group_replication_applier' AND MEMBER_STATE = 'ONLINE'  AND MEMBER_ROLE = 'PRIMARY'";
	const char *stmt_rbr = "select HOST, PORT from performance_schema.replication_connection_configuration where channel_name='kunlun_repl'";

	const char *stmt = NULL;
	if (ha_mode == HA_MGR)
		stmt = stmt_mgr;
	else if (ha_mode == HA_RBR)
		stmt = stmt_rbr;
	else
	{
		Assert(ha_mode == HA_NO_REP);
		return;
	}

	size_t stmtlen = strlen(stmt);
	StmtSafeHandle handle = send_stmt_async(asi, (char*)stmt, stmtlen, CMD_SELECT, false, SQLCOM_SELECT, false);
	MYSQL_ROW row = get_stmt_next_row(handle);
	bool res = true;

	if (!row || row[0] == NULL || row[1] == NULL)
	{
		res = (ha_mode == HA_RBR ? true : false);
	}
	else
	{
		char *endptr = NULL;
		int port = strtol(row[1], &endptr, 10);
		if (strcmp(asi->conn->host, row[0]) || port != asi->conn->port)
			res = false; // current master not what's connected to by asi.
	}

	release_stmt_handle(handle);

	if (!res)
	{
		RequestShardingTopoCheck(asi->shard_id);
		ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("Kunlun-db: Shard (%u) primary node(%u) is not primary node now, retry in a few seconds.",
				asi->shard_id, asi->node_id)));
	}
}

void disconnect_storage_shards()
{
	for (int i = 0; i < cur_session.num_asis_used; i++)
	{
		AsyncStmtInfo *asi = cur_session.asis + i;

		if (asi->node_id == 0 || asi->shard_id == 0 || !IsConnValid(asi))
			continue;
		
		close_remote_conn(asi);
		
		MarkConnValid(asi, false);
		ResetASI(asi);
	}
	/* invalid all handle out of this module */
	handle_epoch ++;
}

void request_topo_checks_used_shards()
{
	for (int i = 0; i < cur_session.num_asis_used; i++)
	{
		AsyncStmtInfo *asi = cur_session.asis + i;
		RequestShardingTopoCheck(asi->shard_id);
	}
}

uint64_t GetTxnRemoteAffectedRows()
{
	uint64_t num_rows = 0;
	for (int i = 0; i < cur_session.num_asis_used; i++)
	{
		AsyncStmtInfo *asi = cur_session.asis + i;
		num_rows += asi->txn_wrows;
	}
	return num_rows;
}

Oid GetCurrentNodeOfShard(Oid shard)
{
	for (int i = 0; i < cur_session.num_asis_used; i++)
	{
		AsyncStmtInfo *asi = cur_session.asis + i;
		if (asi->shard_id == shard)
			return asi->node_id;
	}
	return Invalid_shard_node_id;
}

static void CurrentStatementsShards(StringInfo str)
{
	for (int i = 0; i < cur_session.num_asis_used; i++)
	{
		AsyncStmtInfo *asi = cur_session.asis + i;
		if (asi->curr_stmt)
		{
			appendStringInfo(str, "{shard_id: %u, SQL_statement: %*s}",
					 asi->shard_id, (int)asi->curr_stmt->stmt_len, asi->curr_stmt->stmt);
		}
	}
}
