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
#include "sharding/sharding_conn.h"
#include "sharding/sharding.h"
#include "sharding/mysql_vars.h"
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
#include <unistd.h>
#include <poll.h>
#include <limits.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

extern Oid comp_node_id;

// GUC vars.
// seconds waiting for connect done.
int mysql_connect_timeout = 10;
/*
 * Seconds waiting for read/write done, real wait time is 3/2 times this value
 * according to doc because of read/write retries.
 */
int mysql_read_timeout = 10;
int mysql_write_timeout = 10;
int mysql_max_packet_size = 16384;
bool mysql_transmit_compress = false;

static void send_stmt_to_multi(AsyncStmtInfo *asis, size_t shard_cnt);
static void send_stmt_to_multi_wait(AsyncStmtInfo *asis, size_t shard_cnt);
static void ResetASIStmt(AsyncStmtInfo *asi, bool ended_clean);
static void ResetASI(AsyncStmtInfo *asi);
static bool async_connect(MYSQL *mysql, const char *host, uint16_t port, const char *user, const char *password);
static ShardConnection*GetConnShard(Oid shardid);
static int wait_for_mysql(MYSQL *mysql, int status);
static ShardConnection *AllocShardConnSlot(Oid shardid, int inspos);
static int AllocShardConnNodeSlot(ShardConnection *sconn, Oid nodeid, int *newconn, bool req_chk_onfail);
static void handle_backend_disconnect(AsyncStmtInfo *asi);
static int wait_for_mysql_multi(AsyncStmtInfo *asi, size_t len, struct pollfd *pfds, int timeout_ms);
static void handle_mysql_result(AsyncStmtInfo *pasi);
static void handle_mysql_error(int ret, AsyncStmtInfo *asi);
static MYSQL *GetConnShardNode(Oid shardid, Oid nodeid, int *newconn, bool req_chk_onfail);
static MYSQL *GetConnShardMaster(Oid shardid, int *newconn);
static bool ConnHasFlag(AsyncStmtInfo *asi, int flagbit);
static bool MarkConnFlag(AsyncStmtInfo *asi, int flagbit, bool b);

static void
check_mysql_node_status(AsyncStmtInfo *asi, bool want_master);
static void
make_check_mysql_node_status_stmt(AsyncStmtInfo *asi, bool want_master);
static bool DoingDDL(void);
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
	{
		asi->conn = NULL;
		asi->result_pending = false;
	}
	return MarkConnFlag(asi, CONN_VALID, valid);
}

/*
 * The mapping of postgreSQL database name and schema name to mysql database name:
 * use dbname-schemaname as mysql db name. the max length is 192 bytes in mysql side.
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
	StringInfoData qname;

	initStringInfo(&qname);

	get_database_name2(MyDatabaseId, &qname);
	appendStringInfoString(&qname, "_$$_");

	get_namespace_name2(nspid, &qname);
	if (objname)
		appendStringInfo(&qname, ".%s", objname);

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
	ShardConnSection all_shard_conns; // 1st section
	ShardConnSection *last_section;   // ptr to last section.

	/*
	 * remote stmt send&result recv facility.
	 * */

	AsyncStmtInfo *asis;
	int num_asis_used; // NO. of slots used in current stmt in 'asis'.
	int num_asis; // total NO. of slots in 'asis'.
} ShardingSession;

static ShardingSession cur_session;

void InitShardingSession()
{
	cur_session.last_section = &cur_session.all_shard_conns;
	init_var_cache();
	/*
	  Make sure conns are always closed.
	*/
	before_shmem_exit(disconnect_request_kill_shard_conns, 0);
}

static void disconnect_request_kill_shard_conns(int i, Datum arg)
{
	disconnect_storage_shards();
	ShardConnKillReq *req = makeShardConnKillReq(1/*kill conn*/);
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
	AsyncStmtInfo *asi = NULL;

	if (shardNodeId == InvalidOid)
	{
		shardNodeId = GetShardMasterNodeId(shardid);
		/*
		  Iff shardNodeId == InvalidOid do caller want to connect to the
		  shard's master node. Otherwise caller simply want to connect to
		  the node, so don't check it's a master.
		 */
		if (ha_mode != HA_NO_REP) want_master = true;
	}

	for (int i = 0; i < cur_session.num_asis_used; i++)
	{
		AsyncStmtInfo *pasi = cur_session.asis + i;
		if (pasi->shard_id == shardid && pasi->node_id == shardNodeId)
		{
			if (pasi->conn != NULL)
				return pasi;
			else
			{
				asi = pasi;
				goto make_conn;
			}
		}
	}

	if (cur_session.num_asis_used == cur_session.num_asis)
	{
		cur_session.asis = repalloc(cur_session.asis, sizeof(AsyncStmtInfo) * cur_session.num_asis * 2);
		memset(cur_session.asis + cur_session.num_asis, 0, cur_session.num_asis * sizeof(AsyncStmtInfo));
		cur_session.num_asis *= 2;
	}

	asi = cur_session.asis + cur_session.num_asis_used++;
	Assert(asi->conn == NULL && asi->shard_id == InvalidOid && asi->node_id == InvalidOid);
make_conn:
	asi->conn = GetConnShardNode(shardid, shardNodeId, &newconn, req_chk_onfail);
	asi->shard_id = shardid;
	asi->node_id = shardNodeId;
	asi->need_abort = true;
	asi->rss_owner = NULL;

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
			append_async_stmt(asi, setvar_stmt, setvar_stmtlen, CMD_UTILITY,
							  true, SQLCOM_SET_OPTION);

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

		append_async_stmt(asi, cmdbuf, cmdlen, CMD_UTILITY, false, SQLCOM_SET_OPTION);

		// must append this last in one packet of sql stmts.
		if (want_master) make_check_mysql_node_status_stmt(asi, want_master);

		/*
		 * We have to send the set stmts now because immediately next stmt maybe a DDL
		 * */
		send_stmt_to_multi(asi, 1);

		if (want_master) check_mysql_node_status(asi, want_master);

		/*
		 * Make the communication port brandnew. this is crucial, without this
		 * operation, XA START won't be correctly generated and sent at the
		 * start of a txn branch.
		 * */
		ResetASIStmt(asi, true);
		asi->result_pending = false;
		asi->executed_stmts = 0;
		asi->need_abort = true;
		asi->did_write = asi->did_read = asi->did_ddl = false;

		/*
		 * Now we've set/sent session status to the mysql connection, we can
		 * clear the CONN_RESET bit.
		 * */
		MarkConnReset(asi, false);
		// MarkConnValid(asi, true); marked already
	}

	Assert(IsConnValid(asi) && !IsConnReset(asi));

	return asi;
}

/*
 * Called at start of each txn to totally cleanup all channels used by prev txn.
 * */
void ResetCommunicationHub()
{
	for (int i = 0; i < cur_session.num_asis_used; i++)
	{
		ResetASI(cur_session.asis+i);
	}
	cur_session.num_asis_used = 0;
	// cur_session.num_asis/asis must be valid throughout the session.
}

// called at end(or start) of a stmt to reset certain states but keep some other states.
void ResetCommunicationHubStmt(bool ended_clean)
{
	for (int i = 0; i < cur_session.num_asis_used; i++)
	{
		ResetASIStmt(cur_session.asis+i, ended_clean);
	}
	// cur_session.num_asis_used must stay as is until end(start) of txn,
	// because the commit/abort of current txn will need it. And it will be
	// reset by ResetCommunicationHub();
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
			TopMemoryContext, sizeof (ShardConnSection));

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
		if (req_chk_onfail) RequestShardingTopoCheck(sconn->shard_id);
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

	ShardConnection*sconn = GetConnShard(shardid);
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

	ShardConnection*sconn = GetConnShard(shardid);
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
static ShardConnection*GetConnShard(Oid shardid)
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
 @param req_chk_onfail: if connect to node fails, request master switch check.
 * */
static MYSQL *GetConnShardNode(Oid shardid, Oid nodeid, int *newconn, bool req_chk_onfail)
{
	ShardConnection*sconn = GetConnShard(shardid);
	int slot = AllocShardConnNodeSlot(sconn, nodeid, newconn, req_chk_onfail);
	return sconn->conns[slot];
}

/*
 * Find a ShardConnection object by (shardid, nodeid) from cur_session.
 * If not found, create a new one and cache it.
 * */
static MYSQL *GetConnShardMaster(Oid shardid, int *newconn)
{
	ShardConnection*sconn = GetConnShard(shardid);
	int slot = AllocShardConnNodeSlot(sconn, GetShardMasterNodeId(shardid), newconn, true);
	return sconn->conns[slot];
}
#if 0
/*
 * When poll() tells us a backend connection is closed(POLL_HUP), if we
 * believe other backend connections can still be kept, we should call this
 * function to free the corresponding slots in ShardConnection's arrays.
 *
 * The conn_objs array have to be continuous, and the nodeids array must be
 * continuous and increase only, and the conns array must keep with the
 * nodeids array.
 */
void clear_node_connection(ShardConnection *sconn, Oid nodeid)
{
	Assert(false);
}

/*
 * Close all backend connections of current session to all shards's all nodes.
 * */
void disconnectAll()
{

	Assert(false);
}

/*
 * Close all connections to the specified shard.
 * */
void disconnectShard(Oid shardid)
{

	Assert(false);
}
#endif

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
		/*
		 * If result pending, disconnect to avoid potential async IO issues.
		 * */
		if (asi == asi0 || asi->result_pending)
		{
			if (asi->mysql_res)
			{
				mysql_free_result(asi->mysql_res);
				asi->mysql_res = NULL;
			}

			mysql_close(asi->conn);
			MarkConnValid(asi, false);
		}
		else if (asi->conn)
		{
			if (asi->mysql_res)
			{
				mysql_free_result(asi->mysql_res);
				asi->mysql_res = NULL;
			}
			mysql_close(asi->conn);
			MarkConnValid(asi, false);
		}

		asi->need_abort = false;
		ResetASIStmt(asi, false);
	}

}

#define IS_MYSQL_CLIENT_ERROR(err) (((err) >= CR_MIN_ERROR && (err) <= CR_MAX_ERROR) || ((err) >= CER_MIN_ERROR && (err) <= CER_MAX_ERROR))
static void handle_mysql_error(int ret, AsyncStmtInfo *asi)
{
	Assert(ret != 0);

	MYSQL *conn = asi->conn;
	Oid shardid = asi->shard_id;
	Oid nodeid = asi->node_id;

	/*
	 * The shard and node may not be found from cache, e.g. when they are
	 * removed from system and computing nodes' meta table and cache.
	 *
	 * FindCachedShard(shardid);
	Shard_node_t* pnode = FindCachedShardNode(sconn->shard_id, nodeid);
	*/
	ret = mysql_errno(conn);
	if (ret == 0) return;

	static char errmsg_buf[512];
	errmsg_buf[0] = '\0';
	strncat(errmsg_buf, mysql_error(conn), sizeof(errmsg_buf) - 1);

	/*
	 * Only break the connection for client errors. Errors returned by server
	 * caused by sql stmt simply report to client, those are not caused by
	 * the connection.
	 * */
	if (IS_MYSQL_CLIENT_ERROR(ret))
	{
		/*
		  Mysqld server has no response, maybe because current session is still
		  computing or being blocked, kill each connection in storage shard
		  since we will disconnect next. Simply mysql_close() it isn't enough
		  since the network request won't be picked up in this case.
		*/
		if (ret == CR_SERVER_LOST || ret == CR_SERVER_GONE_ERROR)
		{
			ShardConnKillReq *req = makeShardConnKillReq(1/*kill conn*/);
			if (req)
			{
				appendShardConnKillReq(req);
				pfree(req);
			}
		}

		/*
		 * On client error close the connection to keep clean states.
		 * Close conn before the exception is thrown.
		 * */
		handle_backend_disconnect(asi);
	}
	else if (ret != asi->ignore_error)
	{
		AsyncStmtInfo *pasi = cur_session.asis;
		for (int i = 0; i < cur_session.num_asis_used; i++, pasi++)
		{
			if (pasi->owns_stmt_mem)
			{
				pfree(pasi->stmt); // release the job buffer.
				pasi->owns_stmt_mem = false;
			}
			pasi->stmt = NULL;
			pasi->stmt_len = 0;
			pasi->need_abort = true;
		}
	}

	if (ret == CR_SERVER_LOST || ret == CR_SERVER_GONE_ERROR ||
		ret == CR_SERVER_HANDSHAKE_ERR)
	{
		if (GetShardMasterNodeId(asi->shard_id) == asi->node_id)
		    RequestShardingTopoCheck(asi->shard_id);

		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("Kunlun-db: Connection with MySQL storage node (%u, %u) is gone: %d, %s. Resend the statement.",
						shardid, nodeid, ret, errmsg_buf),
				 errdetail_internal("Disconnected all connections to MySQL storage nodes.")));
	}
	else if (ret == CR_UNKNOWN_ERROR)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: Unknown MySQL client error from MySQL storage node (%u, %u) : %d, %s. Resend the statement.",
						shardid, nodeid, ret, errmsg_buf),
				 errdetail_internal("Disconnected all connections to MySQL storage nodes.")));
	else if (ret == CR_COMMANDS_OUT_OF_SYNC)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: Command out of sync from MySQL storage node (%u, %u) : %d, %s. Resend the statement.",
						shardid, nodeid, ret, errmsg_buf),
				 errdetail_internal("Disconnected all connections to MySQL storage nodes.")));
	else if (IS_MYSQL_CLIENT_ERROR(ret))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: MySQL client error from MySQL storage node (%u, %u) : %d, %s. Resend the statement.",
						shardid, nodeid, ret, errmsg_buf),
				 errdetail_internal("Disconnected all connections to MySQL storage nodes.")));
	else if (ret == ER_OPTION_PREVENTS_STATEMENT &&
		/* It's fragile to rely on error message text for precise error
		 * conditions, but this is all we have from mysql client. If in future
		 * the error message changes, we must respond to that. */
			 (strcasestr(errmsg_buf, "--read-only") ||
			  strcasestr(errmsg_buf, "--super-read-only")))
	{
		/*
		 * A master switch just happened and this computing node doesn't know
		 * it yet. We must have just reconnected (otherwise we would get
		 * CR_SERVER_GONE_ERROR or CR_SERVER_LOST) but we used obsolete info.
		 *
		 * pg_shard/pg_shard_node may have already been updated since we used
		 * the information to reconnect, or they will be updated very soon.
		 * So we will return the ERROR to client so that it can abort the
		 * transaction and retry. And we will disconnect so that next time
		 * connect we may have latest master info(or not, and that would cause
		 * another same loop here, but finally we will have latest info and succeed.)
		 *
		 * Request topology check to update pg_shard/pg_shard_node with latest master info.
		 * */
		bool req_done = RequestShardingTopoCheck(asi->shard_id);
		handle_backend_disconnect(asi);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: MySQL storage node (%u, %u) is no longer a primary(%d, %s), . Retry the transaction.",
						shardid, nodeid, ret, errmsg_buf),
				 errhint("Primary info will be refreshed automatically very soon. "),
				 errdetail_internal("Disconnected all connections to MySQL storage nodes.")));
	}
	else if (ret != asi->ignore_error)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: MySQL storage node (%u, %u) returned error: %d, %s.",
						shardid, nodeid, ret, errmsg_buf)));
	else
		elog(WARNING, "MySQL storage node (%u, %u) returned error: %d, %s, and it's taken as a WARNING.",
			 shardid, nodeid, ret, errmsg_buf);
}


/*
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
	int status;
	MYSQL *ret;

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

	if (mysql_transmit_compress)
		mysql_options(mysql, MYSQL_OPT_COMPRESS, NULL);

	// Never reconnect, because that messes up txnal status.
	my_bool reconnect = 0;
	mysql_options(mysql, MYSQL_OPT_RECONNECT, &reconnect);
	

	/* Returns 0 when done, else flag for what to wait for when need to block. */
	status= mysql_real_connect_start(&ret, mysql, host, user, password, NULL,
									 port, NULL, CLIENT_MULTI_STATEMENTS | (mysql_transmit_compress ? MYSQL_OPT_COMPRESS : 0));
	while (status)
	{
		status= wait_for_mysql(mysql, status);
		status= mysql_real_connect_cont(&ret, mysql, status);
	}

	if (!ret)
	{
		return false;
	}

	elog(LOG, "Connected to mysql instance at %s:%u", host, port);
	return true;
}


static int
wait_for_mysql(MYSQL *mysql, int status)
{
	struct pollfd pfd;
	int timeout;
	int res;

	pfd.fd= mysql_get_socket(mysql);
	pfd.events=
	  (status & MYSQL_WAIT_READ ? POLLIN : 0) |
	  (status & MYSQL_WAIT_WRITE ? POLLOUT : 0) |
	  (status & MYSQL_WAIT_EXCEPT ? POLLPRI : 0);

	/*
	 * This timeout is set to MYSQL handle when the connect/read/write syscall
	 * returned uncompleted, the value is connect_timeout, read_timeout and
	 * write_timeout sent to mysql_options() respectively. It means if the
	 * connect/read/write op has not completed within this timeout period, the
	 * operation should return failure.
	 * */
	if (status & MYSQL_WAIT_TIMEOUT)
		timeout= mysql_get_timeout_value_ms(mysql);
	else
		timeout= -1;
	while (true)
	{
		res= poll(&pfd, 1, timeout);
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
			int status= 0;
			if (pfd.revents & POLLIN)
			  status|= MYSQL_WAIT_READ;
			if (pfd.revents & POLLOUT)
			  status|= MYSQL_WAIT_WRITE;
			if (pfd.revents & POLLPRI)
			  status|= MYSQL_WAIT_EXCEPT;
			return status;
		}
	}
}

  
static int
wait_for_mysql_multi(AsyncStmtInfo *asi, size_t len, struct pollfd *pfds, int timeout_ms)
{
	int res, num_waits = 0;

	for (size_t i = 0; i < len; i++)
	{
		/*
		 * if asi[i].conn is NULL, it means the _cont op failed and the conn was closed.
		 * */
		AsyncStmtInfo *pasi = asi + i;
		struct pollfd *pfd = pfds + i;

		if (pfd->fd == 0)
			pfd->fd = UINT_MAX; // just to make it positive otherwise poll will wait for stdin.
		if (pasi->conn && pasi->result_pending)
			pfd->fd= mysql_get_socket(pasi->conn);
		else
		{
			if (pfd->fd > 0)
				pfd->fd = -pfd->fd;
			continue;
		}

		pfd->events = pfd->revents = 0;
		pfd->events=
		  (pasi->status & MYSQL_WAIT_READ ? POLLIN : 0) |
		  (pasi->status & MYSQL_WAIT_WRITE ? POLLOUT : 0) |
		  (pasi->status & MYSQL_WAIT_EXCEPT ? POLLPRI : 0);

		if (pfd->events == 0 && pfd->fd > 0)
			pfd->fd = -pfd->fd; // io was completed on this fd.
		if (pasi->status & MYSQL_WAIT_TIMEOUT)
		{
			Assert(pfd->events != 0);// there must be some events to wait for.
		}

		if (pasi->status)
		{
			Assert(pfd->fd >= 0 && pfd->events != 0);// there must be some events to wait for.
			num_waits++;
		}
	}

	if (num_waits == 0)
		return 0;

	while (true)
	{
		res= poll(pfds, len, -1);
		if (res == 0)
		{
			for (size_t i = 0; i < len; i++)
			{
				if (asi[i].conn)
					asi[i].status|= MYSQL_WAIT_TIMEOUT;
			}
			break;
		}
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
						(errcode(ERRCODE_SYSTEM_ERROR),
						 errmsg("Kunlun-db: poll() unexpectedly failed with system error (%d : %s)",
						 errno, strerror(errno))));
			}
		}
		else
		{
			for (size_t i = 0; i < len; i++)
			{
				struct pollfd *pfd = pfds + i;
				if (pfd->fd < 0)
					continue; // can't be this one.
				asi[i].status = 0; // Must clear the MYSQL_WAIT_TIMEOUT bit.
				if (pfd->revents & POLLIN)
					asi[i].status|= MYSQL_WAIT_READ;
				if (pfd->revents & POLLOUT)
					asi[i].status|= MYSQL_WAIT_WRITE;
				if (pfd->revents & POLLPRI)
					asi[i].status|= MYSQL_WAIT_EXCEPT;
			}
			break;
		}
	}

	return num_waits;
}


/*
 * Syncly send 'stmt' to one specified shard's master. So far only this
 * function needs to ignore an error.
 * */
void send_remote_stmt(AsyncStmtInfo *asi, char *stmt, size_t len,
	CmdType cmdtype, bool owns_it, enum enum_sql_command sqlcom, int ignore_err)
{
	asi->ignore_error = ignore_err;
	append_async_stmt(asi, stmt, len, cmdtype, owns_it, sqlcom);
	send_stmt_to_multi(asi, 1);
}

/*
 * Send 'stmt' of 'len' bytes to all shards existing in system.
 * */
void
send_stmt_to_all_shards(char *stmt, size_t len, CmdType cmdtype, bool owns_it,
	enum enum_sql_command sqlcom)
{
	List *shardids = GetAllShardIds();
	ListCell *lc = NULL;

	foreach (lc, shardids)
	{
		/*
		 * Txn mgmt cmds are never supposed to be sent to all shards existing
		 * in system.
		 * */
		Assert(cmdtype != CMD_TXN_MGMT);
		AsyncStmtInfo *asi = GetAsyncStmtInfo(lfirst_oid(lc));
		append_async_stmt(asi, stmt, len, cmdtype, owns_it, sqlcom);
	}

	send_stmt_to_multi(cur_session.asis, list_length(shardids));
}

/*
 * Send 'stmt' of 'len' bytes to all shards currently in use in this txn.
 * */
void send_stmt_to_all_inuse(char *stmt, size_t len, CmdType cmdtype, bool owns_it,
	enum enum_sql_command sqlcom, bool written_only)
{
	size_t shard_cnt = cur_session.num_asis_used;
	AsyncStmtInfo *pasi = cur_session.asis;
	for (size_t i = 0; i < shard_cnt; i++)
	{
		if (cmdtype == CMD_TXN_MGMT && sqlcom == SQLCOM_XA_ROLLBACK &&
			(!pasi[i].need_abort || pasi[i].executed_stmts == 0))
			continue;
		if (written_only && !pasi[i].did_write)
			continue;
		append_async_stmt(pasi + i, stmt, len, cmdtype, owns_it, sqlcom);
	}

	send_stmt_to_multi(pasi, shard_cnt);
}

/*
  For now we never send CMD_SELECT mixed with CMD_INSERT/DELETE/UPDATE, but
  we might do this in future, hence this function.
*/
inline static bool 
doing_dml_write(AsyncStmtInfo *asis, size_t shard_cnt)
{
	for (size_t i = 0; i < shard_cnt; i++)
	{
		CmdType cmd = asis[i].cmd;
		if (cmd == CMD_INSERT || cmd == CMD_DELETE || cmd == CMD_UPDATE)
			return true;
	}
	return false;
}


void send_stmt_to_multi_try_wait_all()
{
	AsyncStmtInfo *asis = cur_session.asis;
	size_t shard_cnt = cur_session.num_asis_used;

	send_stmt_to_multi_try_wait(asis, shard_cnt);
}


/*
 * Send all stmts in each asis object's stmt queue. for best performance, never
 * wait for a result to be ready on a connection, when it's not ready, see if
 * another connection's result is ready, and when a ready one is found, send
 * its next stmt.
 *
 * At entry some of the asis slots may have pending results which were sent
 * before the end of an stmt execution(e.g. accumulated insert rows excceeds
 * buffer capacity). and every asis slot must have stmts to send and connection
 * established.
 *
 * shard_cnt: [asis, asis+shard_cnt) ports *may* have stmts to send.
 * */
void send_multi_stmts_to_multi()
{
	AsyncStmtInfo *asis = cur_session.asis;
	size_t shard_cnt = cur_session.num_asis_used;
	send_stmt_to_multi(asis, shard_cnt);
}


/**
 * Asyncly send 'stmt' to the specified list of shards' master nodes, then wait for all replies.
 * caller should free asis array if necessary.
 */
static void send_stmt_to_multi(AsyncStmtInfo *asis, size_t shard_cnt)
{
	PG_TRY();
	{
	while (true)
	{
		bool has_more = false;

		/*
		  if doing insert/delete/update stmts, request global deadlock detection
		  if waited longer than start_global_deadlock_detection_wait_timeout millisecs.
		*/
		if (doing_dml_write(asis, shard_cnt))
			enable_timeout_after(WRITE_SHARD_RESULT_TIMEOUT,
								 start_global_deadlock_detection_wait_timeout);

		send_stmt_to_multi_try_wait(asis, shard_cnt);
		for (size_t i = 0; i < shard_cnt; i++)
		{
			AsyncStmtInfo *pasi = asis + i;
			if (pasi->result_pending)
			{
				has_more = true;
				continue;
			}

			/*
			 * If pasi has no more stmts to work on, or if pasi's SELECT result
			 * have not been fully consumed, we have nothing to do for it.
			 * In the latter case, caller will need to consume and free the
			 * mysql result, then call this func again to move forward.
			 * */
			if (work_on_next_stmt(pasi) == 1)
			{
				send_stmt_to_multi_start(pasi, 1);
				has_more = true;
			}
		}

		if (!has_more)
			break;
		CHECK_FOR_INTERRUPTS();
	}
	disable_timeout(WRITE_SHARD_RESULT_TIMEOUT, false);
	}
	PG_CATCH();
	{
	if (geterrcode() == ERRCODE_QUERY_CANCELED)
	{
		// statement timeout
		request_topo_checks_used_shards();
		/*
		  if statement_timeout set smaller than start_global_deadlock_detection_wait_timeout,
		  this helps to inform gdd in time.
		*/
		kick_start_gdd();

		if (DoingDDL())
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

	/*
	  Current txn will be aborted, no sense to execute the rest stmts. there
	  might be pending results in other some channels, which will be cancelled
	  because of ResetASIStmt() call.
	*/
	CancelAllRemoteStmtsInQueue(true);

	PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * Start async send to multiple shards.
 * return the NO. of conns in asis that are waiting for query results.
 * */
int send_stmt_to_multi_start(AsyncStmtInfo *asis, size_t shard_cnt)
{
	int num_waits = 0, ret = 0;

	for (int j = 0; j < shard_cnt; j++)
	{
		AsyncStmtInfo *pasi = asis + j;

		/*
		 * When there is result pending on a connection, we must first
		 * receive it totally, then send next stmt.
		 * */
		Assert(!pasi->result_pending);

		/*
		 * No need to fetch the connection every time, because if connection
		 * broken during execution of an statement, exception is thrown and
		 * the execution fails. The lifetime of any AsyncStmtInfo is one stmt.
		 * */
		MYSQL *conn = pasi->conn;
		int status = mysql_real_query_start(&ret, conn, pasi->stmt, pasi->stmt_len);

		/*
		 *  if the operation was done in _start, there is no need to wait for
		 *  its results.
		 * */
		if (status)
		{
			num_waits++;
			pasi->status = status;
			pasi->result_pending = true;
			elog(DEBUG1, "sent query to [%u, %u:%ld]: %s", pasi->shard_id,
				 pasi->node_id, mysql_thread_id(pasi->conn), pasi->stmt);
		}
		else
		{
			elog(DEBUG1, "sent query to [%u, %u:%ld] and got result: %s",
				 pasi->shard_id, pasi->node_id, mysql_thread_id(pasi->conn), pasi->stmt);
			pasi->result_pending = false;
			pasi->executed_stmts++;
			if (pasi->owns_stmt_mem)
			{
				pfree(pasi->stmt);
				pasi->owns_stmt_mem = false;
			}
			pasi->stmt = NULL;
			pasi->stmt_len = 0;

			if (ret)
				handle_mysql_error(ret, pasi);
			else
				handle_mysql_result(pasi);
		}
	}

	return num_waits;
}

int GetAsyncStmtInfoUsed()
{
	return cur_session.num_asis_used;
}

AsyncStmtInfo *GetAsyncStmtInfoByIndex(int i)
{
	AsyncStmtInfo *res = (i < cur_session.num_asis_used && i >= 0) ? cur_session.asis+i : NULL;

	// when res->conn is NULL, this assert fails
	//Assert(res->shard_id != InvalidOid && res->node_id != InvalidOid);

	//if (!IsConnValid(res))
	{
		/*
		  In future we may want to reconnect when the conn is invalid, but
		  not now.
		 */
		//if (conn_invalid) // res marked valid inside it.
		  //  GetAsyncStmtInfoNode(res->shard_id, res->node_id, true);
		// else
			//res = NULL;
	}

	return res;
}

void free_mysql_result(AsyncStmtInfo *pasi)
{
	/*
	 * It's likely that multiple executor nodes reads from the same channel,
	 * and in that case the pasi could be freed multiple times, so this assert
	 * holds for none but the 1st free.
	 * Assert(pasi->mysql_res && pasi->cmd == CMD_SELECT);
	 */
	if (!pasi)
		return;

	if (pasi->mysql_res) mysql_free_result(pasi->mysql_res);
	pasi->mysql_res = NULL;
	pasi->cmd = CMD_UNKNOWN;
	Assert(!pasi->result_pending);
}

static bool obtain_mysql_result_rows(AsyncStmtInfo *pasi, int*status)
{
	do {
		Assert(pasi->mysql_res == NULL);
		pasi->mysql_res = mysql_store_result(pasi->conn);
		/*pasi->mysql_res = (pasi->will_rewind ? mysql_store_result(pasi->conn) :
			mysql_use_result(pasi->conn));*/
		pasi->nwarnings += mysql_warning_count(pasi->conn);

		if (!pasi->mysql_res)
		{
			if (mysql_errno(pasi->conn))
				handle_mysql_error(1, pasi);
			else
			{
				/*
				  it's possible that we sent some aux stmts like SET var stmts
				  along with the SELECT/RETURNING stmt and identify as one
				  SELECT/UPDATE/DELETE stmt, so we
				  need to consume results of such SET stmts.
				*/
				Assert(mysql_field_count(pasi->conn) == 0);
				if (mysql_more_results(pasi->conn))
				{
					if ((*status = mysql_next_result(pasi->conn)) > 0)
						handle_mysql_error(-1, pasi);
				}
				else
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("Kunlun-db: A %s statement returned no results.",
							pasi->cmd == CMD_SELECT ? "SELECT" : "RETURNING")));
				}
			}
		}
		else
		{
			/*
			 * The SELECT/RETURNING stmt must be the last stmt in the packet sent.
			 * */
			if (mysql_more_results(pasi->conn))
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Kunlun-db: More than one SELECT statements were sent at once.")));
			pasi->stmt_nrows = 0; // unknown for now, because 'USE' not 'STORE'.
			break;
		}
	
	} while (*status == 0);
	return pasi->mysql_res != NULL;
}

/*
 * Receive SQL stmt result returned from mysql. According to mysql doc, if we
 * send multiple stmts to mysql, we must iterate and receive all results,
 * otherwise the mysql connection will be terminated by client library,
 * and I did see this happen. -- dzw
 * */
static void handle_mysql_result(AsyncStmtInfo *pasi)
{
	int status = 1;

	if (pasi->cmd == CMD_SELECT)
	{
		obtain_mysql_result_rows(pasi, &status);
	}
	else /*(if pasi->cmd == CMD_INSERT || pasi->cmd == CMD_UPDATE ||
			 pasi->cmd == CMD_DELETE || pasi->cmd == CMD_UTILITY)*/
	{
		do {
	        // handle RETURNING result, it must be the last one in a list of
			// stmts sent to mysql at once. so far this constraint is
			// acceptable, otherwise we would need to modify a few
			// send_multi_xxx functions for a new communication framework.
	        if (mysql_field_count(pasi->conn))
			{
				obtain_mysql_result_rows(pasi, &status);
				break;
			}
			else if (mysql_more_results(pasi->conn))
			{
			    if ((status = mysql_next_result(pasi->conn)) > 0)
	                handle_mysql_error(-1, pasi);
			}
			else
				break;
		} while (status == 0);

		uint64_t n = mysql_affected_rows(pasi->conn);
		if (n == (uint64_t)-1)
		   handle_mysql_error(-1, pasi);
		pasi->stmt_nrows += n;
		pasi->txn_wrows += n;

	}

	elog(DEBUG1, "Recvd results from [%u, %u:%ld] : [%u,%u]",
		 pasi->shard_id, pasi->node_id, mysql_thread_id(pasi->conn),
		 pasi->stmt_nrows, pasi->nwarnings);
}

uint64_t GetRemoteAffectedRows()
{
	uint64_t num = 0;

	for (int i = 0; i < cur_session.num_asis_used; i++)
	{
		AsyncStmtInfo *pasi = cur_session.asis + i;
		num += pasi->stmt_nrows;
	}

	return num;
}

MYSQL_RES *GetRemoteRows(AsyncStmtInfo *pasi)
{
	return pasi->mysql_res;
}

/*
 * Wait for all shards to complete.
 * */
static void send_stmt_to_multi_wait(AsyncStmtInfo *asis, size_t shard_cnt)
{
	// Always attempt to wait at least once, if no fd needs to wait, we won't
	// poll() in vain.
	int num_waits = 1;
	struct pollfd fixed[8];

	struct pollfd *pfd = (shard_cnt > 8) ? MemoryContextAllocZero(CurTransactionContext, shard_cnt * sizeof(struct pollfd)) : fixed;

	/*
	 * wait for all to return results.
	 * */
	while (num_waits)
	{
		CHECK_FOR_INTERRUPTS();
		num_waits = wait_for_mysql_multi(asis, shard_cnt, pfd, -1);
		if (num_waits == 0)
			break;
		num_waits = 0;
		for (int i = 0; i < shard_cnt; i++)
		{
			int ret = 0;
			AsyncStmtInfo *pasi = asis + i;
			/*
			 * Don't exclude pasi->status == 0 if pasi->status is 0, still
			 * need one last call of _cont() functions.
			 */
			if (pasi->conn && pasi->result_pending)
			{
				pasi->status= mysql_real_query_cont(&ret, pasi->conn, pasi->status);

	            if (pasi->status == 0)
	            {
	                /* 
	                 * This connection has completed its stmt, no more handling needed.
	                 * Do not set pasi->conn to NULL to denote end of IO, because
	                 * there can be more stmts to send. use asis->result_pending
	                 * instead.
	                 * pasi->conn = NULL;
	                 */
	                pasi->result_pending = false;
					pasi->executed_stmts++;
					if (pasi->owns_stmt_mem)
					{
						pfree(pasi->stmt);
						pasi->owns_stmt_mem = false;
					}
					pasi->stmt = NULL;
					pasi->stmt_len = 0;

	                if (ret)
	                    handle_mysql_error(ret, pasi);
					else
						handle_mysql_result(pasi);
	            }
	            else
	                num_waits++;
			}
		}
	}

	if (pfd != fixed)
		pfree(pfd);
}


/*
 * Peek for ready connections, don't block waiting. for those ready, call _cont
 * to finish the query execution.
 * Return the NO. of conns finished execution by this call.
 * */
int send_stmt_to_multi_try_wait(AsyncStmtInfo *asis, size_t shard_cnt)
{
	// Always attempt to wait at least once, if no fd needs to wait, we won't
	// poll() in vain.
	static struct pollfd fixed[32];

	struct pollfd *pfd = (shard_cnt > 32) ? MemoryContextAllocZero(CurTransactionContext, shard_cnt * sizeof(struct pollfd)) : fixed;
	/*
	 * peek each connection for ready IO, don't wait. and process those
	 * which are IO ready.
	 * */
	int ndone = 0;

	wait_for_mysql_multi(asis, shard_cnt, pfd, -1);

	for (int i = 0; i < shard_cnt; i++)
	{
		int ret = 0;
		AsyncStmtInfo *pasi = asis + i;

		/*
		* Don't exclude pasi->status == 0 if pasi->status is 0, still
		* need one last call of _cont() functions.
		*/
		if (pasi->conn && pasi->result_pending)
		{
			pasi->status= mysql_real_query_cont(&ret, pasi->conn, pasi->status);

	        if (pasi->status == 0)
	        {
	            pasi->result_pending = false;
				pasi->executed_stmts++;
	            // query completed
	            ndone++;
				if (pasi->owns_stmt_mem)
				{
					pfree(pasi->stmt); // release the job buffer.
					pasi->owns_stmt_mem = false;
				}
				pasi->stmt = NULL;
				pasi->stmt_len = 0;

	            if (ret)
	                handle_mysql_error(ret, pasi);
				else
					handle_mysql_result(pasi);
	        }
		}
	}

	if (pfd != fixed)
		pfree(pfd);
	return ndone;
}

static void ResetASICommon(AsyncStmtInfo *asi)
{
	/*
	 * Error thrown by prev stmt could leave unconsumed results, and current txn
	 * is already aborted in computing node.
	 * */
	if (asi->mysql_res)
	{
		free_mysql_result(asi);
		if (asi->conn)
		{
			mysql_close(asi->conn);
			MarkConnValid(asi, false);
		}
		/*
		 * The connection didn't exist or was just reset, either way no need
		 * for an abort.
		 * */
		asi->need_abort = false;
	}

	/*
	 * This could happen if multiple SELECT stmts are sent to remote, and the
	 * during the use of the 1st result an error occurs and other results have
	 * not yet arrived.
	 * */
	if (asi->result_pending && asi->conn)
	{
		mysql_close(asi->conn);
		MarkConnValid(asi, false);
		asi->need_abort = false;
	}
	asi->result_pending = false;
	asi->ignore_error = 0;

	/*
	 * In normal circumstances other than above, asi->conn should be kept valid
	 * in order to avoid repetitive lookup from cur_session. and asi->conn will
	 * be set to NULL when backend connection is found invalid.
	 * */
}

/*
 * Reset asi at end of txn.
 * */
static void ResetASI(AsyncStmtInfo *asi)
{
	/*
	 * We don't release memory here, it's been released when the stmt's result
	 * has been received.
	 * */
	ResetASIStmt(asi, false);

	/*
	 * Reset remaining fields.
	 * stmtq must be kept valid throughout the session.
	 * */
	asi->result_pending = false;
	asi->executed_stmts = 0;
	asi->need_abort = true;
	asi->did_write = asi->did_read = asi->did_ddl = false;
	asi->shard_id = InvalidOid;
	asi->node_id = InvalidOid;
	asi->conn = NULL;
	asi->txn_wrows = 0;
}

/*
 * Reset asi at end(or start) of a stmt.
 * */
static void ResetASIStmt(AsyncStmtInfo *asi, bool ended_clean)
{
	/*
	 * We don't release memory here, it's been released when the stmt's result
	 * has been received.
	 * */
	StmtQueue *q = &(asi->stmtq);
	q->head = q->end = 0;
	if (ended_clean)
		Assert(asi->status == 0 && asi->stmt == NULL &&
			   asi->stmt_len == 0);
	else
	{
		asi->status = 0;
		asi->stmt = NULL;
		asi->stmt_len = 0;
	}

	ResetASICommon(asi);
	// asi->executed_stmts = 0; this must be kept valid until end of txn.
	asi->cmd = CMD_UNKNOWN;
	asi->stmt_nrows = 0;
	asi->nwarnings = 0;
	Assert(asi->mysql_res == NULL);
	asi->mysql_res = NULL;
	asi->owns_stmt_mem = false;
	asi->sqlcom = SQLCOM_END;
}

/*
  To be called only internally after executing a DDL and before executing
  some DMLs in a global txn, so far only used to drop sequences while
  dropping a schema/db.
*/
void ResetASIInternal(AsyncStmtInfo *asi)
{
	/*
	 * We don't release memory here, it's been released when the stmt's result
	 * has been received.
	 * */
	ResetASIStmt(asi, false);

	/*
	 * Reset remaining fields.
	 * stmtq must be kept valid throughout the session.
	 * */
	asi->result_pending = false;
	asi->executed_stmts = 0;
	asi->did_write = asi->did_read = asi->did_ddl = false;
}

/*
  Executor may end a remote scan before its results are consumed and/or
  before all its enqueued stmts are all executed, this function
  does the cleanup work.
*/
void cleanup_asi_work_queue(AsyncStmtInfo *pasi)
{
	if (!pasi) return;

	if (pasi->stmt && pasi->owns_stmt_mem)
		pfree(pasi->stmt); // release the job buffer.

	pasi->owns_stmt_mem = false;
	pasi->stmt = NULL;
	pasi->stmt_len = 0;
	pasi->cmd = CMD_UNKNOWN;
	pasi->sqlcom = SQLCOM_END;

	StmtQueue *q = &(pasi->stmtq);
	for (int i = q->head; i < q->end; i++)
	{
		StmtElem *se = q->queue + i;
		if (se->owns_stmt_mem && se->stmt)
			pfree(se->stmt);

		se->owns_stmt_mem = false;
		se->stmt = NULL;
		se->stmt_len = 0;
		se->cmd = CMD_UNKNOWN;
		se->sqlcom = SQLCOM_END;
	}
	q->head = q->end = 0;
}


/*
 * work on asi->stmtq's next stmt, assign them to asi->stmt/stmt_len.
 * if found next stmt to work on, return 1;
 * if no more stmt to work on, return 0;
 * if current result have not been received or consumed(for SELECT stmt),
 * return -1.
 * */
int work_on_next_stmt(AsyncStmtInfo *asi)
{
	StmtQueue *q = &(asi->stmtq);
	if (q->queue == NULL || q->head >= q->end)
		return 0;
	/*
	 * Current stmt (if any) 's result must have been received when this is
	 * called.
	 *
	 * Also, all recved results must have been consumed
	 * before we can send next stmt.
	 * */
	Assert(asi->result_pending == false);
	if (asi->mysql_res != NULL)
		return -1;

#define FIRST_CMD_IS_DDL (q && q->queue && q->queue[q->head].cmd == CMD_DDL)

/*
 * SET and all the SHOW commands are ADMIN commands, they should not be
 * preceded by XA START. A SET stmt is sent before the 1st stmt of a new
 * connection to storage node is sent, but SHOW commands will be independent
 * stmts so we only consider SET stmt here.
 * */
#define FIRST_CMD_IS_ADMIN (q && q->queue && q->queue[q->head].sqlcom == SQLCOM_SET_OPTION)

	/*
	 * If this shard is accessed for the 1st time, send the txn start stmts
	 * to it first, then send DML stmts.
	 * A DDL stmt is always sent as an autocommit txn without XA cmds wrapped.
	 * Note that if in future we need to send non DML stmts, such as SHOW
	 * commands, set var commands, etc, we won't need to send XA START before
	 * them. It will not be an error to send XA START in such situations though.
	 * */
	if (!asi->did_write && !asi->did_read && !asi->executed_stmts &&
		IsTransactionState() && !FIRST_CMD_IS_DDL && !FIRST_CMD_IS_ADMIN)
	{
		StringInfoData txnstart;
		StmtElem *qse = q->queue + q->head;
		int tslen = 256 + qse->stmt_len;
		if (tslen < 512)
			tslen = 512;

		initStringInfo2(&txnstart, tslen, TopTransactionContext);
		StartTxnRemote(&txnstart);
		appendStringInfoChar(&txnstart, ';');
		appendBinaryStringInfo(&txnstart, qse->stmt, qse->stmt_len);
		if (qse->owns_stmt_mem)
			pfree(qse->stmt);
		qse->stmt_len = lengthStringInfo(&txnstart);
		qse->stmt = donateStringInfo(&txnstart);
		qse->owns_stmt_mem = true;;
	}

	asi->stmt = q->queue[q->head].stmt;
	asi->stmt_len = q->queue[q->head].stmt_len;
	Assert(asi->stmt != NULL && asi->stmt_len > 0);
	asi->owns_stmt_mem = q->queue[q->head].owns_stmt_mem;
	asi->cmd = q->queue[q->head].cmd;
	asi->sqlcom = q->queue[q->head].sqlcom;

	if (!asi->did_write)
		asi->did_write = (asi->cmd == CMD_INSERT || asi->cmd == CMD_UPDATE ||
						  asi->cmd == CMD_DELETE);
	if (!asi->did_ddl)
		asi->did_ddl = (asi->cmd == CMD_DDL);
	if (!asi->did_read)
		asi->did_read = (asi->cmd == CMD_SELECT || asi->cmd == CMD_UTILITY);
	q->head++;
	Assert(q->head <= q->end);
	if (q->head == q->end)
		q->head = q->end = 0;

	return 1;
}

/*
 * Append 'stmt' into asi's job queue. 'stmt' will be sent later when its
 * turn comes, then it will be pfree'd.
 * */
StmtElem *append_async_stmt(AsyncStmtInfo *asi, char *stmt, size_t stmt_len,
	CmdType cmd, bool owns_stmt_mem, enum enum_sql_command sqlcom)
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
		return NULL;
	}

	StmtQueue *q = &(asi->stmtq);
	if (q->queue == NULL)
	{
		q->capacity = 32;
		q->queue = MemoryContextAlloc(TopMemoryContext, q->capacity * sizeof(StmtElem));
	}
	
	Assert(q->head <= q->end);

	if (q->head == q->end) Assert(q->head == 0);

	if (q->end == q->capacity)
	{
		q->queue = repalloc(q->queue, sizeof(StmtElem) * q->capacity * 2);
		memset(q->queue + q->capacity, 0, sizeof(StmtElem)*q->capacity);
		q->capacity *= 2;
	}

	Assert(cmd == CMD_INSERT || cmd == CMD_UPDATE || cmd == CMD_DELETE ||
		   cmd == CMD_SELECT || cmd == CMD_UTILITY || cmd == CMD_DDL || cmd == CMD_TXN_MGMT);

	if (cmd == CMD_DDL && IsExplicitTxn())
	{
		/*
		 * If the asi has not received any results, and there is no stmt
		 * result pending, it's not actually used.
		 * It's crucial to keep the cur_session.num_asi_used right otherwise
		 * we would send XA txn cmds to unused shards, causing an error.
		 * */
		if (asi->result_pending == false && asi->executed_stmts == 0)
		{
			ResetASIStmt(asi, true);
			Assert(cur_session.num_asis_used > 0);
			// nothing done, no txn branch in this channel, can't abort
			asi->need_abort = false;
		}

		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: As demanded by MySQL, a DDL statement can NOT be executed in an explicit transaction.")));
	}

	q->queue[q->end].stmt = stmt;
	q->queue[q->end].stmt_len = stmt_len;
	q->queue[q->end].owns_stmt_mem = owns_stmt_mem;
	q->queue[q->end].cmd = cmd;
	q->queue[q->end].sqlcom = sqlcom;
	/*
	  In DROP TYPE CASCADE, the type may have a dependent table  which is
	  also dropped, but DROP TYPE doesn't go through the remote meta logic so
	  the OP and OBJ types are both generic, mapping no valid sqlcom.
	Assert(sqlcom != SQLCOM_END);
	*/
	return q->queue + q->end++;
}



/*
 * Whether a mysql query has been executed in current transaction.
 * */
bool MySQLQueryExecuted()
{
	AsyncStmtInfo *asi = cur_session.asis;
	for (int i = 0; i < cur_session.num_asis_used; i++, asi++)
	{
		if (asi->executed_stmts || asi->result_pending || asi->did_read ||
			asi->did_write || asi->did_ddl)
			return true;
	}
	return false;
}

/*
 * When a txn is to be aborted, before appending XA ROLLBACK we need to cancel
 * all remote stmts still in queue waiting to be sent. There is no need
 * sending them.
 * */
void CancelAllRemoteStmtsInQueue(bool freestmts)
{
	AsyncStmtInfo *asi = cur_session.asis;
	for (int i = 0; i < cur_session.num_asis_used; i++, asi++)
	{
		for (int j = asi->stmtq.head; j < asi->stmtq.end; j++)
		{
			StmtElem *se = asi->stmtq.queue + j;
			if (se->owns_stmt_mem && freestmts)
				pfree(se->stmt);
			se->stmt = NULL;
			se->stmt_len = 0;
			se->owns_stmt_mem = false;
			se->cmd = CMD_UNKNOWN;
			se->sqlcom = SQLCOM_END;
		}
		asi->stmtq.head = asi->stmtq.end = 0;
		ResetASIStmt(asi, false);
	}
}

static void
make_check_mysql_node_status_stmt(AsyncStmtInfo *asi, bool want_master)
{
	Storage_HA_Mode ha_mode = storage_ha_mode();
	Assert(ha_mode != HA_NO_REP);

	const char *stmt_mgr = "select MEMBER_HOST, MEMBER_PORT from performance_schema.replication_group_members where channel_name='group_replication_applier' and MEMBER_STATE='ONLINE' and MEMBER_ROLE='PRIMARY'";
	const char *stmt_rbr = "select host, port, Channel_name from mysql.slave_master_info"; // TODO: this need extra work

	size_t stmtlen = 0;
	const char *stmt = NULL;
	if (ha_mode == HA_MGR)
		stmt = stmt_mgr;
	else if (ha_mode == HA_RBR)
	{
		stmt = stmt_rbr;
		Assert(false);
	}
	else
	{
		Assert(ha_mode == HA_NO_REP);
		return;
	}

	if (stmtlen == 0 && stmt) stmtlen = strlen(stmt);

	if (stmt) append_async_stmt(asi, stmt, stmtlen, CMD_SELECT, false, SQLCOM_SELECT);
}

static void
check_mysql_node_status(AsyncStmtInfo *asi, bool want_master)
{
	Storage_HA_Mode ha_mode = storage_ha_mode();
	Assert(ha_mode != HA_NO_REP);

	Assert(ha_mode == HA_MGR); // this is true only for now.

	MYSQL_RES *mres = asi->mysql_res;
	Assert(mres);
	bool res = true;

	MYSQL_ROW row = mysql_fetch_row(asi->mysql_res);
	
	if (!row) check_mysql_fetch_row_status(asi);

	if (!row || row[0] == NULL || row[1] == NULL)
		res = false; // not in a mgr cluster, definitely not an MGR master.
	else
	{
		char *endptr = NULL;
		int port = strtol(row[1], &endptr, 10);
		if (strcmp(asi->conn->host, row[0]) || port != asi->conn->port)
			res = false; // current master not what's connected to by asi.
	}

	free_mysql_result(asi);

	if (!res)
	{
		RequestShardingTopoCheck(asi->shard_id);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: Shard (%u) primary node(%u) is not primary node now, retry in a few seconds.",
						asi->shard_id, asi->node_id)));
	}
}


/*
  To be called after mysql_fetch_row() returns NULL, because we are using
  mysql_use_result() rather than mysql_store_result().
 */
void check_mysql_fetch_row_status(AsyncStmtInfo *asi)
{
	int ec = mysql_errno(asi->conn);
	if (ec)
	{
		free_mysql_result(asi);
		RequestShardingTopoCheck(asi->shard_id);
		CancelAllRemoteStmtsInQueue(true);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: Shard (%u) primary node(%u) encountered error (%d, %s) while fetching result rows.",
						asi->shard_id, asi->node_id, ec, mysql_error(asi->conn))));
	}
}

void disconnect_storage_shards()
{
	for (int i = 0; i < cur_session.num_asis_used; i++)
	{
		AsyncStmtInfo *asi = cur_session.asis + i;
	
		if (asi->node_id == 0 || asi->shard_id == 0 || !IsConnValid(asi)) continue;
		if (asi->conn) mysql_close(asi->conn);
		MarkConnValid(asi, false);
		asi->need_abort = false;
		ResetASIStmt(asi, false);
	}
}

void request_topo_checks_used_shards()
{
	for (int i = 0; i < cur_session.num_asis_used; i++)
	{
		AsyncStmtInfo *asi = cur_session.asis + i;
		RequestShardingTopoCheck(asi->shard_id);
	}
}

static bool DoingDDL()
{
	bool ret = false;
	for (int i = 0; i < cur_session.num_asis_used; i++)
	{
		AsyncStmtInfo *asi = cur_session.asis + i;
		if (asi->did_ddl)
		{
			ret = true;
			break;
		}
	}
	return ret;
}

static void CurrentStatementsShards(StringInfo str)
{
	for (int i = 0; i < cur_session.num_asis_used; i++)
	{
		AsyncStmtInfo *asi = cur_session.asis + i;
		if (asi->stmt)
		{
			appendStringInfo(str, "{shard_id: %u, SQL_statement: %*s}",
				asi->shard_id, (int)asi->stmt_len, asi->stmt);
		}
	}
}
