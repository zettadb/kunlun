/*-------------------------------------------------------------------------
 *
 * pg_sharding.c
 *	  sharding meta data caching management code.
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pg_sharding.c
 *
 *
 * INTERFACE ROUTINES
 *
 * NOTES
 *	  This file contains the routines which implement
 *	  the sharding objects caching.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/remote_meta.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/indexing.h"
#include "catalog/pg_shard.h"
#include "catalog/pg_shard_node.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/xidsender.h"
#include "sharding/cluster_meta.h"
#include "sharding/sharding.h"
#include "sharding/sharding_conn.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/lockdefs.h"
#include "storage/lwlock.h"
#include "storage/lwlock.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/timeout.h"
#include "utils/syscache.h"

#include <sys/types.h>
#include <unistd.h>
#include "sharding/mysql/mysqld_error.h"

Shard_id_t Invalid_shard_id = 0;
Shard_id_t First_shard_id = 1;
Shard_node_id_t Invalid_shard_node_id = 0;
Shard_node_id_t First_shard_node_id = 1;

bool ShardExists(const Oid shardid)
{
	bool found;
	HeapTuple tuple = SearchSysCache1(SHARD, ObjectIdGetDatum(shardid));
	if ((found = HeapTupleIsValid(tuple)))
		ReleaseSysCache(tuple);
	
	return found;
}

/*
 * Find cached Shard_node_t objects. If not cached, scan table to cache it and
 * setup its owner's reference to it if the owner is also cached.
 * */
bool FindCachedShardNode(Oid shardid, Oid nodeid, Shard_node_t *out)
{
	bool found = false;
	bool end_txn = false;
	HeapTuple tuple;
	Form_pg_shard_node snode_tuple;
	bool isnull1, isnull2;
	Datum hostaddr, passwd;

	/*StartTransactionCommand will change the MemoryContext */
	MemoryContext memctx = CurrentMemoryContext;
	if (!IsTransactionState())
	{
		StartTransactionCommand();
		end_txn = true;
	}
	tuple = SearchSysCache1(SHARDNODE, ObjectIdGetDatum(nodeid));
	if (HeapScanIsValid(tuple))
	{
		snode_tuple = (Form_pg_shard_node)GETSTRUCT(tuple);
		Assert(snode_tuple->shard_id == shardid);
		found = true;

		if (out)
		{
			out->id = nodeid;
			out->shard_id = snode_tuple->shard_id;
			out->port = snode_tuple->port;
			out->ro_weight = snode_tuple->ro_weight;
			out->svr_node_id = snode_tuple->svr_node_id;
			out->user_name = snode_tuple->user_name;

			hostaddr = SysCacheGetAttr(SHARDNODE, tuple, Anum_pg_shard_node_hostaddr, &isnull1);
			passwd = SysCacheGetAttr(SHARDNODE, tuple, Anum_pg_shard_node_passwd, &isnull2);
			/**/
			if (isnull1 || isnull2)
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("Kunlun-db: Node (%u) lack address or authentication information .", shardid)));
			}
			out->hostaddr = MemoryContextStrdup(memctx, TextDatumGetCString(hostaddr));
			out->passwd = MemoryContextStrdup(memctx, TextDatumGetCString(passwd));
		}

		ReleaseSysCache(tuple);
	}

	if (end_txn)
		CommitTransactionCommand();

	return found;
}


/*
  Return all shards' IDs in a list.
*/
List* GetAllShardIds()
{
	Relation pg_shard_rel;
	HeapTuple tup = NULL;
	SysScanDesc scan;
	List *list = NIL;

	pg_shard_rel = heap_open(ShardRelationId, RowShareLock);
	scan = systable_beginscan(pg_shard_rel, InvalidOid, false, NULL, 0, NULL);
	while ((tup = systable_getnext(scan)) != NULL)
	{
		Form_pg_shard shard = ((Form_pg_shard)GETSTRUCT(tup));
		list = lappend_oid(list, shard->id);
	}

	systable_endscan(scan);
	heap_close(pg_shard_rel, RowShareLock);

	return list;
}

/*
 * Find from cache the shard with minimal 'storage_volume'(which = 1) or
 * 'num_tablets'(which = 2). To be used as the target shard to store a
 * new table.
 * */
Oid FindBestShardForTable(int which, Relation rel)
{
	uint32_t minval = UINT_MAX;
	List *allshards = GetAllShardIds();

	Oid best = InvalidOid;
	if (list_length(allshards) == 0)
	{
		/* do nothing */
	}
	else if (which == 0)
	{
		best = list_nth_oid(allshards, rand() % list_length(allshards));
	}
	else
	{
		ListCell *lc;
		Oid shardid;
		HeapTuple tuple;
		Form_pg_shard shard_tuple;
		foreach (lc, allshards)
		{
			shardid = lfirst_oid(lc);
			tuple = SearchSysCache1(SHARD, ObjectIdGetDatum(shardid));
			if (HeapTupleIsValid(tuple))
			{
				shard_tuple = (Form_pg_shard)GETSTRUCT(tuple);

				if (which == 1 && (best == InvalidOid || shard_tuple->space_volumn < minval))
				{
					minval = shard_tuple->space_volumn;
					best = shardid;
				}
				else if (which == 2 && (best == InvalidOid || shard_tuple->num_tablets < minval))
				{
					minval = shard_tuple->num_tablets;
					best = shardid;
				}
				else if (which == 3 && (best == InvalidOid || shard_tuple->id < minval))
				{
					minval = shard_tuple->id;
					best = shardid;
				}
				ReleaseSysCache(tuple);
			}
		}
	}

	if (best == InvalidOid)
	{
		ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("Kunlun-db: No shard available to store tables.")));
	}

	return best;
}


static Form_pg_shard_node
find_node_tuple(CatCList *nodes, const char *ip, uint16_t port)
{
	Form_pg_shard_node node;

	for (int i = 0; i < nodes->n_members; ++i)
	{
		HeapTuple tuple = &nodes->members[i]->tuple;
		/* match port */
		node = (Form_pg_shard_node)GETSTRUCT(tuple);
		if (node->port != port)
			continue;

		/* match ip address */
		bool isnull;
		Datum addr = SysCacheGetAttr(SHARDNODES,
					     tuple,
					     Anum_pg_shard_node_hostaddr,
					     &isnull);
		char *addr_str = TextDatumGetCString(addr);
		bool match = (strcmp(addr_str, ip) == 0);
		pfree(addr_str);
		if (match)
			return node;
	}

	return NULL;
}


static StmtSafeHandle
send_stmt_no_exception(Oid shardid, Oid nodeid, const char *stmt, size_t len)
{
	AsyncStmtInfo *asi;
	StmtSafeHandle handle = INVALID_STMT_HANLE;

	PG_TRY();
	{
		asi = GetAsyncStmtInfoNode(shardid, nodeid, false);
		handle = send_stmt_async(asi, (char *)stmt, len, CMD_SELECT, false, SQLCOM_SELECT, false);
	}
	PG_CATCH();
	{
		if (stmt_handle_valid(handle))
		{
			release_stmt_handle(handle);
			handle = INVALID_STMT_HANLE;
		}
		/*
		  can't connect to pnode, don't throw error here otherwise we
		  never can find current master if one node is down. So we downgrade
		  the error to warning and output it to server log.
		  We can do this because GetAsyncStmtInfoNode() doesn't touch
		  shared memory otherwise we could damage the shared memory and all
		  other backends could crash.
		*/
		HOLD_INTERRUPTS();
		downgrade_error();
		errfinish(0);
		FlushErrorState();
		RESUME_INTERRUPTS();
	}
	PG_END_TRY();

	return handle;
}


static MYSQL_ROW
get_result_no_exception(StmtSafeHandle handle)
{
	MYSQL_ROW row = NULL;
	PG_TRY();
	{
		row = get_stmt_next_row(handle);
	}
	PG_CATCH();
	{
		cancel_stmt_async(handle);
	}
	PG_END_TRY();

	return row;
}

/*
  Connect to each node of the shard and find which one is the latest master
  of shard 'shardid', and return the master node id via pmaster_nodeid.

  @retval >=0: Number of masters found.
  		  if more than one masters found, the 1st one
 		  is returned via pmaster_nodeid and the rest are ignored, and warning
		  msg is logged in this case, and caller need to retry later.
		  -1: found a primary node not stored in pg_shard_node, caller need retry later.
		  -2: some of the existing established connections broken when sending
		  stmts to it, or other errors. this number is n't for not returned by
		  this function, it's assumed if error thrown out of this function.
		  -3: shard not found
*/
static int FindCurrentMasterNodeId(Oid shardid, Oid *pmaster_nodeid)
{
	Assert(shardid != InvalidOid && pmaster_nodeid);
	const char *fetch_gr_members_sql = NULL;
	int ret = 0;

	Storage_HA_Mode ha_mode = storage_ha_mode();
	if (ha_mode == HA_MGR)
		fetch_gr_members_sql = "select MEMBER_HOST, MEMBER_PORT from performance_schema.replication_group_members where channel_name='group_replication_applier' and MEMBER_STATE='ONLINE' and MEMBER_ROLE='PRIMARY'";
	else if (ha_mode == HA_RBR)
		fetch_gr_members_sql = "select HOST, PORT from performance_schema.replication_connection_configuration where channel_name='kunlun_repl';";
	else
		Assert(ha_mode == HA_NO_REP);

	size_t sqllen = 0;
	if (fetch_gr_members_sql)
		sqllen = strlen(fetch_gr_members_sql);

	*pmaster_nodeid = 0;

	HeapTuple tuple = NULL;
	CatCList *nodes = NULL;
	Form_pg_shard shard_tuple;
	char *shard_name = NULL;
	bool free_txn = false;

	if (!IsTransactionState())
	{
		free_txn = true;
		StartTransactionCommand();
	}

	/* shard info */
	tuple = SearchSysCache1(SHARD, ObjectIdGetDatum(shardid));
	if (!HeapTupleIsValid(tuple))
	{
		elog(WARNING, "Shard %u was not found while looking for its current master node.", shardid);
		ret = -3;
		goto end;
	}
	shard_tuple = (Form_pg_shard)GETSTRUCT(tuple);
	shard_name = shard_tuple->name.data;
	/* info of node shards  */
	nodes = SearchSysCacheList1(SHARDNODES, ObjectIdGetDatum(shardid));
	if (!nodes || nodes->n_members == 0)
	{
		elog(WARNING, "Nodes of shard %u was not found while looking for its current master node.", shardid);
		ret = -3;
		goto end;
	}

	int num_conn_fails = 0;
	if (ha_mode == HA_NO_REP)
	{
		*pmaster_nodeid = shard_tuple->master_node_id;
		ret = 1;
		goto end;
	}

	Oid master_nodeid = InvalidOid;
	const char *master_ip = NULL;
	uint16_t master_port = 0;
	int num_masters = 0;	 // NO. of unique master nodes found from storage shard nodes.
	int num_quorum = 0;	 // NO. of shard nodes which affirm master_node_id to be new master.
	int num_unknowns = 0;	 // NO. of shard nodes who doesn't know about current master.
	int num_new_masters = 0; // NO. of found masters not in pg_shard_node

	/* Query the node one by one to collect information about master */
	for (int i = 0; i < nodes->n_members; i++)
	{
		Form_pg_shard_node node_tuple =
		    (Form_pg_shard_node)GETSTRUCT(&nodes->members[i]->tuple);
		Assert(node_tuple->shard_id == shardid);

		/*
		  can't connect to pnode, don't throw error here otherwise we
		  never can find current master if one node is down. So we downgrade
		  the error to warning and output it to server log.
		  We can do this because GetAsyncStmtInfoNode() doesn't touch
		  shared memory otherwise we could damage the shared memory and all
		  other backends could crash.
		*/
		StmtSafeHandle handle = send_stmt_no_exception(node_tuple->shard_id,
							       node_tuple->id, fetch_gr_members_sql, sqllen);
		if (!stmt_handle_valid(handle))
		{
			num_conn_fails++;
			continue;
		}

		MYSQL_ROW row = get_result_no_exception(handle);
		if (row)
		{
			const char *ip = row[0];
			uint16_t port = strtol(row[1], NULL, 10);

			Form_pg_shard_node node = find_node_tuple(nodes, ip, port);

			if (node == NULL)
			{
				elog(WARNING, "Found a new primary node(%s, %u) of shard %s(%u) not in pg_shard_node, "
					      "meta data in pg_shard_node isn't up to date, retry later.",
				     ip, port, shard_name, shardid);
				num_new_masters++;
				continue;
			}

			if (master_nodeid == InvalidOid)
			{
				master_nodeid = node->id;
				master_ip = pstrdup(ip);
				master_port = port;
				Assert(num_masters == 0);
				num_masters++;
				num_quorum++;
			}
			else if (master_nodeid != node->id)
			{
				elog(WARNING, "Found a new primary node(%s, %u, %u) of shard %s(%u) when we already found a new primary node (%s, %u, %u),"
					      " might be a brain split bug of MGR, but more likely a primary switch is happening right now, retry later.",
				     ip, port, node->id, shard_name, shardid, master_ip, master_port, master_nodeid);
				num_masters++;
			}
			else
			{
				num_quorum++;
			}
		}
		else
		{
			AsyncStmtInfo *asi = RAW_HANDLE(handle)->asi;
			elog(WARNING, "Primary node unknown in shard %s(%u) node (%s, %u, %u).",
			     shard_name, node_tuple->shard_id, asi->conn->host, asi->conn->port, asi->node_id);
			num_unknowns++;
		}

		release_stmt_handle(handle);
	}

	/*
	      We might find different master node from results of all nodes, that means
	      one of the following:
	      1. there is a brain-split, but MGR should be able to avoid this;
	      2. a master switch is going on and we get old view from some of the nodes
	      and new view from others.
	      So if we see this fact, we log WARNING messages and retry later in our caller.

	      We might also find no master available from any shard node, and we report
	      WARNING and exit the function.
	    */
	elog(LOG, "Looking for primary node of shard %s(%u) among %d nodes, with %d unavailable nodes.",
	     shard_name, shardid, nodes->n_members, num_conn_fails);

	if (num_new_masters > 0)
	{
		elog(WARNING, "Found %d new primary nodes in shard %s(%u) which are not registered in pg_shard_node and can't be used by current computing node. Primary node is unknown in %d nodes, with %d unavailable nodes. Retry later.",
		     num_new_masters, shard_name, shardid, num_unknowns, num_conn_fails);
		ret = -1;
		goto end;
	}

	if (num_masters == 0)
		elog(WARNING, "Primary node not found in shard %s(%u), it's unknown in %d nodes, with %d unavailable nodes. Retry later.",
		     shard_name, shardid, num_unknowns, num_conn_fails);
	if (num_masters > 1)
		elog(WARNING, "Multiple(%d) primary nodes found in shard %s(%u). It's unknown in %d nodes, with %d unavailable nodes. Retry later.",
		     num_masters, shard_name, shardid, num_unknowns, num_conn_fails);
	if (num_masters == 1)
		elog(LOG, "Found new primary node (%s, %u, %u) in shard %s(%u), affirmed by %d nodes of the shard. The primary is unknown in %d nodes, with %d unavailable nodes.",
		     master_ip, master_port, master_nodeid, shard_name, shardid, num_quorum, num_unknowns, num_conn_fails);

	*pmaster_nodeid = master_nodeid;
	ret = num_masters;

end:
	ReleaseSysCache(tuple);
	ReleaseSysCacheList(nodes);

	if (free_txn)
		CommitTransactionCommand();

	return ret;
}
/*
  Ask shard 'shardid' nodes which node is master, and if a quorum of them
  affirm the same new master node, it's updated in pg_shard, i.e.
  set the new master node id to pg_shard.master_node_id.
  @retval true on error, false on sucess.
*/
static bool UpdateCurrentMasterNodeId(Oid shardid)
{
	bool ret = true;
	Oid master_nodeid = InvalidOid;
	char strmsg[64];

	SetCurrentStatementStartTimestamp();
	bool end_txn = false;

	if (!IsTransactionState())
	{
		StartTransactionCommand();
		end_txn = true;
	}

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	snprintf(strmsg, sizeof(strmsg), "Finding and updating new primary node for shard %u.", shardid);
	pgstat_report_activity(STATE_RUNNING, strmsg);
	int num_masters = 0;
	PG_TRY(); {
		num_masters = FindCurrentMasterNodeId(shardid, &master_nodeid);
	} PG_CATCH(); {
		num_masters = -2; // existing connection broken, or other errors.
		HOLD_INTERRUPTS();

		downgrade_error();
		errfinish(0);
		FlushErrorState();
		RESUME_INTERRUPTS();
	} PG_END_TRY();

	if (num_masters != 1)
		goto end1;
	
	Relation pg_shard_rel = heap_open(ShardRelationId, RowExclusiveLock);

	uint64_t ntups = 0;
	HeapTuple tup = NULL, tup0 = 0;
	SysScanDesc scan;

	ScanKeyData key;
	ScanKeyInit(&key,
				Anum_pg_shard_id,
				BTEqualStrategyNumber,
				F_OIDEQ, shardid);
	scan = systable_beginscan(pg_shard_rel, ShardOidIndexId, true, NULL, 1, &key);
	Oid old_master_nodeid = InvalidOid;
	while ((tup = systable_getnext(scan)) != NULL)
	{
		ntups++;
		Form_pg_shard pss = ((Form_pg_shard) GETSTRUCT(tup));
		old_master_nodeid = pss->master_node_id;
		tup0 = tup;
		break;
	}

	if (ntups == 0)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: Can not find row in pg_shard for shard %u", shardid)));
		goto end;
	}

	if (ntups > 1)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: Found %lu valid rows for shard %u, but only 1 row is expected.", ntups, shardid)));
		goto end;
	}

	if (old_master_nodeid == master_nodeid)
	{
		ret = false;
		elog(LOG, "New primary node found the same as current primary node(%u) in pg_shard for shard %u.",
			 master_nodeid, shardid);
		goto end;
	}

	Datum values[8] = {0, 0, 0, 0, 0, 0, 0, 0};
	bool nulls[8] = {false, false, false, false, false, false, false, false};
	bool replaces[8] = {false, false, false, false, false, false, false, false};
	replaces[Anum_pg_shard_master_node_id - 1] = true;
	values[Anum_pg_shard_master_node_id - 1] = UInt32GetDatum(master_nodeid);

	HeapTuple newtuple =
		heap_modify_tuple(tup0, RelationGetDescr(pg_shard_rel),
	                      values, nulls, replaces);
	CatalogTupleUpdate(pg_shard_rel, &newtuple->t_self, newtuple);
	ret = false;
end:
	systable_endscan(scan);
	heap_close(pg_shard_rel, NoLock);
end1:
	SPI_finish();
	PopActiveSnapshot();
	if (end_txn)
		CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
	if (num_masters == 1 && old_master_nodeid != master_nodeid)
		elog(LOG, "Updated primary node id of shard %u from %u to %u", shardid, old_master_nodeid, master_nodeid);
	return ret;
}

typedef struct ShardingTopoCheckReq {
	int endidx;
	Oid shardids[MAX_SHARDS];
} ShardingTopoCheckReq;

static ShardingTopoCheckReq *ShardingTopoChkReqs = NULL;

Size ShardingTopoCheckSize()
{
	return sizeof(ShardingTopoCheckReq);
}


void ShardingTopoCheckShmemInit(void)
{
	Size		size;
	bool		found;

	/* Create or attach to the shared array */
	size = ShardingTopoCheckSize();
	ShardingTopoChkReqs = (ShardingTopoCheckReq*)
		ShmemInitStruct("Requests to check storage shard latest primary node.", size, &found);

	if (!found)
	{
		/*
		 * We're the first - initialize.
		 */
		MemSet(ShardingTopoChkReqs, 0, size);
	}
}


/*
  Request to find latest master node of shard 'shardid' and update its pg_shard
  row's master_node_id field.
  @retval true if request placed; false if request not placed.
*/
bool RequestShardingTopoCheck(Oid shardid)
{
	Storage_HA_Mode ha_mode;
	bool done = false;

	if (shardid == METADATA_SHARDID)
		ha_mode = metaserver_ha_mode();
	else
		ha_mode = storage_ha_mode();

	if (ha_mode != HA_NO_REP)
	{
		LWLockAcquire(ShardingTopoCheckLock, LW_EXCLUSIVE);
		if (ShardingTopoChkReqs->endidx < MAX_SHARDS)
		{
			for (int i = 0; !done && i < ShardingTopoChkReqs->endidx; i++)
			{
				done = (shardid == ShardingTopoChkReqs->shardids[i]);
			}

			if (!done)
				ShardingTopoChkReqs->shardids[ShardingTopoChkReqs->endidx++] = shardid;
			done = true;
		}
		LWLockRelease(ShardingTopoCheckLock);

		int pid = get_topo_service_pid();
		if (done && pid != 0 && pid != MyProcPid)
			kill(pid, SIGUSR2);
	}

	return done;
}

/*
  Request to find latest master node of shard 'shardid' and update its pg_shard
  row's master_node_id field.
  @retval true if request placed; false if request not placed.
*/
static int RequestShardingTopoChecks(Oid *poids, int n)
{
	int ndones = 0;
	Storage_HA_Mode ha_mode = storage_ha_mode();

	if (ha_mode == HA_NO_REP) return 0;
	LWLockAcquire(ShardingTopoCheckLock, LW_EXCLUSIVE);
	for (int j = 0; j < n; j++)
	{
		const Oid shardid = poids[j];
		for (int i = 0; i < ShardingTopoChkReqs->endidx; i++)
			if (shardid == ShardingTopoChkReqs->shardids[i])
				goto done;
		// not found, append it
		if (ShardingTopoChkReqs->endidx >= MAX_SHARDS)
			goto end;
		ShardingTopoChkReqs->shardids[ShardingTopoChkReqs->endidx++] = shardid;
done:
		ndones++;
	}
end:
	LWLockRelease(ShardingTopoCheckLock);
	/*
	this function is for now only used to put failed requests back to queue,
	and in this case we should not retry immediately otherwise we'd probably
	fail again, forming a endless loop.
	if (ndones)
		kill(g_remote_meta_sync->main_applier_pid, SIGUSR2);
	*/
	return ndones;
}

int check_primary_interval_secs = 3;

void ProcessShardingTopoReqs()
{
	static Oid reqs[MAX_SHARDS], fail_reqs[MAX_SHARDS];
	int nreqs = 0, nfail_reqs = 0;

	LWLockAcquire(ShardingTopoCheckLock, LW_EXCLUSIVE);
	if (ShardingTopoChkReqs->endidx > 0)
	{
		memcpy(reqs, ShardingTopoChkReqs->shardids, sizeof(Oid)*ShardingTopoChkReqs->endidx);
		nreqs = ShardingTopoChkReqs->endidx;
		ShardingTopoChkReqs->endidx = 0;
	}
	LWLockRelease(ShardingTopoCheckLock);

	static time_t last_master_update_ts = 0;
	bool selfinit = false;
	Storage_HA_Mode ha_mode = storage_ha_mode();
	if (nreqs == 0 && ha_mode != HA_NO_REP &&
		last_master_update_ts + check_primary_interval_secs < time(NULL))
	{
                List *shardlst = GetAllShardIds();
		ListCell *lc;

                foreach (lc, shardlst)
                {
			reqs[nreqs++] = lfirst_oid(lc);
		}
                list_free(shardlst);
		reqs[nreqs++] = METADATA_SHARDID;
		selfinit = true;
	}

	if (nreqs > 0)
		elog(LOG, "Start processing %d %s sharding topology checks.",
			 nreqs, selfinit ? "actively initiated" : "");

	for (int i = 0; i < nreqs; i++)
	{
		bool ret = false;
		if (reqs[i] == METADATA_SHARDID)
			ret = UpdateCurrentMetaShardMasterNodeId();
		else
			ret = UpdateCurrentMasterNodeId(reqs[i]);
		if (ret)
			fail_reqs[nfail_reqs++] = reqs[i];
	}

	if (nfail_reqs > 0)
	{
		int nadded = RequestShardingTopoChecks(fail_reqs, nfail_reqs);
		Assert(nadded == nfail_reqs);
		elog(LOG, "Re-added %d of %d failed topology checks to request queue.", nadded, nfail_reqs);
	}

	if (nreqs > 0)
	{
		last_master_update_ts = time(NULL);
		elog(LOG, "Completed processing %d sharding topology checks, failed %d checks and will retry later.",
			 nreqs - nfail_reqs, nfail_reqs);
	}
}

//====================================================


// this is too big, can't alloc so much
#define MAX_SHARD_CONN_KILL_REQ_SIZE ((MAXALIGN(sizeof(ShardConnKillReq)) + MAXALIGN(sizeof(ShardNodeConnId))*MAX_SHARDS*MAX_NODES_PER_SHARD - 1) * 10000)

typedef struct ShardConnKillReqQ {
	uint32_t nreqs;
	Size total_sz; // total space in bytes to write requests
	Size write_pos;// offset starting after the header where next request should be written.

} ShardConnKillReqQ;

static ShardConnKillReqQ *shard_conn_kill_reqs = NULL;
Size ShardConnKillReqQSize()
{
	return 1024*1024;
}


void ShardConnKillReqQShmemInit(void)
{
	Size		size;
	bool		found;

	/* Create or attach to the shared array */
	size = ShardConnKillReqQSize();
	shard_conn_kill_reqs = (ShardConnKillReqQ*)
		ShmemInitStruct("Requests to kill conns&queries of shard/meta clusters.", size, &found);

	if (!found)
	{
		/*
		 * We're the first - initialize the header.
		 */
		MemSet(shard_conn_kill_reqs, 0, sizeof(ShardConnKillReqQ));
		shard_conn_kill_reqs->total_sz = size - sizeof(ShardConnKillReqQ);
		shard_conn_kill_reqs->write_pos = sizeof(ShardConnKillReqQ);
	}
}

Size ShardConnKillReqSize(int nodes)
{
	return (MAXALIGN(sizeof(ShardConnKillReq)) + MAXALIGN(sizeof(ShardNodeConnId))*(nodes - 1));
}


/*
  type: 1: connection; 2: query
  connid: the connid to kill
*/
ShardConnKillReq *makeMetaConnKillReq(char type, uint32_t connid)
{
	Assert(type == 1 || type == 2);
	if (connid == 0) return NULL;

	Size sz = ShardConnKillReqSize(1);
	ShardConnKillReq *req = (ShardConnKillReq *)palloc0(sz);

	req->type = type;
	req->num_ents = 1;
	req->next_req_off = sz;
	req->flags = METASHARD_REQ;

	req->entries[0].shardid = req->entries[0].nodeid = 0;
	req->entries[0].connid = connid;
	elog(DEBUG1, "requesting metadata cluster kill %s %u", type == 1 ? "connection" : "query", connid);
	return req;
}

/*
  Kill all mysql conns or current query running in such conns of current
  backend connecting to mysql nodes.
*/
ShardConnKillReq *makeShardConnKillReq(char type)
{
	Assert(type == 1 || type == 2);

	int nasis = GetAsyncStmtInfoUsed(), idx = 0;
	Size sz = ShardConnKillReqSize(nasis);
	ShardConnKillReq *req = (ShardConnKillReq *)palloc0(sz);

	req->type = type;
	req->flags = STORAGE_SHARD_REQ;

	for (int i = 0; i < nasis; i++)
	{
		AsyncStmtInfo *asis = GetAsyncStmtInfoByIndex(i);
		if (!asis->conn)
		{
			continue;
		}

		Assert(asis->shard_id != Invalid_shard_id && asis->node_id != Invalid_shard_node_id);
		ShardNodeConnId *pconn = req->entries + idx++;
		pconn->shardid = asis->shard_id;
		pconn->nodeid = asis->node_id;
		pconn->connid = mysql_thread_id(asis->conn);
		Assert(pconn->connid != 0);
	}

	if (idx == 0) // no entry added
	{
		pfree(req);
		return NULL;
	}

	req->num_ents = idx;
	req->next_req_off = ShardConnKillReqSize(idx);

	elog(DEBUG1, "requesting shards kill %s ", type == 1 ? "connection" : "query");
	return req;
}

void appendShardConnKillReq(ShardConnKillReq*req)
{
	// if not enough space to write the request, wait.
	int cntr = 0;
	/*
	   This function could be called by reapShardConnKillReqs()->GetAsyncStmtInfoNode()
	   when connection to storage shards broken. In that case we should not lock
	   the lwlock again otherwise the process will self-lock.
	   */
	const bool locked = LWLockHeldByMe(KillShardConnReqLock);
	const bool curproc_is_topo_service = get_topo_service_pid() == MyProcPid;
	int topo_service_pid = get_topo_service_pid();

	if (!locked) LWLockAcquire(KillShardConnReqLock, LW_EXCLUSIVE);
	while (shard_conn_kill_reqs->write_pos + req->next_req_off >
			shard_conn_kill_reqs->total_sz + sizeof(ShardConnKillReqQ))
	{
		if (!locked) LWLockRelease(KillShardConnReqLock);
		/*
		   In both cases should not waste time enqueuing a low priority request.
		   When locked, can't release the lock because we don't know here
		   whether that's the right thing to do.
		   */
		if (locked || curproc_is_topo_service) return;

		/*
		   no big deal to drop such a req, don't wait here infinitely.
		   statement timeout mechanism can't work here since enable_timeout()
		   not called explicitly and not in a txn.
		   */
		if (cntr++ > 7)
			return;
		elog(WARNING, "shard conn kill req queue is full, waiting to enq a req.");

		if (!curproc_is_topo_service && topo_service_pid != 0)
			kill(topo_service_pid, SIGUSR2);

		usleep(10000);
		if (!locked) LWLockAcquire(KillShardConnReqLock, LW_EXCLUSIVE);
	}

	// here we have the KillShardConnReqLock lock
	const uint32_t reqlen = req->next_req_off;
	req->next_req_off += shard_conn_kill_reqs->write_pos;
	memcpy((char*)shard_conn_kill_reqs + shard_conn_kill_reqs->write_pos,
			req, reqlen);
	shard_conn_kill_reqs->write_pos += reqlen;
	shard_conn_kill_reqs->nreqs++;
	if (!locked) LWLockRelease(KillShardConnReqLock);

	if (!curproc_is_topo_service && topo_service_pid != 0)
		kill(topo_service_pid, SIGUSR2);
}

typedef struct MetaShardKillConnReq {
	int connid;
	char type;
} MetaShardKillConnReq;
typedef struct MetaShardKillConnReqSection {
	int end;
	MetaShardKillConnReq arr[32];
	struct MetaShardKillConnReqSection *next;
} MetaShardKillConnReqSection;

static void freeMetaShardKillConnReqSection(MetaShardKillConnReqSection*sec);
static void appendMetaConnKillReq(MetaShardKillConnReqSection *sec, uint32_t connid, char type);

static void appendMetaConnKillReq(MetaShardKillConnReqSection *sec, uint32_t connid, char type)
{
	while (sec->next) sec = sec->next;

	if (sec->end == 32)
	{
		sec->next = (MetaShardKillConnReqSection*)
			MemoryContextAllocZero(TopMemoryContext,
				sizeof(MetaShardKillConnReqSection));
		sec = sec->next;
	}

	MetaShardKillConnReq *req = sec->arr + sec->end;
	req->connid = connid;
	req->type = type;
	sec->end++;
}

static void freeMetaShardKillConnReqSection(MetaShardKillConnReqSection*sec)
{
	sec = sec->next; // 1st one needn't be freed.
	while (sec)
	{
		MetaShardKillConnReqSection *pnext = sec->next;
		pfree(sec);
		sec = pnext;
	}
}

void reapShardConnKillReqs()
{
	Assert(IsTransactionState());
	uint32_t metashard_connid = 0;
	uint32_t pos = sizeof(ShardConnKillReqQ);
	int num_shard_reqs = 0, num_meta_reqs = 0;
	Oid cur_shardid = 0;
	MetaShardKillConnReqSection metareqs;
	memset(&metareqs, 0, sizeof(metareqs));

	set_stmt_ignored_error(ER_NO_SUCH_THREAD);
	LWLockAcquire(KillShardConnReqLock, LW_EXCLUSIVE);
	PG_TRY();
	{
		for (uint32_t i = 0; i < shard_conn_kill_reqs->nreqs; i++)
		{
			ShardConnKillReq *req = (ShardConnKillReq *)((char *)shard_conn_kill_reqs + pos);
			Assert(req->type == 1 || req->type == 2);
			ShardNodeConnId *sncs = req->entries;

			if (req->num_ents == 0)
				continue;

			if (req->flags & METASHARD_REQ)
			{
				metashard_connid = sncs[0].connid;
				Assert(metashard_connid != 0);
				Assert(req->num_ents == 1);
				appendMetaConnKillReq(&metareqs, metashard_connid, req->type);
				num_meta_reqs++;
				continue;
			}

			for (int j = 0; j < req->num_ents; j++)
			{
				Assert(sncs[j].shardid != Invalid_shard_id &&
				       sncs[j].nodeid != Invalid_shard_node_id && sncs[j].connid != 0);
				cur_shardid = sncs[j].shardid;
				AsyncStmtInfo *asi = GetAsyncStmtInfoNode(cur_shardid, sncs[j].nodeid, true);
				/*
				  There is no txnal cxt here, so allocing from top-memcxt and
				  should tell async-comm module to free it.
				*/
				char *stmt = (char *)palloc(64);
				int slen = snprintf(stmt, 64, "kill %s %u",
						    (req->type == 1 ? "connection" : "query"), sncs[j].connid);
				Assert(slen < 64);
				send_stmt_async_nowarn(asi, stmt, slen, CMD_UTILITY, true, SQLCOM_KILL);
			}

			num_shard_reqs += req->num_ents;
			pos = req->next_req_off;
		}

		shard_conn_kill_reqs->write_pos = sizeof(ShardConnKillReqQ);
		shard_conn_kill_reqs->nreqs = 0;

		LWLockRelease(KillShardConnReqLock);
	}
	PG_CATCH(); // the GetAsyncStmtInfoNode() could fail of mysql connect error.
	{
		set_stmt_ignored_error(0);
		LWLockReleaseAll();
		/*
		  For mysql connect error, don't rethrow, but request a master check and
		  proceed with the rest of this function, so that we will still send kill
		  stmts to those reachable master nodes, and metashard node.
		  The kill-conn requests are intact, they will be handled in next round,
		  and this means some requests may be processed multiple times but that's
		  no harm.
		*/
		if (geterrcode() == ERRCODE_CONNECTION_FAILURE)
		{
			RequestShardingTopoCheck(cur_shardid);
			HOLD_INTERRUPTS();

			downgrade_error();
			errfinish(0);
			FlushErrorState();
			RESUME_INTERRUPTS();
		}
		else
			PG_RE_THROW();
	}
	PG_END_TRY();
	set_stmt_ignored_error(0);

	if (num_shard_reqs == 0)
		goto do_meta;

	flush_all_stmts();

do_meta:
	if (num_shard_reqs > 0 || num_meta_reqs > 0)
		elog(INFO, "Reaped %d shard kill reqs and %d meta kill reqs.",
		     num_shard_reqs, num_meta_reqs);
	if (num_meta_reqs == 0)
		return;
	// kill metashard_connid on all metashard nodes, because a primary switch
	// may have just finished and current master known to this computing node
	// isn't the one containing the target conn to kill.
	for (MetaShardKillConnReqSection *sec = &metareqs; sec; sec = sec->next)
	{
		for (int i = 0; i < sec->end; i++)
		{
			KillMetaShardConn(sec->arr[i].type, sec->arr[i].connid);
		}
	}
	freeMetaShardKillConnReqSection(&metareqs);
}

Oid GetShardMasterNodeId(Oid shardid)
{
	Oid nodeid = InvalidOid;
	HeapTuple tuple;
	bool free_txn = false;
	/* Make sure in transaction */
	if (!IsTransactionState())
	{
		free_txn = true;
		StartTransactionCommand();
	}

	tuple = SearchSysCache1(SHARD, ObjectIdGetDatum(shardid));
	if (HeapTupleIsValid(tuple))
	{
		nodeid = ((Form_pg_shard)GETSTRUCT(tuple))->master_node_id;
		ReleaseSysCache(tuple);

		/* Check if node exists */
		if (nodeid != InvalidOid)
		{
			tuple = SearchSysCache1(SHARDNODE, ObjectIdGetDatum(nodeid));
			if (HeapTupleIsValid(tuple))
			{
				ReleaseSysCache(tuple);
			}
			else
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("Kunlun-db: Master node (%u) of Shard (%u) not found.", nodeid, shardid)));
			}
		}
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("Kunlun-db: Shard (%u) not found.", shardid)));
	}

	if (free_txn)
		CommitTransactionCommand();

	return nodeid;
}


static bool got_sigterm = false;
static bool got_sighup = true;
static void
topo_service_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

static void
topo_service_sighup(SIGNAL_ARGS)
{
       got_sighup = true;
}

static void
topo_service_siguser2(SIGNAL_ARGS)
{
	int save_errno = errno;
	SetLatch(MyLatch);
	errno = save_errno;
}

/* The proc id of the topo service */
static int *ptopo_service_pid = NULL;

int get_topo_service_pid()
{
	bool found = false;
	if (ptopo_service_pid)
		return *ptopo_service_pid;
	ptopo_service_pid = (int *)ShmemInitStruct("topo_service_pid", sizeof(pid_t), &found);
	if (!found)
		*ptopo_service_pid = 0;
	return *ptopo_service_pid;
}


/**
 * The main loop of the topology service, responsible for updating the
 * cluster topology, and handling requests to kill shard connection
 */
void TopoServiceMain(void)
{
	pqsignal(SIGHUP, topo_service_sighup);
	pqsignal(SIGTERM, topo_service_sigterm);
	pqsignal(SIGUSR2, topo_service_siguser2);
	InitializeTimeouts();
	IsBackgroundWorker = true;
	skip_tidsync = true;

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	on_exit_reset();
	InitPostgres("postgres", InvalidOid, NULL, InvalidOid, NULL, false);
	InitShardingSession();

	/* Set the topo service pid */
	(void) get_topo_service_pid();
	Assert (ptopo_service_pid);
	*ptopo_service_pid = MyProcPid;

	while (!got_sigterm)
	{
		/*
		 * In case of a SIGHUP, just reload the configuration.
		 */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		PG_TRY();
		{
			// task 1: handle topology update requests.
			enable_remote_timeout();
			ProcessShardingTopoReqs();
			disable_remote_timeout();

			// task 2: kill connections/queries
			{
				StartTransactionCommand();
				reapShardConnKillReqs();
				CommitTransactionCommand();
			}

			wait_latch(5000);
		}
		PG_CATCH();
		{
			EmitErrorReport();
			FlushErrorState();
			if (IsTransactionState())
				AbortCurrentTransaction();
			/* in case of crazy log */
			wait_latch(1000);
		}
		PG_END_TRY();
	}

	proc_exit(0);
}

