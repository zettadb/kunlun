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
#include "catalog/pg_shard.h"
#include "catalog/pg_shard_node.h"
#include "sharding/sharding.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "access/genam.h"
#include "access/htup.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "storage/lockdefs.h"
#include "access/sysattr.h"
#include "utils/fmgroids.h"
#include "catalog/indexing.h"
#include "access/htup_details.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "sharding/sharding_conn.h"
#include "storage/lwlock.h"
#include "pgstat.h"
#include "catalog/indexing.h"
#include "miscadmin.h"
#include "utils/snapmgr.h"
#include "executor/spi.h"
#include "access/remote_meta.h"
#include "sharding/cluster_meta.h"
#include "storage/lwlock.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include <sys/types.h>
#include <unistd.h>
#include "sharding/mysql/mysqld_error.h"

static HTAB *ShardNodeCache = NULL;
static HTAB *ShardCache = NULL;
Shard_id_t Invalid_shard_id = 0;
Shard_id_t First_shard_id = 1;
Shard_node_id_t Invalid_shard_node_id = 0;
Shard_node_id_t First_shard_node_id = 1;
static bool ShardCacheInvalidated = false;

static size_t LoadAllShards(bool init);
static Shard_node_t *find_node_by_ip_port(Shard_t *ps, const char *ip, uint16_t port);
static Shard_t* FindCachedShardInternal(const Oid shardid, bool cache_nodes);

extern int storage_ha_mode;

/*
 * Copy shard meta info from px to py
 * */
static void copy_shard_meta(Form_pg_shard px, Shard_t *py)
{
	strncpy(py->name.data, px->name.data, sizeof(py->name));
	py->id = px->id;
	py->master_node_id = px->master_node_id;
	Assert(px->num_nodes < 256);
	py->num_nodes = (uint8_t)px->num_nodes;
	py->storage_volumn = px->space_volumn;
	py->num_tablets = px->num_tablets;
}

static void copy_shard_node_meta(Form_pg_shard_node px, Shard_node_t *py, HeapTuple tup, Relation shardnoderel)
{
	/*
	 * Create shard node object and put to hash table.
	 * */
	strncpy(py->user_name.data, px->user_name.data, sizeof(py->user_name));

	//py->id = HeapTupleGetOid(tup);
	py->id = px->id;
	py->shard_id = px->shard_id;
	py->svr_node_id = px->svr_node_id;
	py->ip = px->ip; // ip address
	Assert(px->port < 0xffff);
	py->port = (uint16)px->port;
	py->ro_weight = px->ro_weight;

	// copy py->passwd
	bool isNull = false;
	MemoryContext old_memcxt;
	Datum value = heap_getattr(tup, Anum_pg_shard_node_passwd, RelationGetDescr(shardnoderel), &isNull);
	old_memcxt = MemoryContextSwitchTo(CacheMemoryContext);
	if (!isNull)
		py->passwd = TextDatumGetCString(value); // palloc's memory for the string
	else
		py->passwd = NULL;
	MemoryContextSwitchTo(old_memcxt);
}


/*
 * Set up pshard->shard_nodes[] reference to the Shard_node_t object
 * referenced by noderef->ptr, if noderef->ptr isn't already in
 * pshard->shard_nodes.
 * */
static void AddShard_node_ref_t(Shard_t *pshard, Shard_node_ref_t *noderef)
{
	Shard_node_ref_t *slot = NULL;
	Assert(noderef && noderef->id != Invalid_shard_node_id && noderef->ptr != NULL);
	Assert(pshard && pshard->id != Invalid_shard_id);
	Assert(noderef->ptr->shard_id == pshard->id);

	for (int i = 0; i < MAX_NODES_PER_SHARD; i++)
	{
		Shard_node_ref_t *ref = pshard->shard_nodes + i;
		if (ref->id == noderef->id)
		{
			Assert(ref->ptr != NULL);
			if (ref->ptr != noderef->ptr)
			{
				pfree(ref->ptr);
				ref->ptr = noderef->ptr;
			}
			return;
		}

		if (ref->id == Invalid_shard_node_id || ref->ptr == NULL)
		{
			Assert(ref->id == Invalid_shard_node_id && ref->ptr == NULL);
			slot = ref;
			break;
		}
		else
			Assert(ref->ptr && ref->ptr != noderef->ptr);
	}

	Assert(slot);
	slot->id = noderef->id;
	slot->ptr = noderef->ptr;
}


/*
 * Initialize shard and shard_node object caching.
 * */
void ShardCacheInit()
{
	HASHCTL		ctl;

	/*
	 * make sure cache memory context exists
	 */
	if (!CacheMemoryContext)
		CreateCacheMemoryContext();

	/*
	 * create hashtable that indexes the shard cache
	 */
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(Shard_ref_t);
	ShardCache = hash_create("Shard cache by shard OID", 256,
							 &ctl, HASH_ELEM | HASH_BLOBS);

	ctl.entrysize = sizeof(Shard_node_ref_t);
	ShardNodeCache = hash_create("Shard node cache by shard node OID", 1000,
							 &ctl, HASH_ELEM | HASH_BLOBS);
	LoadAllShards(true);
}


static size_t LoadAllShards(bool init)
{
	Relation	shardrel;
	SysScanDesc scan;
	HeapTuple	tup;
	bool found, end_local_txn = false;
	size_t nshards = 0;

	if (!IsTransactionState())
	{
		StartTransactionCommand();
		end_local_txn = true;
	}

	/* Grab an appropriate lock on the pg_shard relation */
	shardrel = heap_open(ShardRelationId, RowExclusiveLock);


	/*
	 * Do a full table scan to load & cache all shard info. Do not need index
	 * or scan key.
	 * */
	scan = systable_beginscan(shardrel, InvalidOid, false,
							  NULL, 0, NULL);

	while ((tup = systable_getnext(scan)) != NULL)
	{
		Form_pg_shard px = ((Form_pg_shard) GETSTRUCT(tup));

		Shard_ref_t* shardref = hash_search(ShardCache, &px->id, HASH_ENTER, &found);
		Assert(!init || !found);

		if (!found)
		{
			Shard_t *py = (Shard_t *)MemoryContextAllocZero(CacheMemoryContext, sizeof(Shard_t));

			/* copy fields */
			copy_shard_meta(px, py);
			shardref->ptr = py;
			shardref->id = py->id;
			// allow py->master_node_id to be 0 otherwise system can't startup.
		}
		nshards++; 
	}

	/* Clean up after the scan */
	systable_endscan(scan);
	heap_close(shardrel, RowExclusiveLock);


	shardrel = heap_open(ShardNodeRelationId, RowExclusiveLock);
	/*
	 * Do a full table scan to load & cache all shard node info.
	 * Do not need index or scan key.
	 * */
	scan = systable_beginscan(shardrel, InvalidOid, false,
							  NULL, 0, NULL);

	while ((tup = systable_getnext(scan)) != NULL)
	{
		Form_pg_shard_node px = ((Form_pg_shard_node) GETSTRUCT(tup));

		Shard_node_ref_t* shardnoderef = hash_search(ShardNodeCache, &px->id, HASH_ENTER, &found);
		Assert(!init || !found);
		Shard_node_t *py = NULL;

		if (!found)
		{
			//Assert(py == NULL); this is how the hashtable works.
			py = (Shard_node_t *)MemoryContextAllocZero(CacheMemoryContext, sizeof(Shard_node_t));
			copy_shard_node_meta(px, py, tup, shardrel);

			shardnoderef->ptr = py;
			shardnoderef->id = py->id;
		}
		else
			py = shardnoderef->ptr;

		Shard_ref_t *shardref = hash_search(ShardCache, &py->shard_id, HASH_FIND, &found);
		Assert(shardref != NULL && shardref->ptr != NULL && found);

		/*
		 * Reference the shard node in shard object. Find a Shard_node_ref_t
		 * free slot for the shard node from py.
		 * */
		AddShard_node_ref_t(shardref->ptr, shardnoderef);
	}

	/* Clean up after the scan */
	systable_endscan(scan);
	heap_close(shardrel, RowExclusiveLock);
	ShardCacheInvalidated = false; // set to true when a invalidate msg is processed.
	if (end_local_txn)
		CommitTransactionCommand();

	return nshards;
}

/*
 * Invalidate cached Shard_t objects, optionally including cached
 * Shard_node_t objects. Note that if and only if a Shard_node_t object
 * is cached is it referenced in owner Shard_t::shard_nodes array.
 * */
void InvalidateCachedShard(Oid shardid, bool includingShardNodes)
{
	if (!ShardCache)
		return;
	bool found = false;
	Shard_ref_t *shardref = (Shard_ref_t *)hash_search(ShardCache, &shardid, HASH_FIND, &found);
	if (!shardref)
	{
		Assert(!found);
		return;
	}

	Assert(found && shardref);

	if (includingShardNodes)
	{
		Shard_t *pshard = shardref->ptr;
		for (int i = 0; i < MAX_NODES_PER_SHARD; i++)
		{
			Shard_node_ref_t *ref = pshard->shard_nodes + i;
			if (ref->id != Invalid_shard_node_id)
			{
				Assert(ref->ptr);
				hash_search(ShardNodeCache, &ref->id, HASH_REMOVE, &found);
				pfree(ref->ptr);
				ref->ptr = NULL;
				ref->id = Invalid_shard_node_id;
				Assert(found);
			}
		}
	}

	pfree(shardref->ptr);
	hash_search(ShardCache, &shardid, HASH_REMOVE, NULL);
	ShardCacheInvalidated = true;
}

/*
 * Invalidate cached shard node from ShardNodeCache and the owner Shard_t
 * object if it's also cached.
 * */
void InvalidateCachedShardNode(Oid shardid, Oid nodeid)
{
	bool found = false;
	if (!ShardCache || !ShardNodeCache)
		return;
	Shard_ref_t *shardref = (Shard_ref_t *)hash_search(ShardCache, &shardid, HASH_FIND, &found);
	// It's likely that the shard object was not reloaded after invalidated last time.
	if (shardref == NULL)
	{
		Assert(!found);
		goto next_step;
	}

	Assert(found && shardref);

	// 1. Remove the shard node reference from the cached Shard_t object.
	found = false;

	for (int i = 0; i < MAX_NODES_PER_SHARD; i++)
	{
		/*
		 * All MAX_NODES_PER_SHARD slots must be checked because there can be
		 * {0,NULL} slots in the middle.
		 * */
		Shard_node_ref_t *pnode = shardref->ptr->shard_nodes+i;
		if (pnode->id == nodeid)
		{
			Assert(pnode->ptr != NULL);
			pnode->id = Invalid_shard_node_id;
			pnode->ptr = NULL;
			found = true;
			break;
		}
	}
	// Assert(found); it's likely that the node was not reloaded after invalidated last time.
	//
next_step:
	// 2. Remove the shardnode entry from ShardNodeCache.
	found = false;
	Shard_node_ref_t *shardnoderef = (Shard_node_ref_t *)hash_search(ShardNodeCache, &nodeid, HASH_FIND, &found);
	if (shardnoderef)
	{
		pfree(shardnoderef->ptr);
		shardnoderef->ptr = NULL;
	}

	hash_search(ShardNodeCache, &nodeid, HASH_REMOVE, &found);
	ShardCacheInvalidated = true;
}

Shard_t* FindCachedShard(const Oid shardid)
{
	return FindCachedShardInternal(shardid, false);
}


/*
 * Find from hash table the cached shard, if not found, scan tables to cache
 * it, and setup reference to its Shard_node_t objects.
 * */
static Shard_t* FindCachedShardInternal(const Oid shardid, bool cache_nodes)
{
	bool found = false;
	Assert(shardid != Invalid_shard_id);
	if (shardid == Invalid_shard_id)
		return NULL;

	bool commit_txn = false;
	Shard_t *pshard = NULL;
	Shard_ref_t *shardref = (Shard_ref_t *)hash_search(ShardCache, &shardid, HASH_FIND, &found);
	if (shardref)
	{
		if (shardref->ptr->master_node_id == InvalidOid)
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Kunlun-db: Shard (%s, %u)'s primary node unknown.",
							shardref->ptr->name.data, shardref->ptr->id)));

		if (!cache_nodes)
			return shardref->ptr;
		else
		{
			pshard = shardref->ptr;
			if (!IsTransactionState())
			{
				StartTransactionCommand();
				commit_txn = true;
			}
			goto fetch_nodes;
		}
	}

	// The object was invalidated, fetch it from pg_shard table and cache it.
	ScanKeyData key, key1;
	SysScanDesc scan;
	HeapTuple	tuple;
	int nfound = 0;
	if (!IsTransactionState())
	{
		StartTransactionCommand();
		commit_txn = true;
	}

	Relation shardrel = heap_open(ShardRelationId, RowExclusiveLock);
	ScanKeyInit(&key,
				/*ObjectIdAttributeNumber*/Anum_pg_shard_id,
				BTEqualStrategyNumber,
				F_OIDEQ, shardid);

	scan = systable_beginscan(shardrel, ShardOidIndexId, true,
							  NULL, 1, &key);
	while ((tuple = systable_getnext(scan)) != NULL)
	{
		Form_pg_shard px = ((Form_pg_shard) GETSTRUCT(tuple));
		pshard = (Shard_t*)MemoryContextAllocZero(CacheMemoryContext, sizeof(Shard_t));

		/* copy fields */
		copy_shard_meta(px, pshard);
		// pshard->id = HeapTupleGetOid(tuple);

		Shard_ref_t* shardref = hash_search(ShardCache, &pshard->id, HASH_ENTER, &found);
		Assert(!found);
		shardref->ptr = pshard;
		shardref->id = pshard->id;
		nfound++;
	}

	systable_endscan(scan);
	heap_close(shardrel, RowExclusiveLock);
	if (!(nfound == 1 && pshard != NULL))
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: Shard (%u) not found.", shardid)));
	if (pshard->master_node_id == InvalidOid)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: Shard (%s, %u)'s primary node unknown.",
						pshard->name.data, pshard->id)));
fetch_nodes:
	/*
	 * Fetch all shard nodes belonging to this shard and cache those not
	 * already in ShardNodeCache, and add node refs into pshard->shard_nodes array.
	 */
	shardrel = heap_open(ShardNodeRelationId, RowExclusiveLock);

	ScanKeyInit(&key1,
				Anum_pg_shard_node_shard_id, BTEqualStrategyNumber,
				F_OIDEQ, shardid);
	scan = systable_beginscan(shardrel, ShardNodeShardIdIndexId, true,
							  NULL, 1, &key1);

	nfound = 0;
	while ((tuple = systable_getnext(scan)) != NULL)
	{
		Form_pg_shard_node px = ((Form_pg_shard_node) GETSTRUCT(tuple));
		Oid node_oid = px->id; //HeapTupleGetOid(tuple);
		Shard_node_ref_t *noderef = NULL;
		if ((noderef = hash_search(ShardNodeCache, &node_oid, HASH_FIND, &found)) == NULL)
		{
			Assert(!found);
			Shard_node_t *pnode = (Shard_node_t *)MemoryContextAllocZero(CacheMemoryContext, sizeof(Shard_node_t));
			copy_shard_node_meta(px, pnode, tuple, shardrel);

			noderef = hash_search(ShardNodeCache, &node_oid, HASH_ENTER, &found);
			Assert(noderef && !found);
			noderef->ptr = pnode;
			noderef->id = node_oid;
		}
		else
			Assert(found && noderef->ptr);
		Assert(noderef);
		AddShard_node_ref_t(pshard, noderef);
		nfound++;
	}

	Assert(nfound > 0);
	systable_endscan(scan);
	heap_close(shardrel, RowExclusiveLock);
	if (commit_txn)
		CommitTransactionCommand();
	return pshard;
}

/*
 * Find cached Shard_node_t objects. If not cached, scan table to cache it and
 * setup its owner's reference to it if the owner is also cached.
 * */
Shard_node_t* FindCachedShardNode(Oid shardid, Oid nodeid)
{
	bool found = false;

	Assert(shardid != Invalid_shard_id && nodeid != Invalid_shard_node_id);
	if (shardid == Invalid_shard_id || nodeid == Invalid_shard_node_id)
		return NULL;

	Shard_node_ref_t *shardnoderef = (Shard_node_ref_t *)hash_search(ShardNodeCache, &nodeid, HASH_FIND, &found);
	if (shardnoderef)
	{
		return shardnoderef->ptr;
	}

	ScanKeyData key;
	SysScanDesc scan;
	HeapTuple	tup;
	int nfound = 0;
	Shard_node_t *pnode = NULL;
	bool end_txn = false;

	if (!IsTransactionState())
	{
		StartTransactionCommand();
		end_txn = true;
	}
	Relation shardrel = heap_open(ShardNodeRelationId, RowExclusiveLock);
	ScanKeyInit(&key,
				/*ObjectIdAttributeNumber*/Anum_pg_shard_node_id,
				BTEqualStrategyNumber,
				F_OIDEQ, nodeid);

	scan = systable_beginscan(shardrel, ShardNodeOidIndexId, true,
							  NULL, 1, &key);
	while ((tup = systable_getnext(scan)) != NULL)
	{
		Form_pg_shard_node px = ((Form_pg_shard_node) GETSTRUCT(tup));
		pnode = (Shard_node_t *)MemoryContextAllocZero(CacheMemoryContext, sizeof(Shard_node_t));

		/* copy fields */
		copy_shard_node_meta(px, pnode, tup, shardrel);

		Shard_node_ref_t *noderef = hash_search(ShardNodeCache, &nodeid, HASH_ENTER, &found);
		Assert(!found);
		noderef->id = pnode->id;
		noderef->ptr = pnode;
		nfound++;
		Shard_t *pshard = (Shard_t *)hash_search(ShardCache, &shardid, HASH_FIND, &found);
		if (pshard)
		{
			AddShard_node_ref_t(pshard, noderef);
		}
	}
	Assert(nfound == 1);
	systable_endscan(scan);
	heap_close(shardrel, RowExclusiveLock);
	if (end_txn)
		CommitTransactionCommand();

	return pnode;
}



/*
 * Find from cache the shard with minimal 'storage_volume'(which = 1) or
 * 'num_tablets'(which = 2). To be used as the target shard to store a
 * new table.
 * */
Shard_t *FindBestCachedShard(int which)
{
	HASH_SEQ_STATUS seq_status;
	hash_seq_init(&seq_status, ShardCache);
	Shard_ref_t *ref;
	Shard_t *ps, *best = NULL;
	uint32_t minval = UINT_MAX;

	if (ShardCacheInvalidated)
	{
		LoadAllShards(false);
	}

	while ((ref = hash_seq_search(&seq_status)) != NULL)
	{
		ps = ref->ptr;
		if (which == 1 && (!best || ps->storage_volumn < minval))
		{
			minval = ps->storage_volumn;
			best = ps;
		}

		if (which == 2 && (!best || ps->num_tablets < minval))
		{
			minval = ps->num_tablets;
			best = ps;
		}
	}

	if (!best)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: No shard available to store tables.")));
	}

	best->num_tablets++;
	/*
	 * TODO:
	 * This is a rough guess, to be accurate, clustermgr should periodically
	 * update each computing node's stats.
	 * */
	best->storage_volumn += 100;

	return best;
}


/*
 * @retval the NO. of entries in ShardCache.
 * */
size_t startShardCacheSeq(HASH_SEQ_STATUS *seq_status)
{
	//if (ShardCacheInvalidated)
	{
		/*
		  Do this unconditionally, otherwise global deadlock detector can't
		  see shards in a newly created computing node that has not
		  restarted yet, even if the node has been added to the cluster.

		  This function is not in performance critical path and in all other
		  cases such issue don't exist.
		*/
		LoadAllShards(false);
	}

	size_t ret = hash_get_num_entries(ShardCache);

	hash_seq_init(seq_status, ShardCache);
	return ret;
}

size_t get_num_all_valid_shards()
{
	return LoadAllShards(false);
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
*/
static int FindCurrentMasterNodeId(Oid shardid, Oid *pmaster_nodeid)
{
	Assert(shardid != InvalidOid && pmaster_nodeid);
	static const char *fetch_gr_members_sql = "select MEMBER_HOST, MEMBER_PORT from performance_schema.replication_group_members where channel_name='group_replication_applier' and MEMBER_STATE='ONLINE' and MEMBER_ROLE='PRIMARY'";
	static size_t sqllen = 0;
	if (!sqllen) sqllen = strlen(fetch_gr_members_sql);
	int num_conn_fails = 0;
	Shard_t *ps = FindCachedShardInternal(shardid, true);
	Shard_node_ref_t *pnoderef = ps->shard_nodes;
	for (int i = 0; i < ps->num_nodes; i++)
	{
		Shard_node_t *pnode = pnoderef[i].ptr;
		AsyncStmtInfo *asi = NULL;
		PG_TRY(); {
			asi = GetAsyncStmtInfoNode(pnode->shard_id, pnode->id, false);
		} PG_CATCH() ; {
			/*
			  can't connect to pnode, don't throw error here otherwise we
			  never can find current master if one node is down. So we downgrade
			  the error to warning and output it to server log.
			  We can do this because GetAsyncStmtInfoNode() doesn't touch
			  shared memory otherwise we could damage the shared memory and all
			  other backends could crash.
			*/
			num_conn_fails++;
			/*
			  Get rid of the exception, log it to server log only to free
			  error stack space.
			  Do not abort the current pg txn, keep running in it. it's not pg
			  error that the mysql node can't be reached.
			 */
			HOLD_INTERRUPTS();

			downgrade_error();
			errfinish(0);
			FlushErrorState();
			RESUME_INTERRUPTS();

		} PG_END_TRY();
		if (asi)
			append_async_stmt(asi, fetch_gr_members_sql, sqllen, CMD_SELECT, false, SQLCOM_SELECT);
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
	send_multi_stmts_to_multi();

	size_t num_asis = GetAsyncStmtInfoUsed();
	elog(LOG, "Looking for primary node of shard %s(%u) among %lu nodes, with %d unavailable nodes.",
		 ps->name.data, shardid, num_asis, num_conn_fails);
	/*
	 * Receive results.
	 * */
	Oid master_nodeid = InvalidOid;
	const char *master_ip = NULL;
	uint16_t master_port = 0;
	int num_masters = 0; // NO. of unique master nodes found from storage shard nodes.
	int num_quorum = 0; // NO. of shard nodes which affirm master_node_id to be new master.
	int num_unknowns = 0; // NO. of shard nodes who doesn't know about current master.
	int num_new_masters = 0; // NO. of found masters not in pg_shard_node

	for (size_t i = 0; i < num_asis; i++)
	{
		CHECK_FOR_INTERRUPTS();
		AsyncStmtInfo *asi = GetAsyncStmtInfoByIndex(i);
		if (!ASIConnected(asi)) continue; // connection failed, node unavailable.
		MYSQL_RES *mres = asi->mysql_res;
		int nrows = 0;

		if (mres == NULL)
		{
			elog(WARNING, "Primary node unknown in shard %s(%u) node (%s, %u, %u).",
				 ps->name.data, asi->shard_id, asi->conn->host, asi->conn->port, asi->node_id);
			free_mysql_result(asi);
			num_unknowns++;
			continue;
		}
		do {
			MYSQL_ROW row = mysql_fetch_row(mres);
			if (row == NULL)
			{
				if (nrows == 0)
				{
					num_unknowns++;
					elog(WARNING, "Primary node unknown in shard %s(%u) node (%s, %u, %u).",
				 		 ps->name.data, asi->shard_id, asi->conn->host, asi->conn->port, asi->node_id);
				}
				check_mysql_fetch_row_status(asi);
				free_mysql_result(asi);
				break;
			}
			nrows++;
			const char *ip = row[0];
			char *endptr = NULL;
			uint16_t port = strtol(row[1], &endptr, 10);
			Shard_node_t *sn = find_node_by_ip_port(ps, ip, port);
			if (sn == NULL)
			{
				elog(WARNING, "Found a new primary node(%s, %u) of shard %s(%u) not in pg_shard_node, "
					 "meta data in pg_shard_node isn't up to date, retry later.",
					 ip, port, ps->name.data, shardid);
				num_new_masters++;
				continue;
			}

			if (master_nodeid == InvalidOid)
			{
				master_nodeid = sn->id;
				master_ip = pstrdup(ip);
				master_port = port;
				Assert(num_masters == 0);
				num_masters++;
				num_quorum++;
			}
			else if (master_nodeid != sn->id)
			{
				elog(WARNING, "Found a new primary node(%s, %u, %u) of shard %s(%u) when we already found a new primary node (%s, %u, %u),"
					 " might be a brain split bug of MGR, but more likely a primary switch is happening right now, retry later.",
					 ip, port, sn->id, ps->name.data, shardid, master_ip, master_port, master_nodeid);
				num_masters++;
			}
			else
			{
				Assert(master_nodeid == sn->id);
				num_quorum++;
			}
		} while (true);
	}

	if (num_new_masters > 0)
	{
		elog(WARNING, "Found %d new primary nodes in shard %s(%u) which are not registered in pg_shard_node and can't be used by current computing node. Primary node is unknown in %d nodes, with %d unavailable nodes. Retry later.",
			 num_new_masters, ps->name.data, shardid, num_unknowns, num_conn_fails);
		return -1;
	}

	if (num_masters == 0)
		elog(WARNING, "Primary node not found in shard %s(%u), it's unknown in %d nodes, with %d unavailable nodes. Retry later.",
			 ps->name.data, shardid, num_unknowns, num_conn_fails);
	if (num_masters > 1)
		elog(WARNING, "Multiple(%d) primary nodes found in shard %s(%u). It's unknown in %d nodes, with %d unavailable nodes. Retry later.",
			 num_masters, ps->name.data, shardid, num_unknowns, num_conn_fails);
	if (num_masters == 1)
		elog(LOG, "Found new primary node (%s, %u, %u) in shard %s(%u), affirmed by %d nodes of the shard. The primary is unknown in %d nodes, with %d unavailable nodes.",
			 master_ip, master_port, master_nodeid, ps->name.data, shardid, num_quorum, num_unknowns, num_conn_fails);

	*pmaster_nodeid = master_nodeid;
	return num_masters;
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
	heap_close(pg_shard_rel, RowExclusiveLock);
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
	if (storage_ha_mode == HA_NO_REP) return false;

	bool done = false;
	LWLockAcquire(ShardingTopoCheckLock, LW_EXCLUSIVE);
	if (ShardingTopoChkReqs->endidx >= MAX_SHARDS)
		goto end;
	for (int i = 0; i < ShardingTopoChkReqs->endidx; i++)
		if (shardid == ShardingTopoChkReqs->shardids[i])
			goto done;

	ShardingTopoChkReqs->shardids[ShardingTopoChkReqs->endidx++] = shardid;
done:
	done = true;
end:
	LWLockRelease(ShardingTopoCheckLock);
	if (done && g_remote_meta_sync->main_applier_pid != 0 &&
		getpid() != g_remote_meta_sync->main_applier_pid)
		kill(g_remote_meta_sync->main_applier_pid, SIGUSR2);
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

	if (storage_ha_mode == HA_NO_REP) return 0;
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


void ProcessShardingTopoReqs()
{
	Oid reqs[MAX_SHARDS], fail_reqs[MAX_SHARDS];
	memset(reqs, 0, sizeof(reqs));
	int nreqs = 0, nfail_reqs = 0;

	LWLockAcquire(ShardingTopoCheckLock, LW_EXCLUSIVE);
	if (ShardingTopoChkReqs->endidx > 0)
	{
		memcpy(reqs, ShardingTopoChkReqs->shardids, sizeof(Oid)*ShardingTopoChkReqs->endidx);
		nreqs = ShardingTopoChkReqs->endidx;
		ShardingTopoChkReqs->endidx = 0;
	}
	LWLockRelease(ShardingTopoCheckLock);

	if (nreqs > 0)
		elog(LOG, "Start processing %d sharding topology checks.", nreqs);

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
		elog(LOG, "Completed processing %d sharding topology checks, failed %d checks and will retry later.", nreqs - nfail_reqs, nfail_reqs);
}

static Shard_node_t *find_node_by_ip_port(Shard_t *ps, const char *ip, uint16_t port)
{
	Shard_node_ref_t *pnoderef = ps->shard_nodes;
	for (int i = 0; i < ps->num_nodes; i++)
	{
		Shard_node_t *sn = pnoderef[i].ptr;
		if (strcmp(sn->ip.data, ip) == 0 && port == sn->port)
			return sn;
	}
	return NULL;
}


void RequestShardingTopoCheckAllStorageShards()
{

	if (storage_ha_mode == HA_NO_REP) return;
	SetCurrentStatementStartTimestamp();
	bool end_txn = false;

	if (!IsTransactionState())
	{
		StartTransactionCommand();
		end_txn = true;
	}
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	
	Relation pg_shard_rel = heap_open(ShardRelationId, RowExclusiveLock);

	HeapTuple tup = NULL;
	SysScanDesc scan;

	scan = systable_beginscan(pg_shard_rel, InvalidOid, false, NULL, 0, NULL);
	LWLockAcquire(ShardingTopoCheckLock, LW_EXCLUSIVE);
	while ((tup = systable_getnext(scan)) != NULL)
	{
		Form_pg_shard ps = ((Form_pg_shard) GETSTRUCT(tup));
		if (ShardingTopoChkReqs->endidx >= MAX_SHARDS)
		{
			elog(ERROR, "Serious error: found more than %u shards in pg_shard table.", MAX_SHARDS);
			break;
		}
		ShardingTopoChkReqs->shardids[ShardingTopoChkReqs->endidx++] = ps->id;
	}

	LWLockRelease(ShardingTopoCheckLock);
	systable_endscan(scan);
	heap_close(pg_shard_rel, RowExclusiveLock);
	SPI_finish();
	PopActiveSnapshot();
	if (end_txn) CommitTransactionCommand();
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
	const bool curproc_is_main_applier = (g_remote_meta_sync->main_applier_pid != 0 &&
		getpid() == g_remote_meta_sync->main_applier_pid);

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
		if (locked || curproc_is_main_applier) return;

		/*
		  no big deal to drop such a req, don't wait here infinitely.
		  statement timeout mechanism can't work here since enable_timeout()
		  not called explicitly and not in a txn.
		*/
		if (cntr++ > 100)
			return;
		elog(WARNING, "shard conn kill req queue is full, waiting to enq a req.");

		if (!curproc_is_main_applier)
			kill(g_remote_meta_sync->main_applier_pid, SIGUSR2);

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

	if (!curproc_is_main_applier)
		kill(g_remote_meta_sync->main_applier_pid, SIGUSR2);
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
	uint32_t metashard_connid = 0;
	uint32_t pos = sizeof(ShardConnKillReqQ);
	int num_shard_reqs = 0, num_meta_reqs = 0;
	Oid cur_shardid = 0;
	MetaShardKillConnReqSection metareqs;
	memset(&metareqs, 0, sizeof(metareqs));

	LWLockAcquire(KillShardConnReqLock, LW_EXCLUSIVE);
	PG_TRY();
	for (uint32_t i = 0; i < shard_conn_kill_reqs->nreqs; i++)
	{
		ShardConnKillReq *req = (ShardConnKillReq *)((char*)shard_conn_kill_reqs + pos);
		Assert(req->type == 1 || req->type == 2);
		ShardNodeConnId *sncs = req->entries;

		if (req->num_ents == 0) continue;

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
			AsyncStmtInfo*asi = GetAsyncStmtInfoNode(cur_shardid, sncs[j].nodeid, true);
			/*
			  There is no txnal cxt here, so allocing from top-memcxt and
			  should tell async-comm module to free it.
			*/
			char *stmt = (char*)palloc(64);
			int slen = snprintf(stmt, 64, "kill %s %u",
				(req->type == 1 ? "connection" : "query"), sncs[j].connid);
			Assert(slen < 64);
			asi->ignore_error = ER_NO_SUCH_THREAD;
			append_async_stmt(asi, stmt, slen, CMD_UTILITY, true, SQLCOM_KILL);
		}

		num_shard_reqs += req->num_ents;
		pos = req->next_req_off;
	}

	shard_conn_kill_reqs->write_pos = sizeof(ShardConnKillReqQ);
	shard_conn_kill_reqs->nreqs = 0;

	LWLockRelease(KillShardConnReqLock);
	PG_CATCH(); // the GetAsyncStmtInfoNode() could fail of mysql connect error.
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
		RequestShardingTopoCheck(cur_shardid);
	else
		PG_RE_THROW();
	PG_END_TRY();

	if (num_shard_reqs == 0) goto do_meta;

	PG_TRY();
	/*
	  Connection could break while sending the stmts or receiving results,
	  and this is perfectly OK, topo check and kill-conn reqs are all enqueued
	*/
	send_multi_stmts_to_multi();
	PG_CATCH();

	PG_END_TRY();

	// reset ignore_err fields.
	int nasis = GetAsyncStmtInfoUsed();
	for (int i = 0; i < nasis; i++)
	{
		AsyncStmtInfo *asi = GetAsyncStmtInfoByIndex(i);
		asi->ignore_error = 0;
	}
do_meta:
	if (num_shard_reqs > 0 || num_meta_reqs > 0)
		elog(INFO, "Reaped %d shard kill reqs and %d meta kill reqs.",
			num_shard_reqs, num_meta_reqs);
	if (num_meta_reqs == 0) return;
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

void inform_cluster_log_applier_main()
{
	kill(g_remote_meta_sync->main_applier_pid, SIGUSR2);
}

