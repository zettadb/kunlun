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
#include "storage/lockdefs.h"
#include "storage/lwlock.h"
#include "storage/lwlock.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"

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


/*
 * Cache reference, in order for Shard_node_t objects to be
 * cached/invalidated seperately.
 * */
typedef struct Shard_node_ref_t
{
  Shard_node_id_t id;
  Shard_node_t *ptr; // this is 0 if the node is invalidated from cache.
} Shard_node_ref_t;

typedef struct Shard_t
{
  NameData name;
  uint8_t master_node_idx; // master node index into shard_nodes.
  uint8_t num_nodes; // number of nodes, including master;
  Shard_id_t id; // shard id
  Shard_node_id_t master_node_id; // this is mainly needed at cache init.
  /*int64 last_master_switch_id; each master switch is logged in backend shard,
	this is the id of the log record of the last master switch . No need, the
	last one always has the biggest id. */
  Shard_node_ref_t shard_nodes[MAX_NODES_PER_SHARD];

  // Below fields changes much more frequently than above, they should be in
  // another cache line.
  uint32_t storage_volumn;// data volumn in KBs
  uint32_t num_tablets;// number of tablets, including whole tables
} Shard_t;

typedef struct Shard_ref_t
{
  Oid id; // shard id
  Shard_t *ptr;
} Shard_ref_t;

static size_t LoadAllShards(bool init);
static Shard_node_t *find_node_by_ip_port(Shard_t *ps, const char *ip,
	uint16_t port);
static bool FindCachedShardInternal(const Oid shardid, bool cache_nodes,
	Shard_t* out, Shard_node_t **pshard_nodes);
static void free_shard_nodes(Shard_node_t *snodes);

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
	Assert(px->port < 0xffff);
	py->port = (uint16)px->port;
	py->ro_weight = px->ro_weight;

	// copy py->passwd
	bool isNull = false;
	MemoryContext old_memcxt;
	Datum value = heap_getattr(tup, Anum_pg_shard_node_hostaddr, RelationGetDescr(shardnoderel), &isNull);
	old_memcxt = MemoryContextSwitchTo(CacheMemoryContext);
	if (!isNull)
		py->hostaddr = TextDatumGetCString(value); // palloc's memory for the string
	else
		py->hostaddr = NULL;

	value = heap_getattr(tup, Anum_pg_shard_node_passwd, RelationGetDescr(shardnoderel), &isNull);
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
	Relation	shardrel, shardnoderel;
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
	shardnoderel = heap_open(ShardNodeRelationId, RowExclusiveLock);


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


	/*
	 * Do a full table scan to load & cache all shard node info.
	 * Do not need index or scan key.
	 * */
	scan = systable_beginscan(shardnoderel, InvalidOid, false,
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
			copy_shard_node_meta(px, py, tup, shardnoderel);

			shardnoderef->ptr = py;
			shardnoderef->id = py->id;
		}
		else
		{
			py = shardnoderef->ptr;
			Assert(py->id == px->id);
		}
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

	heap_close(shardnoderel, NoLock);
	heap_close(shardrel, NoLock);
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
				pfree(ref->ptr->hostaddr);
				pfree(ref->ptr->passwd);
				pfree(ref->ptr);
				ref->ptr = NULL;
				ref->id = Invalid_shard_node_id;
				//Assert(found); the node may have been evicted from the cache.
			}
		}
	}

	pfree(shardref->ptr);
	hash_search(ShardCache, &shardid, HASH_REMOVE, NULL);
	ShardCacheInvalidated = true;
	elog(DEBUG1, "Invalidated cached shard %u.", shardid);
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
	Shard_node_ref_t *shardnoderef = (Shard_node_ref_t *)
		hash_search(ShardNodeCache, &nodeid, HASH_FIND, &found);
	if (shardnoderef)
	{
		pfree(shardnoderef->ptr->hostaddr);
		pfree(shardnoderef->ptr->passwd);
		pfree(shardnoderef->ptr);
		shardnoderef->ptr = NULL;
	}

	hash_search(ShardNodeCache, &nodeid, HASH_REMOVE, &found);
	ShardCacheInvalidated = true;
	elog(DEBUG1, "Invalidated cached shard node %u.%u.", shardid, nodeid);
}

bool ShardExists(const Oid shardid)
{
	return FindCachedShardInternal(shardid, false, NULL, NULL);
}


/*
 * Find from hash table the cached shard, if not found, scan tables to cache
 * it. and if 'cache_nodes is true, setup reference to its Shard_node_t objects.
 * Return the found shard by copying it to 'out', shallow copy all fields
 * except 'shard_nodes[i].ptr' objects.
 * Only deep copy shard_nodes[i].ptr objects into an
 * array held by *pshard_nodes and only when requested
 * to(i.e. pshard_nodes not NULL and cache_nodes is true).

 * @reval true if the cached Shard_t object is found, and it's copied to out;
 *        false if it's not found and *out is intact.
 *  Note that if not deep copied, out->shard_nodes[i].ptr field of any slots
 * in the array can NOT be accessed because they ref
 *  cached shard nodes which could be invalidated too.
 * */
static bool
FindCachedShardInternal(const Oid shardid, bool cache_nodes,
	Shard_t* out, Shard_node_t **pshard_nodes)
{
	bool found = false, found_shard = false;
	Assert(shardid != Invalid_shard_id);
	if (shardid == Invalid_shard_id)
		return NULL;

	bool commit_txn = false;
	if (!IsTransactionState())
	{
		StartTransactionCommand();
		commit_txn = true;
	}

	Shard_t *pshard = NULL;
	Relation shardrel = heap_open(ShardRelationId, RowExclusiveLock);
	Relation shardnoderel = heap_open(ShardNodeRelationId, RowExclusiveLock);
	ScanKeyData key, key1;
	SysScanDesc scan;
	HeapTuple	tuple;
	int nfound = 0;
	Shard_ref_t *shardref = (Shard_ref_t *)
		hash_search(ShardCache, &shardid, HASH_FIND, &found_shard);

	if (shardref)
	{
		Assert(shardref->id == shardid);
		if (!cache_nodes)
		{
			pshard = shardref->ptr;
			Assert(pshard->id == shardid);
			if (out) memcpy(out, pshard, sizeof(*out));
			goto end;
		}
		else
		{
			pshard = shardref->ptr;
			Assert(pshard->id == shardid);
			goto fetch_nodes;
		}
	}

	// The object was invalidated, fetch it from pg_shard table and cache it.
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
		Assert(pshard->id == shardid);

		shardref = hash_search(ShardCache, &pshard->id, HASH_ENTER, &found);
		Assert(!found);
		shardref->ptr = pshard;
		shardref->id = pshard->id;
		nfound++;
	}

	systable_endscan(scan);
	if (!(nfound == 1 && pshard != NULL && pshard->id == shardid))
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: Shard (%u) not found.", shardid)));
fetch_nodes:

	/*
	 * Fetch all shard nodes belonging to this shard and cache those not
	 * already in ShardNodeCache, and add node refs into pshard->shard_nodes array.
	 */

	Assert(pshard->id == shardid);
	ScanKeyInit(&key1,
				Anum_pg_shard_node_shard_id, BTEqualStrategyNumber,
				F_OIDEQ, shardid);
	scan = systable_beginscan(shardnoderel, ShardNodeShardIdIndexId, true,
							  NULL, 1, &key1);
	Assert(pshard->id == shardid);

	nfound = 0;

	while ((tuple = systable_getnext(scan)) != NULL)
	{
		Form_pg_shard_node px = ((Form_pg_shard_node) GETSTRUCT(tuple));
		Oid node_oid = px->id;
		Shard_node_ref_t *noderef = NULL;
		Assert(pshard->id == shardid);
		noderef = hash_search(ShardNodeCache, &node_oid, HASH_ENTER, &found);
		if (!found)
		{
			Shard_node_t *pnode = (Shard_node_t *)MemoryContextAllocZero(
				CacheMemoryContext, sizeof(Shard_node_t));
			copy_shard_node_meta(px, pnode, tuple, shardnoderel);
			noderef->ptr = pnode;
			noderef->id = node_oid;
		}
		else
			Assert(found && noderef && noderef->id == node_oid &&
				   noderef->ptr && noderef->ptr->id == node_oid);
		Assert(noderef);
		AddShard_node_ref_t(pshard, noderef);
		nfound++;
	}

	if (nfound == 0)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: No nodes found for shard (%u, %s).",
				 		shardid, pshard->name.data)));
	Assert(nfound > 0);
	systable_endscan(scan);

	/*
	  copy to out. all fields of out could be shallow copied and used by
	  our caller except shard_nodes, deep copy the array and its Shard_node_t
	  objects.
	*/
	if (out)
	{
		memcpy(out, pshard, sizeof(*out));
		if (pshard_nodes)
		{
			Assert(pshard->num_nodes <= MAX_NODES_PER_SHARD);
			*pshard_nodes = MemoryContextAllocZero(CacheMemoryContext,
				sizeof(Shard_node_t)*MAX_NODES_PER_SHARD);
			for (int i = 0, j = 0; i < MAX_NODES_PER_SHARD; i++)
			{
				if (!pshard->shard_nodes[i].ptr) continue;
				Shard_node_t *psnode = (*pshard_nodes) + j;

				memcpy(psnode, pshard->shard_nodes[i].ptr, sizeof(Shard_node_t));
				out->shard_nodes[j].ptr = psnode;
				psnode->hostaddr = MemoryContextStrdup(CacheMemoryContext, psnode->hostaddr);
				psnode->passwd = MemoryContextStrdup(CacheMemoryContext, psnode->passwd);
				j++;
			}
		}
	}
end:
	heap_close(shardnoderel, NoLock);
	heap_close(shardrel, NoLock);
	if (commit_txn)
		CommitTransactionCommand();
	return pshard != NULL;
}

/*
 * Find cached Shard_node_t objects. If not cached, scan table to cache it and
 * setup its owner's reference to it if the owner is also cached.
 * */
bool FindCachedShardNode(Oid shardid, Oid nodeid, Shard_node_t*out)
{
	bool found = false;

	Assert(shardid != Invalid_shard_id && nodeid != Invalid_shard_node_id);
	if (shardid == Invalid_shard_id || nodeid == Invalid_shard_node_id)
		return NULL;

	bool end_txn = false;
	if (!IsTransactionState())
	{
		StartTransactionCommand();
		end_txn = true;
	}
	Shard_node_t *pnode = NULL;
	Relation shardrel = heap_open(ShardNodeRelationId, RowExclusiveLock);
	Shard_node_ref_t *shardnoderef = (Shard_node_ref_t *)
		hash_search(ShardNodeCache, &nodeid, HASH_FIND, &found);
	if (shardnoderef)
	{
		pnode = shardnoderef->ptr;
		goto end;
	}

	ScanKeyData key;
	SysScanDesc scan;
	HeapTuple	tup;
	int nfound = 0;

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

	if (nfound == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: Shard node (%u %u) not found.",
				 shardid, nodeid)));

	if (nfound > 1)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: Duplicate shard node (%u %u) found, found %u nodes.",
				 shardid, nodeid, nfound)));

	Assert(nfound == 1);
	systable_endscan(scan);
end:
	if (out)
	{
		memcpy(out, pnode, sizeof(*out));
		// pnode->hostaddr/passwd alloced in Cachememcxt
		out->hostaddr = pstrdup(pnode->hostaddr);
		out->passwd = pstrdup(pnode->passwd);
	}

	heap_close(shardrel, NoLock);
	if (end_txn)
		CommitTransactionCommand();

	return pnode != NULL;
}



/*
 * Find from cache the shard with minimal 'storage_volume'(which = 1) or
 * 'num_tablets'(which = 2). To be used as the target shard to store a
 * new table.
 * */
Oid FindBestShardForTable(int which, Relation rel)
{
	HASH_SEQ_STATUS seq_status;
	Shard_ref_t *ref;
	Shard_t *ps, *best = NULL;
	uint32_t minval = UINT_MAX;

	if (ShardCacheInvalidated)
	{
		LoadAllShards(false);
	}

	int num_shards = 0;
	List *pshard_list = NULL;
	hash_seq_init(&seq_status, ShardCache);

	while ((ref = hash_seq_search(&seq_status)) != NULL)
	{
		num_shards++;
		ps = ref->ptr;
		if (which  == 0) pshard_list = lappend(pshard_list, ps);
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
	
	if (which  == 0) best = list_nth(pshard_list, rand() % num_shards);

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
	best->storage_volumn += 4096;

	return best->id;
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

	Storage_HA_Mode ha_mode = storage_ha_mode();
	if (ha_mode == HA_MGR)
		fetch_gr_members_sql = "select MEMBER_HOST, MEMBER_PORT from performance_schema.replication_group_members where channel_name='group_replication_applier' and MEMBER_STATE='ONLINE' and MEMBER_ROLE='PRIMARY'";
	else if (ha_mode == HA_RBR)
	{
		fetch_gr_members_sql = "select host, port, Channel_name from mysql.slave_master_info";
		Assert(false);
	}
	else Assert(ha_mode == HA_NO_REP);

	size_t sqllen = 0;
	if (fetch_gr_members_sql) sqllen = strlen(fetch_gr_members_sql);

	int num_conn_fails = 0;
	Shard_t shard;
	Shard_t *ps = NULL;
	Shard_node_t *pshard_nodes = NULL;
	if (FindCachedShardInternal(shardid, true, &shard, &pshard_nodes)) ps = &shard;
	else
	{
		if (pmaster_nodeid) *pmaster_nodeid = 0;
		if (pshard_nodes) free_shard_nodes(pshard_nodes);
		elog(WARNING, "Shard %u not found while looking for its current master node.", shardid);
		return -3;
	}

	Shard_node_ref_t *pnoderef = ps->shard_nodes;
	if (ha_mode == HA_NO_REP)
	{
		Assert(ps->num_nodes == 1);
		*pmaster_nodeid = pnoderef[0].ptr->id;
		if (pshard_nodes) free_shard_nodes(pshard_nodes);
		return 1;
	}

	for (int i = 0; i < MAX_NODES_PER_SHARD; i++)
	{
		Shard_node_t *pnode = pnoderef[i].ptr;
		if (!pnode) continue;

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
		if (pshard_nodes) free_shard_nodes(pshard_nodes);
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
	if (pshard_nodes) free_shard_nodes(pshard_nodes);
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
	Storage_HA_Mode ha_mode = storage_ha_mode();
	if (ha_mode == HA_NO_REP) return false;

	int pid;
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
	pid = get_topo_service_pid();
	if (done && pid != 0 && pid != MyProcPid)
		kill(pid, SIGUSR2);

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
		HASH_SEQ_STATUS seq_status;
		hash_seq_init(&seq_status, ShardCache);
		Shard_ref_t *ref;
		while ((ref = hash_seq_search(&seq_status)) != NULL)
		{
			reqs[nreqs++] = ref->id;
		}
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

static Shard_node_t *find_node_by_ip_port(Shard_t *ps, const char *ip, uint16_t port)
{
	Shard_node_ref_t *pnoderef = ps->shard_nodes;
	for (int i = 0; i < MAX_NODES_PER_SHARD; i++)
	{
		Shard_node_t *sn = pnoderef[i].ptr;
		if (!sn) continue;
		if (strcmp(sn->hostaddr, ip) == 0 && port == sn->port)
			return sn;
	}
	return NULL;
}


void RequestShardingTopoCheckAllStorageShards()
{
	Storage_HA_Mode ha_mode = storage_ha_mode();
	if (ha_mode == HA_NO_REP) return;
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
	heap_close(pg_shard_rel, NoLock);
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
	uint32_t metashard_connid = 0;
	uint32_t pos = sizeof(ShardConnKillReqQ);
	int num_shard_reqs = 0, num_meta_reqs = 0;
	Oid cur_shardid = 0;
	MetaShardKillConnReqSection metareqs;
	memset(&metareqs, 0, sizeof(metareqs));

	ResetCommunicationHub();
	LWLockAcquire(KillShardConnReqLock, LW_EXCLUSIVE);
	PG_TRY();
	{
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
	}
	PG_CATCH(); // the GetAsyncStmtInfoNode() could fail of mysql connect error.
	{
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

	if (num_shard_reqs == 0) goto do_meta;

	PG_TRY();
	{
	/*
	  Connection could break while sending the stmts or receiving results,
	  and this is perfectly OK, topo check and kill-conn reqs are all enqueued
	*/
	send_multi_stmts_to_multi();
	}
	PG_CATCH();
	{
		PG_RE_THROW();
	}
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

Oid GetShardMasterNodeId(Oid shardid)
{
	Shard_t shard;
	Oid ret = InvalidOid;
	if (FindCachedShardInternal(shardid, false, &shard, NULL))
		ret = shard.master_node_id;
	return ret;
}

/*
  Return all shards' IDs in a list.
*/
List *GetAllShardIds()
{
	HASH_SEQ_STATUS seqstat;
	Shard_ref_t *ptr;
	List *l = NULL;

	LoadAllShards(false);
	hash_seq_init(&seqstat, ShardCache);
	while ((ptr = hash_seq_search(&seqstat)) != NULL)
	{
		l = lappend_oid(l, ptr->id);
	}
	return l;
}

static void free_shard_nodes(Shard_node_t *snodes)
{
	for (int i = 0; i < MAX_NODES_PER_SHARD; i++)
	{
		void *p = snodes[i].hostaddr;
		if (p) pfree(p);
		p = snodes[i].passwd;
		if (p) pfree(p);
	}
	pfree(snodes);
}


static bool got_sigterm = false;
static void
topo_service_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
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
	ShardCacheInit();
	InitShardingSession();

	/* Set the topo service pid */
	(void) get_topo_service_pid();
	Assert (ptopo_service_pid);
	*ptopo_service_pid = MyProcPid;

	bool got_sighup = true;
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
			reapShardConnKillReqs();

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

