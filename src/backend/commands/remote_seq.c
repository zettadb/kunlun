/*-------------------------------------------------------------------------
 *
 * remote_seq.c
 *	  Kunlun remote sequences implementation.
 *
 *
 * Copyright (c) 2019 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * IDENTIFICATION
 *	  src/backend/commands/remote_seq.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/remote_meta.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_sequence.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/remote_seq.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "utils/varlena.h"
#include <unistd.h>

const static int64_t InvalidSeqVal = -9223372036854775808L;

static int appendRemoteSeqFetch(Oid seq_relid, int cache_times);

typedef struct SharedSeqEntKey
{
	Oid dbid, seqrelid;
}SharedSeqEntKey;

typedef struct SharedSeqEnt
{
	SharedSeqEntKey key;
	bool fetching;
	bool consumed;
	/*
	  whether currval was already returned to user.
	  if false, return currval when nextval() is called the 1st time.
	*/
	bool currval_used;
	/*
	  Next time new range is reserved, this field will be set to
	  this->currval_used. this is for setval() to work correctly.
	*/
	bool is_called;
	int cache_times; // suppose this many connections are using this seq
	// when fetched from storage shard last time? can't be too often, we will
	// increase cache_times adaptively according to this ts.
	time_t last_fetch_ts;
	int64_t increment;

	/*
	  increment from currval to return next seq value
	*/
	int64_t currval;
	/*
	  The last seq value fetched for this computing node from storage shard
	  and persisted there.
	*/
	int64_t lastval;
}SharedSeqEnt;

static HTAB *shared_seq_cache = NULL;

typedef struct SeqFetchReq
{
	Oid seqrelid;
	// NO. of seq values to fetch from target storage shard.
	// when result comes, the fetched new last value is stored here.
	int64_t cache;
}SeqFetchReq;

// both must be < 65536
#define MAXSEQWAITERS 512
#define MAXSEQREQS 256

typedef struct SeqFetchReqs
{
	Oid dbid;
	uint16_t num_reqs, num_waiters, num_fetching_waiters;
	int proc_id; // pid of the cluster log applier process for this db
	SeqFetchReq reqs[MAXSEQREQS];
	PGPROC *waiters[MAXSEQREQS]; //reqs[i] was enqueued by waiters[i]
	/*
	  Waiters enqueued here when they find we are already fething values for
	  its seq from storage shards.
	*/
	PGPROC *fetching_waiters[MAXSEQWAITERS];
}SeqFetchReqs;

// per db SeqFetchReqs
static SeqFetchReqs *g_seq_fetch_reqs = NULL;
// array base
static SeqFetchReqs *g_seq_fetch_reqs_base = NULL;

static int reapRemoteSeqReqs(SeqFetchReqs *dest);
static void informRemoteSeqWaiters(SeqFetchReqs*dest);
static void RemoteSeqFetch(SeqFetchReqs*dest);
static void updateRemoteSeqCache(SeqFetchReqs *dest);
static void InitSharedSeqCache(void);
static SharedSeqEnt * add_seq_to_shared_cache(Oid seqrelid,
	bool is_called, bool *pfound);

#define MAX_DBS 256 
Size RemoteSeqFetchShmemSize()
{
	return sizeof(SeqFetchReqs) * MAX_DBS;
}

void CreateRemoteSeqFetchShmem()
{
	bool found = false;
	Size size = RemoteSeqFetchShmemSize();
	g_seq_fetch_reqs_base = (SeqFetchReqs *)ShmemInitStruct("Remote Sequence value group fetch", size, &found);
	if (!found)
	{
		/*
		 * We're the first - initialize.
		 */
		MemSet(g_seq_fetch_reqs_base, 0, size);
	}

	InitSharedSeqCache();
}

static void Init_seq_fetch_reqs()
{
	if (g_seq_fetch_reqs) return;

	SeqFetchReqs *sfr = NULL;

	for (int i = 0; i < MAX_DBS; i++)
	{
		sfr = g_seq_fetch_reqs_base + i;
		if (sfr->dbid == InvalidOid)
		{
			g_seq_fetch_reqs = sfr;
			g_seq_fetch_reqs->dbid = MyDatabaseId;
			break;
		}
		else if (sfr->dbid == MyDatabaseId)
		{
			g_seq_fetch_reqs = sfr;
			break;
		}
	}
	if (g_seq_fetch_reqs == NULL)
		ereport(ERROR,
			(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
			errmsg("Kunlun-db: Too many databases for sequence processing, at most %d allowed.", MAX_DBS)));
}

static int
seq_req_compare(const void *key1, const void *key2, Size keysize)
{
	Assert(keysize == sizeof(SharedSeqEntKey));
	SharedSeqEntKey*r1 = (SharedSeqEntKey*)key1;
	SharedSeqEntKey*r2 = (SharedSeqEntKey*)key2;
	if (r1->dbid != r2->dbid)
		return r1->dbid - r2->dbid;
	else
		return r1->seqrelid - r2->seqrelid;
}

static void InitSharedSeqCache()
{
	HASHCTL info;
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(SharedSeqEntKey);
	info.entrysize = sizeof(SharedSeqEnt);
	info.num_partitions = 64;
	info.match = seq_req_compare;
	info.hash = tag_hash;
	shared_seq_cache = ShmemInitHash("shared sequence cache", 1024, 4096, &info,
		HASH_PARTITION | HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);
}

/*

*/
static SharedSeqEnt *
add_seq_to_shared_cache(Oid seqrelid, bool is_called, bool *pfound)
{
	Assert(pfound && seqrelid != InvalidOid);
	SharedSeqEntKey key;
	key.dbid = MyDatabaseId;
	key.seqrelid = seqrelid;

	SharedSeqEnt*sse = (SharedSeqEnt*)hash_search(shared_seq_cache,
		&key, HASH_ENTER, pfound);
	if (*pfound)
		return sse;

	/*
	  the tuple and its page will be pinned a long time, this is OK. but the
	  lwlock can't be hold so long, it's released before every IO/wait/sleep.
	*/
	HeapTuple pgstuple = SearchSysCache1(SEQRELID, ObjectIdGetDatum(seqrelid));
	if (!HeapTupleIsValid(pgstuple))
		elog(ERROR, "cache lookup failed for sequence %u", seqrelid);
	Form_pg_sequence seq = (Form_pg_sequence) GETSTRUCT(pgstuple);
	ReleaseSysCache(pgstuple);

	sse->key = key;
	sse->fetching = false;
	sse->currval = seq->last_fetched;
	sse->lastval = seq->last_fetched;
	sse->currval_used = true;
	sse->is_called = is_called;

	/*
	  When a seq is created, 'last_fetched' is InvalidSeqVal, but that
	  doesn't mean it's consumed. so always try fetch once whenever the
	  seq is loaded to shared cache.
	*/
	sse->consumed = false;
	sse->cache_times = 10; // suppose 10 connections are using this seq
	sse->last_fetch_ts = time(0);
	sse->increment = seq->seqincrement;
	return sse;
}

int64_t fetch_next_val(Relation seqrel)
{
	int64_t val = 0;
	bool found = false;
	Oid seqrelid = seqrel->rd_id;
again:
	// the lock is released in appendRemoteSeqFetch().
	LWLockAcquire(RemoteSeqFetchLock, LW_EXCLUSIVE);
	SharedSeqEnt*sse = add_seq_to_shared_cache(seqrelid, false, &found);
	time_t now;

	if (!found)
	{
		appendRemoteSeqFetch(seqrelid, sse->cache_times);
		goto again;
	}

	if (unlikely(sse->consumed))
	{
		LWLockRelease(RemoteSeqFetchLock);
		ereport(ERROR,
			(errcode(ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED),
			errmsg("Kunlun-db: nextval(): consumed all values of sequence \"%s\" ",
					seqrel->rd_rel->relname.data)));
	}

	Assert(sse->lastval != InvalidSeqVal && sse->currval != InvalidSeqVal);
	if (unlikely(!sse->currval_used))
	{
		sse->currval_used = true;
		LWLockRelease(RemoteSeqFetchLock);
		return sse->currval;
	}
	else if (likely((sse->increment > 0 && sse->currval + sse->increment <= sse->lastval) ||
		(sse->increment < 0 && sse->currval + sse->increment >= sse->lastval)))
	{
		val = (sse->currval += sse->increment);
		LWLockRelease(RemoteSeqFetchLock);
		return val;
	}
	else
	{
		now = time(0);
		/*
		  If fetched values are consumed within 5 seconds, fetch more
		  in a batch.
		*/
		if (now - sse->last_fetch_ts < 5 && sse->cache_times < 100000)
		{
			sse->cache_times *= 2;
			sse->last_fetch_ts = now;
		}
		if (sse->cache_times == 0)
			sse->cache_times = 10;

		appendRemoteSeqFetch(seqrelid, sse->cache_times);
		goto again;
	}

	Assert(false);
	return 0;
}

inline static void inform_seq_hdlr_proc(int pid)
{
	Assert(pid > 0);
	kill(pid, SIGUSR2);
}

/*
  enqueue req and wait for notification. bg proc will pickup such reqs and
  fetch values from storage shards. can't do remote fetch in user txns
  otherwise if user txn aborts, fetched seq values would be invalid but they
  may have been used in other txns.
  @retval 0 if successful; 1 if wait timed out
*/
static int appendRemoteSeqFetch(Oid seqrelid, int cache_times)
{
	Assert(cache_times != 0);

	bool found = false;
	SharedSeqEntKey key;
	key.dbid = MyDatabaseId;
	key.seqrelid = seqrelid;
	Init_seq_fetch_reqs();
	HeapTuple pgstuple = SearchSysCache1(SEQRELID, ObjectIdGetDatum(seqrelid));
	if (!HeapTupleIsValid(pgstuple))
		elog(ERROR, "cache lookup failed for sequence %u", seqrelid);
	Form_pg_sequence seq = (Form_pg_sequence) GETSTRUCT(pgstuple);
retry:
	CHECK_FOR_INTERRUPTS();
	// LWLockAcquire(RemoteSeqFetchLock, LW_EXCLUSIVE); acquired in caller fetch_next_val().
	// the hash entry ptr may have changed while RemoteSeqFetchLock was released.
	SharedSeqEnt*sse = (SharedSeqEnt*)hash_search(shared_seq_cache, &key, HASH_FIND, &found);
	Assert(found);
	Assert(seq->seqincrement == sse->increment);

	if (!sse->fetching)
	{
		if (!(sse->currval == InvalidSeqVal && sse->lastval == InvalidSeqVal) &&
			((seq->seqincrement > 0 && sse->currval <= sse->lastval - seq->seqincrement) ||
			 (seq->seqincrement < 0 && sse->currval >= sse->lastval - seq->seqincrement)))
		{
			/*
			  Another session has requested earlier and the bg proc has
			  fetched more values for the seq.
			  use substraction to avoid overflow/underflow.
			*/
			LWLockRelease(RemoteSeqFetchLock);
			ReleaseSysCache(pgstuple);
			return 0;
		}

		if (g_seq_fetch_reqs->num_reqs >= MAXSEQREQS ||
			g_seq_fetch_reqs->num_waiters >= MAXSEQWAITERS)
		{
			LWLockRelease(RemoteSeqFetchLock);
			elog(WARNING, "SeqFetchReqs req queue full(%d/%d, %d/%d), waiting for a slot.",
				 g_seq_fetch_reqs->num_reqs, MAXSEQREQS, g_seq_fetch_reqs->num_waiters, MAXSEQWAITERS);
			/*
			  Statement timeout mechanism will make sure control can return
			  to client instead of permanently loop&wait, also the case for
			  the wait&loop in else branch.
			*/
			usleep(10000);
			LWLockAcquire(RemoteSeqFetchLock, LW_EXCLUSIVE); // reacquire
			goto retry;
		}

		SeqFetchReq *sfr = g_seq_fetch_reqs->reqs + g_seq_fetch_reqs->num_reqs++;
		sfr->seqrelid = seqrelid;
		sfr->cache = cache_times;
		sse->fetching = true;
		g_seq_fetch_reqs->waiters[g_seq_fetch_reqs->num_waiters++] = MyProc;
	}
	else
	{
		if (g_seq_fetch_reqs->num_fetching_waiters >= MAXSEQWAITERS)
		{
			LWLockRelease(RemoteSeqFetchLock);
			elog(WARNING, "SeqFetchReqs req queue full(%d/%d), waiting for a slot.",
				 g_seq_fetch_reqs->num_fetching_waiters, MAXSEQWAITERS);
			usleep(10000);
			LWLockAcquire(RemoteSeqFetchLock, LW_EXCLUSIVE); // reacquire
			goto retry;
		}

		g_seq_fetch_reqs->fetching_waiters[g_seq_fetch_reqs->num_fetching_waiters++] = MyProc;
	}

	int proc_id = g_seq_fetch_reqs->proc_id;
	LWLockRelease(RemoteSeqFetchLock);
	ReleaseSysCache(pgstuple);

	if (proc_id) inform_seq_hdlr_proc(proc_id);

	if (MyProc->last_sem_wait_timedout)
	{
		PGSemaphoreReset(MyProc->sem);
		MyProc->last_sem_wait_timedout = false;
	}
	int ret = PGSemaphoreTimedLock(MyProc->sem, StatementTimeout);
	if (ret == 1)
	{
		MyProc->last_sem_wait_timedout = true;
		RequestShardingTopoCheckAllStorageShards();
		ShardConnKillReq *req = makeShardConnKillReq(1/*kill conn*/);
		if (req)
		{
			appendShardConnKillReq(req);
			pfree(req);
		}
	}
	else
		Assert(ret == 0);
	return ret;
}

static int reapRemoteSeqReqs(SeqFetchReqs *dest)
{
	/*
	  We could later use one queue &lwlock for each db, but that seems overkill
	  for now.
	*/
	LWLockAcquire(RemoteSeqFetchLock, LW_EXCLUSIVE);

	Init_seq_fetch_reqs();
	if (g_seq_fetch_reqs->proc_id == 0)
		g_seq_fetch_reqs->proc_id = getpid();

	memcpy(dest->reqs, g_seq_fetch_reqs->reqs, g_seq_fetch_reqs->num_reqs*sizeof(SeqFetchReq));
	memcpy(dest->waiters, g_seq_fetch_reqs->waiters, g_seq_fetch_reqs->num_waiters*sizeof(void*));
	dest->num_reqs = g_seq_fetch_reqs->num_reqs;
	dest->num_waiters = g_seq_fetch_reqs->num_waiters;
	g_seq_fetch_reqs->num_reqs = 0;
	g_seq_fetch_reqs->num_waiters = 0;

	LWLockRelease(RemoteSeqFetchLock);
	return dest->num_reqs;
}


/*
  Update all requested seqs' seq->last_fetched in a txn before releasing
  waiters so that all used seq values are persisted.
*/
static void updateRemoteSeqCache(SeqFetchReqs *dest)
{
	Relation seqrel = heap_open(SequenceRelationId, RowExclusiveLock);

	uint64_t ntups = 0;
	HeapTuple tup = NULL;
	SysScanDesc scan;
	Datum values[9] = {0, 0, 0, 0, 0, 0, 0, 0, 0};
	bool nulls[9] = {false, false, false, false, false, false, false, false, false};
	bool replaces[9] = {false, false, false, false, false, false, false, false, false};
	ScanKeyData key;
	Form_pg_sequence seq;
	SeqFetchReq *dest_reqi = NULL;

	for (int i = 0; i < dest->num_reqs; i++)
	{
		dest_reqi = dest->reqs + i;

		ScanKeyInit(&key,
					Anum_pg_sequence_seqrelid,
	                BTEqualStrategyNumber,
					F_OIDEQ, dest_reqi->seqrelid);
	    scan = systable_beginscan(seqrel, SequenceRelidIndexId, true, NULL, 1, &key);
	    while ((tup = systable_getnext(scan)) != NULL)
	    {
	        seq = ((Form_pg_sequence) GETSTRUCT(tup));
	        ntups++;
	        break;
	    }
	
		if (ntups == 0)
	    {
	        ereport(ERROR, 
	                (errcode(ERRCODE_INTERNAL_ERROR),
	                 errmsg("Kunlun-db: Failed to find valid sequence with id(%u) in pg_sequence", dest_reqi->seqrelid)));
	    }
	
		replaces[Anum_pg_sequence_last_fetched - 1] = true;
		values[Anum_pg_sequence_last_fetched - 1] = Int8GetDatum(dest_reqi->cache);
		HeapTuple newtuple =
	        heap_modify_tuple(tup, RelationGetDescr(seqrel),
		                      values, nulls, replaces);
	    CatalogTupleUpdate(seqrel, &newtuple->t_self, newtuple);
		systable_endscan(scan);

	}

	heap_close(seqrel, RowExclusiveLock);
}

static void informRemoteSeqWaiters(SeqFetchReqs*dest)
{
	for (int i = 0; i < dest->num_waiters; i++)
		PGSemaphoreUnlock(dest->waiters[i]->sem);
	
	LWLockAcquire(RemoteSeqFetchLock, LW_EXCLUSIVE);
	Init_seq_fetch_reqs();
	for (int i = 0; i < g_seq_fetch_reqs->num_fetching_waiters; i++)
		PGSemaphoreUnlock(g_seq_fetch_reqs->fetching_waiters[i]->sem);
	g_seq_fetch_reqs->num_fetching_waiters = 0;
	LWLockRelease(RemoteSeqFetchLock);
}

inline static SeqFetchReq*
find_seq_fetch_req_by_id(SeqFetchReqs*dest, Oid seqrelid)
{
	for (int i = 0; i < dest->num_reqs; i++)
	{
		SeqFetchReq *sfr = dest->reqs + i;
		if (sfr->seqrelid == seqrelid)
			return sfr;
	}

	return NULL;
}

extern bool use_mysql_native_seq;
static void RemoteSeqFetch(SeqFetchReqs*dest)
{
	static NameData dbname, schmname;
	if (dbname.data[0] == '\0')
		get_database_name3(MyDatabaseId, &dbname);

	Relation rel;
	SeqFetchReq *dest_reqi = NULL;
	int sqlbuflen = 384;
	char *sqlbuf = NULL;

	for (int i = 0; i < dest->num_reqs; i++)
	{
		dest_reqi = dest->reqs + i;
		rel = relation_open(dest_reqi->seqrelid, RowExclusiveLock);
		get_namespace_name3(rel->rd_rel->relnamespace, &schmname);
		sqlbuf = palloc(sqlbuflen);
		Assert(dest_reqi->cache != 0);

		int slen = snprintf(sqlbuf, sqlbuflen,
				 "%c set @newstart = 0, @newval=0, @retcode=0, @seqrelid = %u; call mysql.seq_reserve_vals('%s_%s_%s', '%s', %ld, @newstart, @newval, @retcode); select @newstart, @newval, @retcode, @seqrelid",
				 i == 0 ? ' ' : ';', dest_reqi->seqrelid, dbname.data,
				 /* mysql escapes the $$ to @0024@0024 if it's created via CREATE SEQUENCE stmt. */
				 use_mysql_native_seq ? "@0024@0024" : "$$",
				 schmname.data,
				 rel->rd_rel->relname.data, dest_reqi->cache);
		Assert(slen < sqlbuflen);

		AsyncStmtInfo *asi = GetAsyncStmtInfo(rel->rd_rel->relshardid);
		/*
		  dzw: we are calling a proc which updates tables, but we here have to
		  take the query as a SELECT stmt in order to fetch its result rows.
		*/
		append_async_stmt(asi, sqlbuf, slen, CMD_SELECT, false, SQLCOM_SELECT);
		relation_close(rel, NoLock);
	}

	PG_TRY();
	send_multi_stmts_to_multi();
	PG_CATCH();
	/*
	  set fetching to off for all seqs.
	*/
	HASH_SEQ_STATUS seq_status;
	hash_seq_init(&seq_status, shared_seq_cache);
	SharedSeqEnt*sse = NULL;
	while ((sse = (SharedSeqEnt*)hash_seq_search(&seq_status)) != NULL)
	{
		if (sse->key.dbid == MyDatabaseId) sse->fetching = false;
	}

	PG_RE_THROW();
	PG_END_TRY();

	size_t num_asis = GetAsyncStmtInfoUsed();
	char *endptr = NULL;

	for (size_t i = 0; i < num_asis; i++) 
	{    
		CHECK_FOR_INTERRUPTS();
	 
		AsyncStmtInfo *asi = GetAsyncStmtInfoByIndex(i);

		if (!ASIConnected(asi)) continue;

		MYSQL_RES *mres = asi->mysql_res;

		if (mres == NULL)
		{
			free_mysql_result(asi);
			continue;
		}

		/*
		  Update seq state in shared cache.
		*/
		LWLockAcquire(RemoteSeqFetchLock, LW_EXCLUSIVE);
		do {
			MYSQL_ROW row = mysql_fetch_row(mres);
			if (row == NULL)
			{
				check_mysql_fetch_row_status(asi);
				free_mysql_result(asi);
				break;
			}

			Assert(row[2] && row[3]);
			Oid seqrelid = strtoul(row[3], &endptr, 10);
			int retcode = strtol(row[2], &endptr, 10);

			SeqFetchReq *req = find_seq_fetch_req_by_id(dest, seqrelid);
			Assert(req);
			bool found;
			SharedSeqEntKey  sse_key;
			sse_key.dbid = MyDatabaseId;
			sse_key.seqrelid = req->seqrelid;
			SharedSeqEnt*sse = (SharedSeqEnt*)hash_search(shared_seq_cache,
				&sse_key, HASH_FIND, &found);
			Assert(found);

			// seq values consumed, both must be NULL.
			Assert((row[0] && row[1] && retcode > 0) || (!row[0] && !row[1] && retcode < 0));
			if (retcode == -1)
				elog(WARNING, "Sequence(%u,%u) not found in storage shard node (%u, %u)",
					 MyDatabaseId, seqrelid, asi->shard_id, asi->node_id);
			else if (retcode == -2)
				elog(WARNING, "Sequence(%u,%u) all values consumed.", MyDatabaseId, seqrelid);
			else
				Assert(retcode > 0); // NO. of seq values fetched

			if (row[0])
			{
				sse->currval = strtoll(row[0], &endptr, 10);// new start point
			}
			else
				sse->currval = InvalidSeqVal;// seq all values consumed

			if (row[1])
				req->cache = strtoll(row[1], &endptr, 10);
			else
				req->cache = InvalidSeqVal;// seq all values consumed
			sse->currval_used = sse->is_called;
			sse->fetching = false;
			sse->lastval = dest_reqi->cache;
			sse->consumed = (sse->lastval == InvalidSeqVal);
		} while (true);
		LWLockRelease(RemoteSeqFetchLock);
	}
}

void fetchSeqValues()
{
	/*
	  At most fetch 10 times but quit early if a round returns no reqs. This is
	  a balance so that other jobs won't starve and service seqs asap for best
	  performance.
	*/
	int max = 10, i, cnt;
	static SeqFetchReqs reqq;
	Assert(!IsTransactionState());

	for (i = 0; i < max; i++)
	{
		cnt = reapRemoteSeqReqs(&reqq);
		if (cnt == 0)
			break;

		PG_TRY();
		StartTransactionCommand();
		SPI_connect();
		RemoteSeqFetch(&reqq);
		updateRemoteSeqCache(&reqq);
		SPI_finish();

		PG_TRY();
		CommitTransactionCommand();
		PG_CATCH();
		LWLockReleaseAll();
		/*
		  clear requested&cached seqs from dynahash.
		  This IS quite necessary because this dynahash lives after this
		  process exit because of this error and be restarted.
		*/
		SharedSeqEntKey key;
		bool found;

		LWLockAcquire(RemoteSeqFetchLock, LW_EXCLUSIVE);
		for (int i = 0; i < reqq.num_reqs; i++)
		{
			SeqFetchReq *sfr = reqq.reqs + i;
			key.dbid = MyDatabaseId;
			key.seqrelid = sfr->seqrelid;
			hash_search(shared_seq_cache, &key, HASH_REMOVE, &found);
		}
		LWLockRelease(RemoteSeqFetchLock);
		PG_RE_THROW();
		PG_END_TRY();

		informRemoteSeqWaiters(&reqq);
		PG_CATCH();
		LWLockReleaseAll();
		informRemoteSeqWaiters(&reqq);
		PG_RE_THROW();
		PG_END_TRY();
	}

	if (i == max)
		elog(WARNING, "Some sequences need to cache more values for better performance.");
}

void InvalidateCachedSeq(Oid seqrelid)
{
	bool found = false;
	SharedSeqEntKey key;
	key.dbid = MyDatabaseId;
	key.seqrelid = seqrelid;

	LWLockAcquire(RemoteSeqFetchLock, LW_EXCLUSIVE);
	hash_search(shared_seq_cache, &key, HASH_REMOVE, &found);
	LWLockRelease(RemoteSeqFetchLock);
	elog(DEBUG3, "Invalidated sequence %u", seqrelid);
}

void do_remote_setval(Relation seqrel, int64 next, bool is_called)
{
	Oid seqrelid = seqrel->rd_id;
	RemoteAlterSeq raseq;
	init_remote_alter_seq(&raseq);
	raseq.do_restart = true;
	raseq.restart_val = next;
	appendStringInfo(&raseq.update_stmt, "%c curval = %ld",
			raseq.update_stmt.len > 0 ? ',':' ', next);
	appendStringInfo(&raseq.update_stmt_peer, " restart %ld", next);

	TrackAlterSeq(seqrel, NULL, &raseq, false, true);

	bool found;
	LWLockAcquire(RemoteSeqFetchLock, LW_EXCLUSIVE);
	/*
	  dzw:
	  if another session bump this seq first, it will always use 'next',
	  and this could cause reuse of 'next' if is_called is true.
	  In kunlun-percona's mysql.sequences table we always assume the 'currval'
	  is already used, and that's what the 'next' value in
	  setval('next') and 'RESTART next' is assigned to. so we don't skip another
	  number here otherwise next+2 is returned as 1st value after the setval() call.
	*/
	SharedSeqEnt*sse = add_seq_to_shared_cache(seqrelid, false/*is_called*/, &found);
	if (!found)
	{
		appendRemoteSeqFetch(seqrelid, sse->cache_times);
	}
	else
		LWLockRelease(RemoteSeqFetchLock);
}
