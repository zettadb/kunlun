/*-------------------------------------------------------------------------
 *
 * sequence_service.c
 *	  Kunlun remote sequences implementation.
 *
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
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
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "postmaster/xidsender.h"
#include "storage/proc.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "utils/varlena.h"
#include <unistd.h>

#include "sequence_service.h"

const static int64_t InvalidSeqVal = -9223372036854775808L;
#define MAXSEQREQS 256

typedef
struct SharedSeqEntKey
{
	Oid dbid;
	Oid seqrelid;
}SharedSeqEntKey;

typedef
struct SharedSeqEnt
{
	SharedSeqEntKey key;
	char db[NAMEDATALEN];
	char schema[NAMEDATALEN];
	char name[NAMEDATALEN];

	bool need_reload;

	/* True if the sequence is exhausted */
	bool exhausted;

	/*
	 * Whether currval was already returned to user.
	 * if false, return currval when nextval() is called the 1st time.
	 */
	bool currval_used;

	/**
	 * Next time new range is reserved, this field will be set to
	 * this->currval_used. this is for setval() to work correctly.
	 */
	bool is_called;

	/* The number of seq vals fetched from storage shard at one time */
	int cache_times;

	// Suppose there are many connections using this seq
	// when fetched from storage shard last time? can't be too often, we will
	// increase cache_times adaptively according to this ts.
	time_t last_fetch_ts;

	/* The increment step */
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

	/* Errcode/message report by background sequence fetch processor */
	int errcode;
	char message[128];
} SharedSeqEnt;

static HTAB *shared_seq_cache = NULL;

typedef
struct SeqFetchReq
{
	SharedSeqEntKey key;
	// NO. of seq values to fetch from target storage shard.
	// when result comes, the fetched new last value is stored here.
	int64_t cache;

	Oid shardid;
	
	PGPROC *waiter;
}SeqFetchReq;

typedef
struct SeqFetchReqQueue
{
	pid_t proc_id;
	SeqFetchReq queue[MAXSEQREQS];
	Size free;
}SeqFetchReqQueue;

static SeqFetchReqQueue *seq_fetch_queue = NULL;
static LWLock *seq_fetch_queue_lock = NULL;

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

void create_remote_sequence_shmmem()
{
	bool found = false;
	seq_fetch_queue = (SeqFetchReqQueue *)ShmemInitStruct(
		"Remote Sequence request queue",
		sizeof(SeqFetchReqQueue),
		&found);

	if (!found)
	{
		seq_fetch_queue->proc_id = 0;
		seq_fetch_queue->free = 0;
		seq_fetch_queue_lock = &GetNamedLWLockTranche("Remote Sequence request queue lock")->lock;

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
}

static SharedSeqEnt *
add_seq_shared_cache(Relation seqrel, bool is_called, bool *pfound)
{
	Assert(LWLockHeldByMe(seq_fetch_queue_lock));

	Oid seqrelid = seqrel->rd_id;
	Assert(pfound && seqrelid != InvalidOid);
	SharedSeqEntKey key;
	key.dbid = MyDatabaseId;
	key.seqrelid = seqrelid;

	SharedSeqEnt *sse = (SharedSeqEnt*)hash_search(shared_seq_cache, &key, HASH_ENTER, pfound);
	if (*pfound && !sse->need_reload)
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
	sse->need_reload = false;
	sse->currval = seq->last_fetched;
	sse->lastval = seq->last_fetched;
	sse->currval_used = true;
	sse->is_called = is_called;

	/*
	  When a seq is created, 'last_fetched' is InvalidSeqVal, but that
	  doesn't mean it's exhausted. so always try fetch once whenever the
	  seq is loaded to shared cache.
	*/
	sse->exhausted = false;
	sse->cache_times = 100;
	sse->last_fetch_ts = time(0);
	sse->increment = seq->seqincrement;
	strncpy(sse->db, get_database_name(MyDatabaseId), NAMEDATALEN);
	strncpy(sse->schema, get_namespace_name(seqrel->rd_rel->relnamespace), NAMEDATALEN);
	strncpy(sse->name, seqrel->rd_rel->relname.data, NAMEDATALEN);
	sse->errcode = 0;
	return sse;
}

void invalidate_seq_shared_cache(Oid dbid, Oid seqrelid, bool remove)
{
	bool found = false;
	SharedSeqEntKey key;
	List *list = NIL;
	ListCell *lc;

	LWLockAcquire(seq_fetch_queue_lock, LW_EXCLUSIVE);
	if (seqrelid == InvalidOid)
	{
		HASH_SEQ_STATUS seq_status;
		hash_seq_init(&seq_status, shared_seq_cache);
		SharedSeqEnt *sse = NULL;
		while ((sse = (SharedSeqEnt *)hash_seq_search(&seq_status)) != NULL)
		{
			if (sse->key.dbid == dbid)
				list = lappend_oid(list, sse->key.seqrelid);
		}
	}
	else
	{
		list = lappend_oid(list, seqrelid);
	}

	foreach(lc, list)
	{
		key.dbid = dbid;
		key.seqrelid = lfirst_oid(lc);
		if (remove)
		{
			hash_search(shared_seq_cache, &key, HASH_REMOVE, &found);
		}
		else
		{
			SharedSeqEnt *sse = hash_search(shared_seq_cache, &key, HASH_FIND, &found);
			if (sse)
				sse->need_reload = true;
		}
	}
	LWLockRelease(seq_fetch_queue_lock);
	
	list_free(list);

	elog(DEBUG3, "Invalidated sequence %u.%u", dbid, seqrelid);
}

/*
  enqueue req and wait for notification. bg proc will pickup such reqs and
  fetch values from storage shards. can't do remote fetch in user txns
  otherwise if user txn aborts, fetched seq values would be invalid but they
  may have been used in other txns.
  @retval 0 if successful; 1 if wait timed out
*/
static int append_seq_req(Oid seqrelid, int cache_times, Oid shardid)
{
	Assert(cache_times != 0);
	bool found = false;
	SharedSeqEntKey key;
	key.dbid = MyDatabaseId;
	key.seqrelid = seqrelid;

	HeapTuple pgstuple = SearchSysCache1(SEQRELID, ObjectIdGetDatum(seqrelid));
	if (!HeapTupleIsValid(pgstuple))
		elog(ERROR, "cache lookup failed for sequence %u", seqrelid);

	Form_pg_sequence seq = (Form_pg_sequence)GETSTRUCT(pgstuple);

	LWLockAcquire(seq_fetch_queue_lock, LW_EXCLUSIVE);
	SharedSeqEnt *sse = (SharedSeqEnt *)hash_search(shared_seq_cache, &key, HASH_FIND, &found);
	Assert(found);
	sse->errcode = 0;
	if (seq->seqincrement != sse->increment)
	{
		sse->need_reload = true;
		LWLockRelease(seq_fetch_queue_lock);
		return 0;
	}

	/* get the request slot */
	do
	{
		if (!(sse->currval == InvalidSeqVal && sse->lastval == InvalidSeqVal) &&
			((seq->seqincrement > 0 && sse->currval <= sse->lastval - seq->seqincrement) ||
			 (seq->seqincrement < 0 && sse->currval >= sse->lastval - seq->seqincrement)))
		{
			LWLockRelease(seq_fetch_queue_lock);
			ReleaseSysCache(pgstuple);
			return 0;
		}

		if (seq_fetch_queue->free >= MAXSEQREQS)
		{
			LWLockRelease(seq_fetch_queue_lock);
			elog(WARNING, "SeqFetchReqs req queue full, waiting for a slot.");
			/*
			  Statement timeout mechanism will make sure control can return
			  to client instead of permanently loop&wait, also the case for
			  the wait&loop in else branch.
			*/
			usleep(10000);
			CHECK_FOR_INTERRUPTS();
			LWLockAcquire(seq_fetch_queue_lock, LW_EXCLUSIVE);
			continue;
		}
	} while (0);

	SeqFetchReq *req = &seq_fetch_queue->queue[seq_fetch_queue->free++];
	req->key.dbid = MyDatabaseId;
	req->key.seqrelid = seqrelid;
	req->shardid = shardid;
	req->cache = cache_times;
	req->waiter = MyProc;

	int proc_id = seq_fetch_queue->proc_id;
	LWLockRelease(seq_fetch_queue_lock);
	ReleaseSysCache(pgstuple);

	/* notify the bgworker a new request */
	if (proc_id)
		kill(proc_id, SIGUSR2);

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
		ShardConnKillReq *req = makeShardConnKillReq(1 /*kill conn*/);
		if (req)
		{
			appendShardConnKillReq(req);
			pfree(req);
		}
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: Timed out reserving sequence(%u) value range.",
						seqrelid)));
	}
	else
	{
		Assert(ret == 0);
		if (sse->errcode)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Kunlun-db: got error from sequence fetch processor: (%d, %s)",
							sse->errcode, sse->message)));
		}
	}
	return ret;
}

static Form_pg_sequence_data
read_seq_tuple_with_lock(Relation rel, Buffer *buf, HeapTuple seqdatatuple)
{
	Page page;
	ItemId lp;
	Form_pg_sequence_data seq;

	*buf = ReadBuffer(rel, 0);
	LockBuffer(*buf, BUFFER_LOCK_EXCLUSIVE);

	page = BufferGetPage(*buf);
	
	lp = PageGetItemId(page, FirstOffsetNumber);
	Assert(ItemIdIsNormal(lp));

	/* Note we currently only bother to set these two fields of *seqdatatuple */
	seqdatatuple->t_data = (HeapTupleHeader)PageGetItem(page, lp);
	seqdatatuple->t_len = ItemIdGetLength(lp);

	seq = (Form_pg_sequence_data) GETSTRUCT(seqdatatuple);

	return seq;
}

int64_t remote_fetch_nextval(Relation seqrel)
{
	Page page;
	Buffer buf;
	int64_t val = 0;
	bool found = false;
	bool req_new_seq = false;
	Oid seqrelid = seqrel->rd_id;
	Form_pg_sequence_data seq;
	HeapTupleData seqdatatuple;
	SharedSeqEnt *sse;
	bool need_reload = false;

	/* lock and read sequence tuple */
	seq = read_seq_tuple_with_lock(seqrel, &buf, &seqdatatuple);
	page = BufferGetPage(buf);

	do
	{
		LWLockAcquire(seq_fetch_queue_lock, LW_EXCLUSIVE);
		sse = add_seq_shared_cache(seqrel, false, &found);
		sse->errcode = 0;
		LWLockRelease(seq_fetch_queue_lock);

		req_new_seq = false;

		if (!found || need_reload)
		{
			req_new_seq = true;
			sse->lastval = seq->last_value;
			sse->currval = seq->last_value;
			need_reload = false;
		}
		else if (unlikely(sse->exhausted))
		{
			UnlockReleaseBuffer(buf);
			ereport(ERROR,
					(errcode(ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED),
					 errmsg("Kunlun-db: nextval(): exhausted all values of sequence \"%s\" ",
							seqrel->rd_rel->relname.data)));
		}
		else if (unlikely(!sse->currval_used))
		{
			sse->currval_used = true;
			val = sse->currval;
		}
		else if (likely((sse->increment > 0 && sse->currval + sse->increment <= sse->lastval) ||
						(sse->increment < 0 && sse->currval + sse->increment >= sse->lastval)))
		{
			val = (sse->currval += sse->increment);
		}
		else
		{
			time_t now = time(0);
			/*
			  If fetched values are exhausted within 5 seconds, fetch more
			  in a batch.
			*/
			if (now - sse->last_fetch_ts < 5 && sse->cache_times < 100000)
			{
				sse->cache_times *= 2;
			}
			else if (now - sse->last_fetch_ts > 5)
			{
				sse->cache_times /= 2;
			}

			if (sse->cache_times == 0)
				sse->cache_times = 100;

			sse->last_fetch_ts = now;
			req_new_seq = true;
		}

		// successfull get new sequence, unlock the buffer and return
		if (!req_new_seq)
		{
			UnlockReleaseBuffer(buf);
			return val;
		}

		// add new seq qrequest to req queue
		PG_TRY();
		{
			int ret = append_seq_req(seqrelid,
									 sse->cache_times,
									 seqrel->rd_rel->relshardid);
			Assert(ret == 0);

			/* The modified fields by req handler maybe invalid*/
			if (sse->need_reload)
			{
				need_reload = true;
			}
			else
			{
				GetTopTransactionId();
				START_CRIT_SECTION();
				{
					// log the new fetched sequence in log
					MarkBufferDirty(buf);

					xl_seq_rec xlrec;
					XLogRecPtr recptr;

					XLogBeginInsert();
					XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);

					/* set values that will be saved in xlog */
					seq->last_value = sse->lastval;
					seq->is_called = true;

					xlrec.node = seqrel->rd_node;

					XLogRegisterData((char *)&xlrec, sizeof(xl_seq_rec));
					XLogRegisterData((char *)seqdatatuple.t_data, seqdatatuple.t_len);

					recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG);

					PageSetLSN(page, recptr);
				}
				END_CRIT_SECTION();
			}
		}
		PG_CATCH();
		{
			// UnlockReleaseBuffer(buf);
			PG_RE_THROW();
		}
		PG_END_TRY();

	} while (true);

	Assert(false);
	return 0;
}

static
int reap_remote_seq_reqs(SeqFetchReq *dest)
{
	int nreqs = 0;
	LWLockAcquire(seq_fetch_queue_lock, LW_EXCLUSIVE);

	/* update the proc id if changed */
	if (seq_fetch_queue->proc_id != MyProcPid)
	{
		if (seq_fetch_queue->proc_id != 0)
			elog(WARNING, "Seq request reaper change from %d to %d", seq_fetch_queue->proc_id, MyProcPid);

		seq_fetch_queue->proc_id = MyProcPid;
	}

	nreqs = seq_fetch_queue->free;
	memcpy(dest, seq_fetch_queue->queue, nreqs*sizeof(SeqFetchReq));
	seq_fetch_queue->free = 0;
	LWLockRelease(seq_fetch_queue_lock);
	return nreqs;
}

inline static SeqFetchReq*
find_seq_req_by_id(SeqFetchReq *reqs, int num_reqs, SharedSeqEntKey key)
{
	for (int i = 0; i < num_reqs; i++)
	{
		if (seq_req_compare(&reqs[i].key, &key, sizeof(key)) == 0)
			return &reqs[i];
	}

	return NULL;
}

/*
 * [{"newval": 143, "retcode": 11, "newstart": 133, "seqrelid": 1}, ...]
 */
#define STRING_WITH_LEN(X) (X), ((size_t) (sizeof(X) - 1))
static char* parse_seq_json_result(
		char *pos, int64_t *newval, int64_t *newstart, Oid *dbid, Oid *seqrelid, int *retcode)
{
	bool quoted = false;
	char quote, c, p;
	char *key, *value, *endptr;

	if (*pos == '}')
		++pos;
	key = value = NULL;
	p = c = 0;
	while (*pos && p!='}')
	{
		p = c;
		c = *pos;
		/* key is found */
		if (key && !quoted)
		{
			if (!value && (isdigit(c) || c == '-'))
				value = pos;
			else if ((value && !isalnum(c)) ||
					 (!value && strncasecmp(pos, "null", 4) == 0))
			{
				/* value is found */
				if (strncasecmp(key, STRING_WITH_LEN("newval")) == 0)
				{
					*newval = value ? strtoll(value, &endptr, 10) : InvalidSeqVal;
				}
				else if (strncasecmp(key, STRING_WITH_LEN("newstart")) == 0)
				{
					*newstart = value ? strtoll(value, &endptr, 10) : InvalidSeqVal;
				}
				else if (strncasecmp(key, STRING_WITH_LEN("dbid")) == 0)
				{
					*dbid = value ? strtoul(value, &endptr, 10) : InvalidOid;
				}
				else if (strncasecmp(key, STRING_WITH_LEN("seqrelid")) == 0)
				{
					*seqrelid = value ? strtoul(value, &endptr, 10) : InvalidOid;
				}
				else if (strncasecmp(key, STRING_WITH_LEN("retcode")) == 0)
				{
					Assert(value);
					*retcode = strtol(value, &endptr, 10);
				}

				if (!value)
					pos += 4;
				key = value = NULL;
			}
			++pos;
			continue;
		}
		if (c != '\'' && c != '\"')
		{
			++pos;
			continue;
		}
	
		if (!quoted)
		{
			quoted = true;
			quote = c;
			key = pos + 1;
		}
		else if (c == quote)
		{
			quoted = false;
		}
		++pos;
	}

	return pos;
}

extern bool use_mysql_native_seq;
static void handle_seq_reqs(SeqFetchReq *reqs, int num_reqs)
{
	SeqFetchReq *dest_reqi = NULL;
	List *sendshard = NIL;
	List *sendsql = NIL;
	ListCell *lc1, *lc2;
	bool found;

	/* Generate sql to fetch sequence */
	for (int i = 0; i < num_reqs; i++)
	{
		dest_reqi = reqs + i;
		StringInfo str = NULL;
		bool first = false;

		forboth(lc1, sendshard, lc2, sendsql)
		{
			if (lfirst_oid(lc1) == dest_reqi->shardid)
				str = lfirst(lc2);
		}
		if (!str)
		{
			str = palloc(sizeof(StringInfoData));
			initStringInfo(str);
			appendStringInfo(str, "call kunlun_sysdb.reserve_seq(convert('[");
			sendshard = lappend_oid(sendshard, dest_reqi->shardid);
			sendsql = lappend(sendsql, str);
			first = true;
		}

		Assert(dest_reqi->cache != 0);

		LWLockAcquire(seq_fetch_queue_lock, LW_EXCLUSIVE);
		SharedSeqEnt *sse = (SharedSeqEnt *)hash_search(shared_seq_cache,
														&dest_reqi->key, HASH_FIND, &found);
		LWLockRelease(seq_fetch_queue_lock);
		Assert(found);

		appendStringInfo(str, "%c{\"dbid\":%u, \"seqrelid\":%u, \"dbname\": \"%s_%s_%s\", \"seqname\":\"%s\", \"nvals\":%ld}",
						 first ? ' ' : ',',
						 dest_reqi->key.dbid,
						 dest_reqi->key.seqrelid,
						 sse->db,
						 /* mysql escapes the $$ to @0024@0024 if it's created via CREATE SEQUENCE stmt. */
						 use_mysql_native_seq ? "@0024@0024" : "$$",
						 sse->schema,
						 sse->name, dest_reqi->cache);
	}

	/* Send sql to storage nodes */
	forboth(lc1, sendshard, lc2, sendsql)
	{
		Oid shardid = lfirst_oid(lc1);
		StringInfo str = (StringInfo)lfirst(lc2);
		appendStringInfo(str, "]', JSON), @res); SELECT @res;");

		int slen = str->len;
		char *sqlbuf = donateStringInfo(str);
		pfree(str);
		AsyncStmtInfo *asi = GetAsyncStmtInfo(shardid);
		/*
		  dzw: we are calling a proc which updates tables, but we here have to
		  take the query as a SELECT stmt in order to fetch its result rows.
		  So 2PC won't be done at txn commit, but 1PC is done always. And this
		  happens to be no harm because the updated sequences are independent
		  from each other, if a txn branch fails to commit for any reason,
		  it's OK for those seq changes to get lost and seq changes on other
		  shards take effect.
		*/
		append_async_stmt(asi, sqlbuf, slen, CMD_SELECT, false, SQLCOM_SELECT);
	}

	PG_TRY();
	{
		send_multi_stmts_to_multi();
	}
	PG_CATCH();
	{
		/* TODO: send error to the waiter */
		PG_RE_THROW();
	}
	PG_END_TRY();

	size_t num_asis = GetAsyncStmtInfoUsed();

	/* Update the Shared Sequence hash entry based on the fetched sequence */
	Oid dbid, seqrelid;
	int retcode;
	int64_t newval, newstart;
	SeqFetchReq *req;
	
	for (size_t i = 0; i < num_asis; i++)
	{
		CHECK_FOR_INTERRUPTS();

		AsyncStmtInfo *asi = GetAsyncStmtInfoByIndex(i);

		MYSQL_RES *mres = asi->mysql_res;

		if (mres == NULL)
		{
			free_mysql_result(asi);
			/* Notify the waiter ? */
			continue;
		}

		/*
		  Update seq state in shared cache.
		*/
		do
		{
			MYSQL_ROW row = mysql_fetch_row(mres);
			if (row == NULL)
			{
				check_mysql_fetch_row_status(asi);
				free_mysql_result(asi);
				break;
			}

			Assert(row[0]);
			char *json = row[0];
			while (true)
			{
				dbid = InvalidOid;
				seqrelid = InvalidOid;
				newstart = InvalidSeqVal;
				newval = InvalidSeqVal;
				json = parse_seq_json_result(json, &newval, &newstart, &dbid, &seqrelid, &retcode);
				if (dbid == 0 || seqrelid == 0)
					break;
				
				// seq values exhausted, both must be NULL.
				if (retcode == -1)
					elog(WARNING, "Sequence(%u,%u) not found in storage shard node (%u, %u)",
						 dbid, seqrelid, asi->shard_id, asi->node_id);
				else if (retcode == -2)
					elog(WARNING, "Sequence(%u,%u) all values exhausted.", dbid, seqrelid);
				else
					Assert(retcode > 0); // NO. of seq values fetched

				SharedSeqEntKey key;
				key.dbid = dbid;
				key.seqrelid = seqrelid;
				req = find_seq_req_by_id(reqs, num_reqs, key);
				Assert(req);

				LWLockAcquire(seq_fetch_queue_lock, LW_EXCLUSIVE);
				SharedSeqEnt *sse = (SharedSeqEnt *)hash_search(shared_seq_cache,
																&key, HASH_FIND, &found);
				/* The sequence maybe dropped by user*/
				if (sse)
				{
					sse->currval = newstart;
					sse->currval_used = sse->is_called;
					sse->lastval = newval;
					sse->exhausted = (sse->lastval == InvalidSeqVal);
				}

				LWLockRelease(seq_fetch_queue_lock);
			}
		} while (true);
	}
}


/* Signal handle */
static volatile sig_atomic_t got_sigterm = false;
static void
seq_req_handler_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

static void
seq_req_handler_siguser2(SIGNAL_ARGS)
{
	int save_errno = errno;
	SetLatch(MyLatch);
	errno = save_errno;
}


static char rt_error_message[1024];
static emit_log_hook_type saved_emit_log_hook;
static inline void
copy_error_message(ErrorData *edata)
{
	strncpy(rt_error_message, edata->message, sizeof(rt_error_message));
	if (saved_emit_log_hook)
		saved_emit_log_hook(edata);
}

void sequence_serivce_main()
{
	int cnt;
	static SeqFetchReq reqs[MAXSEQREQS];
	MemoryContext loopMemContext, oldMemContext;

	pqsignal(SIGTERM, seq_req_handler_sigterm);
	pqsignal(SIGUSR2, seq_req_handler_siguser2);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection("postgres", NULL, 0);
	ShardCacheInit();
	InitShardingSession();

	/* Copy the error message to waiter*/
	saved_emit_log_hook = emit_log_hook;
	emit_log_hook = copy_error_message;

	Assert(!IsTransactionState());

	/* Alloc and switch to specific memory context */
	loopMemContext =
		AllocSetContextCreate(TopMemoryContext,
							  "Sequence serivce loop memory context",
							  ALLOCSET_DEFAULT_SIZES);
	oldMemContext =
		MemoryContextSwitchTo(loopMemContext);

	while (!got_sigterm)
	{
		cnt = reap_remote_seq_reqs(reqs);
		if (cnt == 0)
		{
			wait_latch(1000);
			continue;
		}

		PG_TRY();
		{
			StartTransactionCommand();
			SPI_connect();
			handle_seq_reqs(reqs, cnt);
			SPI_finish();
			CommitTransactionCommand();
		}
		PG_CATCH();
		{
			LWLockReleaseAll();
			if (IsTransactionState())
				AbortCurrentTransaction();

			EmitErrorReport();
			FlushErrorState();

			/*
			  dzw: as explained above, clear requested&cached seqs from dynahash.
			  This IS quite necessary because this dynahash lives after this
			  process exit because of this error and be restarted.
			*/
			bool found;

			LWLockAcquire(seq_fetch_queue_lock, LW_EXCLUSIVE);
			for (int i = 0; i < cnt; i++)
			{
				SeqFetchReq *req = reqs + i;
				SharedSeqEnt *sse = hash_search(shared_seq_cache, &req->key, HASH_FIND, &found);
				sse->need_reload = true;
				sse->errcode = top_errcode();
				if (sse->errcode)
					strncpy(sse->message, rt_error_message, sizeof(sse->message));
			}
			LWLockRelease(seq_fetch_queue_lock);
		}
		PG_END_TRY();

		// inform the waiters
		for (int i = 0; i < cnt; ++i)
		{
			SeqFetchReq *req = reqs + i;
			PGSemaphoreUnlock(req->waiter->sem);
		}
		MemoryContextReset(loopMemContext);
	}

	MemoryContextSwitchTo(oldMemContext);
}

void remote_setval(Relation seqrel, int64_t next, bool iscalled)
{
	HeapTupleData tuple;
	StringInfoData sql;
	Buffer buf;

	/* Reset sequence in transaction may block the background fetch process */
	if (IsExplicitTxn())
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Kunlun-db: set curval of sequence in explicit transaction is not support")));
	}

	PG_TRY();
	{
		initStringInfo(&sql);
		appendStringInfo(&sql, "update kunlun_sysdb.sequences set curval=%ld where db='%s_%s_%s' and name='%s'",
						 next,
						 get_database_name(MyDatabaseId),
						 use_mysql_native_seq ? "@0024@0024" : "$$",
						 get_namespace_name(seqrel->rd_rel->relnamespace),
						 seqrel->rd_rel->relname.data);

		AsyncStmtInfo *asi = GetAsyncStmtInfo(seqrel->rd_rel->relshardid);

		append_async_stmt(asi, sql.data, sql.len, CMD_UPDATE, true, SQLCOM_UPDATE);

		send_multi_stmts_to_multi();
	}
	PG_CATCH();
	{
		PG_RE_THROW();
	}
	PG_END_TRY();

	/**
	 *  Get the lock of the seq file page, before invalidate the cache.
	 */
	read_seq_tuple_with_lock(seqrel, &buf, &tuple);

	invalidate_seq_shared_cache(MyDatabaseId, seqrel->rd_id, false);

	UnlockReleaseBuffer(buf);
}
