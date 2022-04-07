/*-------------------------------------------------------------------------
 *
 * remotetup.c
 *     Cache TupleTableSlot tuples produced by executor into executer's per
 *     leaf table buffer in order to send to the remote storage node.
 *     Code in this file is derived from printtup.c, for now we only produce
 *     text output. and the code in this file only handles 'insert into' stmt,
 *     Update and delete stmts are handled in dml.c.
 *
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * IDENTIFICATION
 *	  src/backend/access/common/remotetup.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pgtime.h"
#include "miscadmin.h"
#include "access/printtup.h"
#include "access/remotetup.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memdebug.h"
#include "utils/algos.h"
#include "utils/rel.h"
#include "catalog/pg_type.h"
#include "sharding/sharding_conn.h"


/* ----------------
 *		Private state for a remotetup destination object
 *
 * NOTE: finfo is the lookup info for either typoutput or typsend, whichever
 * we are using for this column.
 * ----------------
 */
typedef struct RemotetupAttrInfo
{								/* Per-attribute information */
	Oid			typoutput;		/* Oid for the type's text output fn */
	Oid			typsend;		/* Oid for the type's binary output fn */
	bool		typisvarlena;	/* is it varlena (ie possibly toastable)? */
	int16		format;			/* format code for this column */
	FmgrInfo	finfo;			/* Precomputed call info for output fn */
} RemotetupAttrInfo;

typedef struct RemotetupCacheState
{
	StringInfoData buf;      /* public part, must be 1st field. */
	TupleDesc	attrinfo;		/* The attr info we are set up for */
	int			nattrs;
	/* The offset of the end of the last valid tuple stored in buffer */
	size_t      last_tup_end_offset;
	RemotetupAttrInfo *myinfo;	/* Cached info about each attr */
	Relation target_rel;        /* inserting into this relation. */
	AsyncStmtInfo *pasi;        /* stmt sending port */
} RemotetupCacheState;


static size_t append_value_str(RemotetupCacheState *s, char *valstr, Oid typid);
static size_t bracket_tuple(bool start, RemotetupCacheState *s);
static int remote_insert_blocks = 1;
int max_remote_insert_blocks = 1024;
static inline void grow_remote_insert_blocks()
{
	remote_insert_blocks *= 2;
	if (remote_insert_blocks > max_remote_insert_blocks)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db COPY FROM: Remote insert buffers(%d) exceeds max limit(%d).",
				 		remote_insert_blocks, max_remote_insert_blocks),
				 errhint("Increase max_remote_insert_blocks or shrink tuple to be inserted.")));
}

struct RemotetupCacheState *CreateRemotetupCacheState(Relation rel)
{
	static size_t PerLeafRelStmtBufSz = 4*1024;

	RemotetupCacheState *self = (RemotetupCacheState *)
		palloc0(sizeof(RemotetupCacheState));
	// It's verified that the current memcxt is EState's memcxt.
	initStringInfo2(&self->buf, PerLeafRelStmtBufSz,CurrentMemoryContext);
	self->target_rel = rel;
	self->pasi = GetAsyncStmtInfo(rel->rd_rel->relshardid);
	return self;
}

/*
 * Get the lookup info that remotetup() needs
 */
static void
remotetup_prepare_info(RemotetupCacheState*myState,
					   TupleDesc typeinfo, int numAttrs)
{
	int			i;

	/* get rid of any old data */
	if (myState->myinfo)
		pfree(myState->myinfo);
	myState->myinfo = NULL;

	myState->attrinfo = typeinfo;
	myState->nattrs = numAttrs;
	if (numAttrs <= 0)
		return;

	myState->myinfo = (RemotetupAttrInfo *)
		palloc0(numAttrs * sizeof(RemotetupAttrInfo));

	appendStringInfo(&myState->buf, "insert into %s (",
		make_qualified_name(myState->target_rel->rd_rel->relnamespace,
		  myState->target_rel->rd_rel->relname.data, NULL));

	for (i = 0; i < numAttrs; i++)
	{
		RemotetupAttrInfo *thisState = myState->myinfo + i;
		Form_pg_attribute attr = TupleDescAttr(typeinfo, i);
		if (column_name_is_dropped(attr->attname.data))
		{
			typeinfo->attrs[i].attisdropped = true;
			continue;
		}

		getTypeOutputInfo(attr->atttypid,
						  &thisState->typoutput,
						  &thisState->typisvarlena);
		fmgr_info(thisState->typoutput, &thisState->finfo);
		appendStringInfo(&myState->buf, "%s, ", attr->attname.data);
	}

	shrinkStringInfo(&myState->buf, 2); // remove last ,
	appendStringInfoString(&myState->buf, ") values ");
	myState->last_tup_end_offset = lengthStringInfo(&myState->buf);
}

bool column_name_is_dropped(const char *colname)
{
#define DROPPED_COLNAME "........pg.dropped."
	return (strncmp(colname, DROPPED_COLNAME, sizeof(DROPPED_COLNAME) - 1) == 0);
}

/* ----------------
 *		remotetup --- cache the tuple proper.
 *		slot: the executor's tuple ready for storage;
 *		self: memory buffer to store the remote tuple data
 * @retval: true if successfully stores 'slot' into self;
 * false if insert buffer has no enough memory for 'slot' tuple, and in this case
 * DBA needs to set a bigger remote insert buffer.
 * ----------------
 */
bool cache_remotetup(TupleTableSlot *slot, ResultRelInfo *resultRelInfo)
{
	TupleDesc	typeinfo = resultRelInfo->ri_RelationDesc->rd_att;
	RemotetupCacheState *myState = resultRelInfo->ri_RemotetupCache;
	int			natts = typeinfo->natts;
	int			i;
	size_t tuplen = 0, attrlen = 0, brlen;
	StringInfo self = &myState->buf;

	if (self->len > (1024*1024))
	{
		end_remote_insert_stmt(myState, false);
		remotetup_prepare_info(myState, typeinfo, natts);
	}

	/* Set or update my derived attribute info, if needed */
	if (myState->nattrs != natts || !(myState->attrinfo && typeinfo) ||
		!equalTupleDescs(myState->attrinfo, typeinfo))
	{
retry:
		// a insert stmt could never have >1 table type.
		Assert(myState->buf.len == 0);
		remotetup_prepare_info(myState, typeinfo, natts);
	}

	/* Make sure the tuple is fully deconstructed */
	slot_getallattrs(slot);

	if ((brlen = bracket_tuple(true, myState)) == 0)
	{
		truncateStringInfo(self, myState->last_tup_end_offset);
		if (!end_remote_insert_stmt(myState, false))
			grow_remote_insert_blocks(); // tuple too big
		goto retry;
	}
	tuplen += brlen;

	pg_tz *gmt_tz = pg_tzset("GMT"), *origtz = NULL;
	int orig_datestyle = -1, orig_dateorder = -1, orig_intvstyle = -1;

	/*
	 * cache the attributes of this tuple
	 */
	for (i = 0; i < natts; ++i)
	{
		RemotetupAttrInfo *thisState = myState->myinfo + i;
		Datum		attr = slot->tts_values[i];

		/*
		  skip the dropped column's NULL field value.
		*/
		if (typeinfo->attrs[i].attisdropped)
			continue;

		if (slot->tts_isnull[i])
		{
			if (!(attrlen = append_value_str(myState, NULL, 0)))
			{
				truncateStringInfo(self, myState->last_tup_end_offset);
				if (!end_remote_insert_stmt(myState, false))
					grow_remote_insert_blocks(); // tuple too big
				goto retry;
			}
			tuplen += attrlen;
			continue;
		}

		if (thisState->typisvarlena)
			VALGRIND_CHECK_MEM_IS_DEFINED(DatumGetPointer(attr),
										  VARSIZE_ANY(attr));

		/* Text output */
		char	   *outputstr = NULL;
		Oid atttypid = typeinfo->attrs[i].atttypid;

		/*
		 * Always produce date/time/timestamp/internval/timetz/timestamptz values
		 * using ISO, YMD date style/order in UTC+0 timezone.
		 * Temporarily modify the 3 session vars to do so and restore
		 * them after done.
		 * */
		if (orig_datestyle == -1 && is_date_time_type(atttypid))
		{
			orig_datestyle = DateStyle;
			orig_dateorder = DateOrder;
			origtz = session_timezone;
			orig_intvstyle = IntervalStyle;

			DateStyle = USE_ISO_DATES;
			DateOrder = DATEORDER_YMD;
			IntervalStyle = INTSTYLE_ISO_8601;
			session_timezone = gmt_tz;
		}

		outputstr = OutputFunctionCall(&thisState->finfo, attr);

		/*
		 * Executor has filled up all missing fields with their default values
		 * when the slot is ready to be inserted to physical storage.
		 * So the 'typeinfo' and 'slot' always contain all fields of the table.
		 * */
		if (!(attrlen = append_value_str(myState, outputstr, atttypid)))
		{
			truncateStringInfo(self, myState->last_tup_end_offset);
			if (!end_remote_insert_stmt(myState, false))
				grow_remote_insert_blocks(); // tuple too big
			goto retry;
		}
		tuplen += attrlen;
	}

	if (orig_datestyle != -1)
	{
		DateStyle = orig_datestyle;
		DateOrder = orig_dateorder;
		IntervalStyle = orig_intvstyle;
		session_timezone = origtz;
	}

	brlen = bracket_tuple(false, myState);
	if (brlen == 0)
	{
		truncateStringInfo(self, myState->last_tup_end_offset);
		if (!end_remote_insert_stmt(myState, false))
			grow_remote_insert_blocks(); // tuple too big
		goto retry;
	}

	tuplen -= 2; // the last field's trailing ", " is removed.
	tuplen += brlen;

	myState->last_tup_end_offset += tuplen;
	Assert(myState->last_tup_end_offset == lengthStringInfo(self));
	return true;
}


/*
 * append ( or ), to tuple to form a valid multi-tuple insert stmt.
 * @param start: true if appending start of tuple
 *      false if appending end of tuple
 * */
static size_t bracket_tuple(bool start, RemotetupCacheState *s)
{
	int ret;
	// the last field's trailing ", " is removed.
	int extra = (start ? 0 : 2/*remove last comma(,)*/);
	shrinkStringInfo(&s->buf, extra);

	ret = appendStringInfoString(&s->buf, start ? "(" : "), ");
	return ret;
}

/*
  @retval true if at least one tuple is sent to remote; false if no tuple sent.
*/
bool end_remote_insert_stmt(struct RemotetupCacheState *s, bool end_of_stmt)
{
	StringInfo self = &s->buf;
	if (lengthStringInfo(self) == 0)
		return false;

	if (s->pasi->rss_owner)
	{
		MaterializeOtherRemoteScan(s->pasi->rss_owner);
		Assert(s->pasi->rss_owner == NULL);
	}

	// Each tuple ends with 2 chars ',' and ' ', which is not needed for the
	// last tuple of an insert stmt.
	shrinkStringInfo(self, 2);
	s->last_tup_end_offset -= 2;

	// append our stmt to the AsyncStmtInfo port.
	size_t stmtlen = lengthStringInfo(self);	
	append_async_stmt(s->pasi, donateStringInfo(self), stmtlen, CMD_INSERT, true, SQLCOM_INSERT);
	// the buffer is given to async, can't be used anymore in memory buffer.
	if (!end_of_stmt)
		initStringInfo2(self, BLCKSZ*remote_insert_blocks, CurrentMemoryContext);

	/*
	 * Push the sending work forward. If result pending, try receive it, and
	 * if no result recvd yet, we can't send next stmt.
	 * */
	if (s->pasi->result_pending && send_stmt_to_multi_try_wait(s->pasi, 1) == 0) return true;

	int rc = work_on_next_stmt(s->pasi);
	if (rc == 1)
		send_stmt_to_multi_start(s->pasi, 1);
	else if (rc < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: Internal error: mysql result has not been consumed yet.")));
	return true;
}

char *pg_to_mysql_const(Oid typid, char *c)
{
	static const char *bool_trues[] = {"true","on","yes","1", "t","y"};
	static const char *bool_falses[] = {"false","off","no","0", "f","n"};
	if (typid == BOOLOID)
	{
		for (int i = 0; i < sizeof(bool_trues)/sizeof(char*); i++)
			if (strcasecmp(c, bool_trues[i]) == 0)
				return "true";
		for (int i = 0; i < sizeof(bool_falses)/sizeof(char*); i++)
			if (strcasecmp(c, bool_falses[i]) == 0)
				return "false";
	}

	if (typid == TIMESTAMPTZOID || typid == TIMETZOID)
	{
		/*
		 * Remove the time zone indicator.
		 * */
		char *ptz = strrchr(c, '+');
		if (ptz)
			*ptz = '\0';
		ptz = strrchr(c, '-');
		if (ptz)
		{
			char *pcolon = strchr(c, ':');
			if (pcolon != NULL && pcolon < ptz)
				*ptz = '\0';
		}
	}

	if (typid == CASHOID)
	{
		/*
		 * Money value begins with a cash sign, e.g. $. maybe some bizzar
		 * cash sign has >1 chars.
		 * remove the money value format chars such as $ and , e.g. $1,000.00
		*/
		bool cival = false;
		int cpidx = -1;

		for (int i = 0; c[i] ; i++)
		{
			char ci = c[i];
			if ((ci >= '0' && ci <= '9') || ci == '+' || ci == '-' || ci == '.')
				cival = true;
			else
				cival = false;

			Assert(cpidx < i);
			if (cpidx >= 0 && cival)
				c[cpidx++] = c[i];
			else if (cpidx < 0 && !cival)
				cpidx = i;
		}

		if (cpidx > 0) c[cpidx] = '\0';
	}

	return c;
}

/*
 * Append a field value string, or NULL if the field is null. some field
 * constants need to be single quoted, others must not be.
 * */
static size_t append_value_str(RemotetupCacheState *s, char *valstr, Oid typid)
{
	int ret = 0;

	if (!valstr)
	{
		ret = appendStringInfo(&s->buf, "%s, ", "NULL");
		return ret;
	}

	valstr = pg_to_mysql_const(typid, valstr);
	int need = const_output_needs_quote(typid);
	if (need == -1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Column type not supported by remote storage node in kunlun.")));

	const char *sep = (need ? "'" : "");

	/*
	 * NULL must not be ' quoted.
	 * numeric const values must not be ' quoted.
	 * bool consts can be quoted or not, we choose not to quote it.
	 * varchar(n), char(n), date, time, timestamp, text, blob, enum, set types must be ' quoted;
	 * bit(n) is very special, its constant is like: b'1001110'
	 * */
	ret = appendStringInfo(&s->buf, "%s%s%s%c ",
				   ((typid == BITOID || typid == VARBITOID) ? "b'" : sep), valstr, sep, ',');

	return ret;
}
