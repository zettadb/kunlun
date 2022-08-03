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
#include "executor/nodeRemotescan.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memdebug.h"
#include "utils/algos.h"
#include "utils/rel.h"
#include "catalog/pg_type.h"
#include "sharding/sharding_conn.h"
#include "utils/memutils.h"


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
	RemotetupAttrInfo *myinfo;	/* Cached info about each attr */
	Relation target_rel;        /* inserting into this relation. */
	AsyncStmtInfo *pasi;        /* stmt sending port */
	enum OnConflictAction action;
	StringInfoData action_str;

	int affected_rows;	/* the accumulative number of affected rows */
	List *inflight_handles; /* inflight insert stmts */
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
	initStringInfo2(&self->buf, PerLeafRelStmtBufSz, TopTransactionContext);
	self->target_rel = rel;
	self->pasi = GetAsyncStmtInfo(rel->rd_rel->relshardid);
	self->action = ONCONFLICT_NONE;
	initStringInfo(&self->action_str);
	return self;
}

OnConflictAction 
get_remote_conflict_action(RemotetupCacheState *cachestate)
{
	return cachestate->action;
}

void set_remote_onconflict_action(RemotetupCacheState *cachestate,
								  OnConflictAction action, StringInfo action_clause)
{
	cachestate->action = action;
	if (action == ONCONFLICT_UPDATE)
		appendBinaryStringInfo(&cachestate->action_str, action_clause->data, action_clause->len);
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

	appendStringInfo(&myState->buf, "%s %s into %s (",
		myState->action == ONCONFLICT_REPLACE ? "replace" : "insert",
		myState->action == ONCONFLICT_NOTHING ? "ignore" : "",
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

		thisState->typoutput =
		    my_output_funcoid(attr->atttypid, &thisState->typisvarlena);
		fmgr_info(thisState->typoutput, &thisState->finfo);
		appendStringInfo(&myState->buf, "%s, ", attr->attname.data);
	}

	shrinkStringInfo(&myState->buf, 2); // remove last ,
	appendStringInfoString(&myState->buf, ") values ");
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
	pg_tz   *origtz = NULL;
	int      extra_float_digits_saved = extra_float_digits;

	if (self->len > (1024*1024))
	{
		end_remote_insert_stmt(myState, false);
		remotetup_prepare_info(myState, typeinfo, natts);
	}
	
	/* Set or update my derived attribute info, if needed */
	if (myState->nattrs != natts ||
	    !(myState->attrinfo && typeinfo) ||
	    !equalTupleDescs(myState->attrinfo, typeinfo))
	{
		// a insert stmt could never have >1 table type.
		Assert(myState->buf.len == 0);
		remotetup_prepare_info(myState, typeinfo, natts);
	}

	MemoryContext mem = AllocSetContextCreate(CurrentMemoryContext,
						  "cache remotetup context",
						  ALLOCSET_DEFAULT_SIZES);
	MemoryContext mem_saved =  MemoryContextSwitchTo(mem);

	/* Make sure the tuple is fully deconstructed */
	slot_getallattrs(slot);

	brlen = bracket_tuple(true, myState);
	tuplen += brlen;

	extra_float_digits = 3;
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
			attrlen = append_value_str(myState, NULL, 0);
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
		if (!origtz && is_date_time_type(atttypid))
		{
			origtz = session_timezone;
			session_timezone = pg_tzset("GMT");
		}

		outputstr = OutputFunctionCall(&thisState->finfo, attr);

		/*
		 * Executor has filled up all missing fields with their default values
		 * when the slot is ready to be inserted to physical storage.
		 * So the 'typeinfo' and 'slot' always contain all fields of the table.
		 * */
		attrlen = append_value_str(myState, outputstr, atttypid);
		tuplen += attrlen;
	}

	extra_float_digits = extra_float_digits_saved;
	if (origtz)
	{
		session_timezone = origtz;
	}

	brlen = bracket_tuple(false, myState);
	tuplen -= 2; // the last field's trailing ", " is removed.
	tuplen += brlen;

	MemoryContextSwitchTo(mem_saved);
	MemoryContextDelete(mem);

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

static void check_inflight_insert_stmts(struct RemotetupCacheState *s)
{
	ListCell *lc;
	StmtSafeHandle *handle;

	/* Free the handle of finished statement,
	   and accumulated the number of affected rows
	   */
	while ((lc = list_head(s->inflight_handles)))
	{
		handle = (StmtSafeHandle *)lfirst(lc);
		if (is_stmt_eof(*handle) == false)
			break;

		s->affected_rows += get_stmt_affected_rows(*handle);
		release_stmt_handle(*handle);
		pfree(handle);
		s->inflight_handles = list_delete_first(s->inflight_handles);
	}
}

/*
  @retval true if at least one tuple is sent to remote; false if no tuple sent.
*/
bool end_remote_insert_stmt(struct RemotetupCacheState *s, bool end_of_stmt)
{
	StmtSafeHandle *handle;
	StringInfo stmt = &s->buf;
	size_t stmtlen = lengthStringInfo(stmt);

	if (stmtlen == 0)
		return false;

	// Each tuple ends with 2 chars ',' and ' ', which is not needed for the
	// last tuple of an insert stmt.
	shrinkStringInfo(stmt, 2);

	// append our stmt to the AsyncStmtInfo port.
	stmtlen = lengthStringInfo(stmt);
	handle = palloc0(sizeof(*handle));

	*handle = send_stmt_async(s->pasi,
			donateStringInfo(stmt),
			stmtlen, 
			CMD_INSERT, 
			true, 
			SQLCOM_INSERT,
			false);

	s->inflight_handles = lappend(s->inflight_handles, handle);

	// the buffer is given to async, can't be used anymore in memory buffer.
	if (!end_of_stmt)
		initStringInfo2(stmt, BLCKSZ * remote_insert_blocks, TopTransactionContext);

	check_inflight_insert_stmts(s);

	return true;
}

int get_remote_insert_stmts_affected(struct RemotetupCacheState *s)
{
	check_inflight_insert_stmts(s);
	return s->affected_rows;
}

/*
 * Append a field value string, or NULL if the field is null. some field
 * constants need to be single quoted, others must not be.
 * */
static size_t append_value_str(RemotetupCacheState *s, char *valstr, Oid typid)
{
	return appendStringInfo(&s->buf, "%s, ", valstr ? valstr : "NULL");
}
