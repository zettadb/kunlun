/*-------------------------------------------------------------------------
 *
 * remotetup.h
 *	  POSTGRES remote tuple accumulation and send.
 *
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * src/include/access/remotetup.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef REMOTE_TUP_H
#define REMOTE_TUP_H

#include "postgres.h"
#include "catalog/pg_type.h"
#include "utils/algos.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "lib/stringinfo.h"

struct RemotetupCacheState;

extern struct RemotetupCacheState * CreateRemotetupCacheState(Relation rel);
extern bool cache_remotetup(TupleTableSlot *slot, ResultRelInfo *rri);
extern bool end_remote_insert_stmt(struct RemotetupCacheState *s, bool eos);
extern char *pg_to_mysql_const(Oid typid, char *c);

inline static bool is_date_time_type(Oid typid)
{
	const static Oid dttypes[] =
		{TIMETZOID, TIMESTAMPTZOID, TIMESTAMPOID, DATEOID, TIMEOID, INTERVALOID,
		 TIMESTAMPARRAYOID, DATEARRAYOID, TIMEARRAYOID, TIMESTAMPTZARRAYOID,
		 INTERVALARRAYOID, TIMETZARRAYOID };

	for (int i = 0; i < sizeof(dttypes) / sizeof(Oid); i++)
		if (dttypes[i] == typid)
			return true;
	return false;
}

/*
 * See if 'typid' is of types that can produce interval values by substracting.
 * */
inline static bool is_interval_opr_type(Oid typid)
{
	const static Oid dttypes[] =
		{TIMESTAMPTZOID, TIMESTAMPOID, DATEOID};

	for (int i = 0; i < sizeof(dttypes) / sizeof(Oid); i++)
		if (dttypes[i] == typid)
			return true;
	return false;
}
#endif // !REMOTE_TUP_H
