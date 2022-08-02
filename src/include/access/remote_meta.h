/*-------------------------------------------------------------------------
 *
 * remote_meta.h
 *	  POSTGRES remote access method definitions.
 *
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * src/include/access/remote_meta.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef REMOTE_META_H
#define REMOTE_META_H

#include "postgres.h"
#include "access/tupdesc.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "utils/algos.h"
#include "postmaster/bgworker.h"
#include "catalog/pg_sequence.h"
typedef struct PGPROC PGPROC;
extern bool replaying_ddl_log;
extern char *remote_stmt_ptr;

extern Oid find_root_base_type(Oid typid0);
extern void update_colnames_indices(Relation attrelation, Relation targetrelation,
	int attnum, const char *oldattname, const char *newattname);
extern const char* atsubcmd(AlterTableCmd *subcmd);
extern void build_column_data_type(StringInfo str, Oid typid,
       int32 typmod, Oid collation);
#endif /* !REMOTE_META_H */
