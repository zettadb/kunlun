/*-------------------------------------------------------------------------
 *
 * pg_ddl_log_progress.h
 *	  definition of the "pg_ddl_log_progress" system catalog (pg_ddl_log_progress)
 *
 *
 * Copyright (c) 2019 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * src/include/catalog/pg_ddl_log_progress.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_DDL_LOG_PROGRESS_H
#define PG_DDL_LOG_PROGRESS_H

#include "catalog/genbki.h"
#include "catalog/pg_ddl_log_progress_d.h"


/*
 *
 * A one row catalog table stored in each computing node. It stores the largest
 * ddl operation id this computing node has executed. The ddl operation is
 * eithre replicated from metadata server's ddl log, or comes from client.
 * */
CATALOG(pg_ddl_log_progress,12348,DDLLogProgressRelationId) BKI_SHARED_RELATION BKI_WITHOUT_OIDS
{
	/*
	 * Each db has one such row. One extra row with dbid=0 for sync state log
	 * position, stored in ddl_op_id field.
	 * */
	Oid dbid;

	/*
	 * The max op-id the computing node received from ddl log and completed
	 * execution locally. This op is definitely not initially executed by
	 * this computiing node because each node only look for ddl log ops
	 * executed by other nodes to replicate the ddl operation locally.
	 * */
	int64 ddl_op_id;
	/*
	 * The max op-id a computing node received from client and completed
	 * execution locally.
	 * */
	int64 max_op_id_done_local;
} FormData_pg_ddl_log_progress;

typedef FormData_pg_ddl_log_progress*Form_pg_ddl_log_progress;
#endif /* !PG_DDL_LOG_PROGRESS_H */
