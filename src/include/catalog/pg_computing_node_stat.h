/*-------------------------------------------------------------------------
 *
 * pg_computing_node_stat.h
 *	  definition of the "pg_computing_node_stat" system catalog (pg_computing_node_stat)
 *
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * src/include/catalog/pg_computing_node_stat.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_COMP_NODE_STAT_H
#define PG_COMP_NODE_STAT_H

#include "catalog/genbki.h"
#include "catalog/pg_computing_node_stat_d.h"



/*
 * Each computing node has its own private stats, they don't sync/exchange
 * updates to this table. stored in computing node as a catalog table.
 * Each row is an accumulation of stats within a period of time, note the
 * accumulated_since field. These stats can help determine the quality of
 * data location and help making decisions to improve performance.
 * */
CATALOG(pg_computing_node_stat,12347,ComputingNodeStatRelationId) BKI_SHARED_RELATION
{
	/* primary key references comp_nodes(id), comp node id */
	Oid comp_node_id;
	/* NO. of queries processed */
	int64 num_queries BKI_DEFAULT(0);
	/* NO. of single shard queries, the bigger the better */
	int64 num_1shard_queries BKI_DEFAULT(0);
	/* NO. of slow client queries executed */
	int64 num_slow_queries BKI_DEFAULT(0);
	/* NO. of queries not able to process */
	int64 num_rejected_queries BKI_DEFAULT(0);
	/* NO. of illegal queries handled, i.e. queries violating access priviledges, etc */
	int64 num_illegal_queries BKI_DEFAULT(0);
	/* NO. of read-only queries recved from client */
	int64 num_ro_queries BKI_DEFAULT(0);
	/* NO. of queries sent to storage nodes. */
	int64 num_sent_queries BKI_DEFAULT(0);
	/* total amount in bytes of query results received from storage nodes */
	int64 num_recv_res BKI_DEFAULT(0);
	/* NO. of transactions processed, including aborted, committed, 2PC or single. */
	int64 num_txns BKI_DEFAULT(0);
	/* NO. of single shard transactions executed, the bigger the better */
	int64 num_1shard_txns BKI_DEFAULT(0);
	/* NO. of read-only transactions executed */
	int64 num_ro_txns BKI_DEFAULT(0);
	/* NO. of transactions rolled back */
	int64 num_rb_txns BKI_DEFAULT(0);
	/* NO. of transactions rolled back because of deadlock */
	int64 num_rb_txns_deadlock BKI_DEFAULT(0);
#ifdef CATALOG_VARLEN
	/* all stats of this node are accumulated since this timestamp. */
	timestamptz accumulated_since;
#endif

} FormData_pg_computing_node_stat;

typedef FormData_pg_computing_node_stat*Form_pg_computing_node_stat;
#endif /* !PG_COMP_NODE_STAT_H */
