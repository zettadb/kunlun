/*-------------------------------------------------------------------------
 *
 * planremote.h
 *
 * Copyright (c) 2019 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * IDENTIFICATION
 * 		src/include/optimizer/planremote.h
 *
 *-------------------------------------------------------------------------
*/

#ifndef PLAN_REMOTE_H
#define PLAN_REMOTE_H

#include "postgres.h"
#include "nodes/plannodes.h"

/*
  An edge of *pptr pointing to rs, we may later add a Material node M so that
  *pptr->M->rs. Each edge has one and only one such object, upper nodes may
  refer to it in multiple Lists.
*/
typedef struct RemoteScanRef
{
	Plan **pptr;
	RemoteScan *rs;
	bool materialized;
	/*
	  True if this is the 1st RemoteScan of an Append node. In certain cases
	  only the 1st need to be materialized.
	*/
	bool is_append_1st;
	Oid shardid;
	struct RemoteScanRef *next;
} RemoteScanRef;

/*
  A Plan node's list of shards that it and its decendants access.
*/
typedef struct ShardRemoteScanRef
{
	Oid shardid;
	List *rsrl;// list of RemoteScanRef pointers, or remotescan refs accessing this shard
	struct ShardRemoteScanRef *next; // for next shard
} ShardRemoteScanRef;

extern void materialize_conflicting_remotescans(PlannedStmt *pstmt);
extern bool ReleaseShardConnection(PlanState *ps);
#endif // !PLAN_REMOTE_H
