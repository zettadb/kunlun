/*-------------------------------------------------------------------------
 *
 * planremote.c
 *	  Special planning for remote queries.
 *
 * Copyright (c) 2019 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/plan/planremote.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "nodes/plannodes.h"
#include "optimizer/orclauses.h"
#include "optimizer/placeholder.h"

#include <limits.h>
#include <math.h>

#include "access/htup_details.h"
#include "access/parallel.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "access/remote_dml.h"
#include "access/remote_meta.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/execPartition.h"
#include "executor/executor.h"
#include "executor/nodeAgg.h"
#include "miscadmin.h"
#include "jit/jit.h"
#include "lib/bipartite_match.h"
#include "lib/knapsack.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#ifdef OPTIMIZER_DEBUG
#include "nodes/print.h"
#endif
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/paramassign.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/planremote.h"
#include "optimizer/planner.h"
#include "optimizer/prep.h"
#include "optimizer/subselect.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "parser/parse_agg.h"
#include "rewrite/rewriteManip.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "executor/nodeRemotescan.h"

/*----------------------- Materialize remote scan nodes --------------------*/
static void materialize_remotescans(ShardRemoteScanRef *p, bool mat1st_only);
static bool PlanSubtreeResultPrecached(Plan *plan);
static bool handle_plan_traverse(PlannedStmt *pstmt, Plan *parent_plan,
	Plan **pplan, void *ctx);
static void addRemotescanRefToPlan(Plan *plan, RemoteScanRef *rsr);
static ShardRemoteScanRef *dupShardRemoteScanRefs(ShardRemoteScanRef *src);
static ShardRemoteScanRef *copyShardRemoteScanRef(ShardRemoteScanRef *src);
static void plan_merge_accessed_shards(PlannedStmt *planned_stmt, Plan *plan);
static RemoteScanRef *
makeRemoteScanRef(PlannedStmt *planned_stmt, Plan **pptr,
	RemoteScan *rs, bool append1st);
static ShardRemoteScanRef *
shard_used_in_following_sibling_plans(ListCell *lc, Oid shardid);
static bool is_multi_children_plan(Plan *plan);
static List *get_children_plan_list(Plan *plan);
static void plan_merge_children(Plan *plan);

typedef bool (*decide_visit_plan_t)(Plan *plan, void*ctx);
typedef bool (*handle_plan_traverse_t)(PlannedStmt *pstmt, Plan *parent_plan,
	Plan **plan, void *ctx);

static bool plan_tree_traverse(PlannedStmt *pstmt, Plan *parent_plan, Plan **plan,
	handle_plan_traverse_t callback,
	decide_visit_plan_t decide_visit, void *context);
static List *make_init_plan_list(PlannedStmt *pstmt, Plan *plan);
static void merge_init_plans(PlannedStmt *pstmt, Plan *plan);
static void merge_plan_list(Plan *plan, List*plans);
static bool plan_list_traverse(PlannedStmt *pstmt, Plan **plan, List *plans,
	handle_plan_traverse_t callback,
	decide_visit_plan_t decide_visit, void *context, bool initPlans);

enum PlanBranchType{ NONE, OUTER = 1, INNER };

typedef struct Mat_tree_remote_ctx
{
	/*
	  which branch to materialize, currently always OUTER because outer
	  branch's node is always first retrieved. hashjoin inner is first
	  retrieved to build the hash table, but it still has some exceptions
	  where outer node's tuples are fetched first, so we still materialize
	  its outer branch regardless.
	*/
	enum PlanBranchType mat_target_branch;
	/*
	  which branch is current node being processed on its parent's tree.
	*/
	enum PlanBranchType cur_node_branch;
	/*
	  currently traversing a descendant of a Material node.
	*/
	bool under_material;
	PlannedStmt *planned_stmt;
} Mat_tree_remote_ctx;

static void
materialize_branch_conflicting_remotescans(PlannedStmt *pstmt, Plan *plan,
	Mat_tree_remote_ctx *matctx);
static void
materialize_children(PlannedStmt *pstmt, Plan *plan, Mat_tree_remote_ctx *ctx);
static List*
materialize_init_plans(PlannedStmt *pstmt, Plan *plan, Mat_tree_remote_ctx *ctx);
static void materialize_plan_lists(List *plans1, List *plans2);

static RemoteScanRef *
makeRemoteScanRef(PlannedStmt *planned_stmt, Plan **pptr,
	RemoteScan *rs, bool append1st)
{
	RemoteScanRef *rsr1 = (RemoteScanRef *)palloc0(sizeof(RemoteScanRef));
	rsr1->pptr = pptr;
	rsr1->rs = rs;
	/*
	  find rs's shardid and assign to rsr1->shardid
	*/
	RangeTblEntry *rte = rt_fetch(rs->scanrelid, planned_stmt->rtable);
	rsr1->shardid = rte->relshardid;
	rsr1->is_append_1st = append1st;
	return rsr1;
}

static void addRemotescanRefToPlan(Plan *plan, RemoteScanRef *rsr)
{
	ShardRemoteScanRef *srsr = NULL;
	Assert(plan);

	for (ShardRemoteScanRef *p = plan->shard_remotescan_refs; p; p = p->next)
	{
		if (p->shardid == rsr->shardid)
		{
			srsr = p;
			break;
		}
	}

	if (!srsr)
	{
		srsr = (ShardRemoteScanRef*)palloc0(sizeof(ShardRemoteScanRef));
		srsr->shardid = rsr->shardid;
		srsr->next = plan->shard_remotescan_refs;
		plan->shard_remotescan_refs = srsr;
	}

	srsr->rsrl = lappend(srsr->rsrl, rsr);
}

static ShardRemoteScanRef *copyShardRemoteScanRef(ShardRemoteScanRef *src)
{
	ShardRemoteScanRef *srsr = (ShardRemoteScanRef*)palloc0(sizeof(ShardRemoteScanRef));
	*srsr = *src;
	srsr->rsrl = list_copy(src->rsrl);
	return srsr;
}

static ShardRemoteScanRef *dupShardRemoteScanRefs(ShardRemoteScanRef *src)
{
	ShardRemoteScanRef *p = src, *head = NULL, *tail = NULL;

	while (p)
	{
		ShardRemoteScanRef *srsr = (ShardRemoteScanRef*)palloc0(sizeof(ShardRemoteScanRef));
		*srsr = *src;
		srsr->rsrl = list_copy(src->rsrl);

		if (tail) tail->next = srsr;
		tail = srsr;
		if (head == NULL) head = srsr;

		p = p->next;
	}

	return head;
}

static void plan_merge_accessed_shards(PlannedStmt *pstmt, Plan *plan)
{
	Plan *outp = outerPlan(plan);
	Plan *inp = innerPlan(plan);
	if (plan->initPlan) merge_init_plans(pstmt, plan);

	if (!outp && !inp)
	{
		if (IsA(plan, SubqueryScan))
			outp = ((SubqueryScan*)plan)->subplan;
		else if (is_multi_children_plan(plan))
		{
			plan_merge_children(plan);
			return;
		}
		else
		{
			plan->shard_remotescan_refs = NULL;
			return;
		}
	}
	else
	{
		Assert(!IsA(plan, SubqueryScan));
	}

	if ((!outp || !outp->shard_remotescan_refs) &&
		(!inp || !inp->shard_remotescan_refs))
	{
		plan->shard_remotescan_refs = NULL;
		return;
	}

	// Give outp->shard_remotescan_refs to plan, then go through
	// inp->shard_remotescan_refs and merge inp->shard_remotescan_refs.
	ShardRemoteScanRef *src = (outp && outp->shard_remotescan_refs) ?
		outp->shard_remotescan_refs :
		((inp && inp->shard_remotescan_refs) ? inp->shard_remotescan_refs : NULL);
	Assert(src != NULL);
	plan->shard_remotescan_refs = dupShardRemoteScanRefs(src);

	if (outp && outp->shard_remotescan_refs && inp && inp->shard_remotescan_refs)
	{
		for (ShardRemoteScanRef *q = inp->shard_remotescan_refs; q; q = q->next)
		{
			bool found = false;
			for (ShardRemoteScanRef *p = plan->shard_remotescan_refs; p; p = p->next)
			{
				if (q->shardid == p->shardid)
				{
					list_concat_unique_ptr(p->rsrl, q->rsrl);
					found = true;
					break;
				}
			}

			if (!found)
			{
				ShardRemoteScanRef *q1 = copyShardRemoteScanRef(q);
				q1->next = plan->shard_remotescan_refs;
				plan->shard_remotescan_refs = q1;
			}
		}
	}
}


/*
  Materialize decendant remotescans in either left/outer or right/inner branch.
*/
static void materialize_branch_conflicting_remotescans(PlannedStmt *pstmt,
	Plan *plan, Mat_tree_remote_ctx *ctx)
{
	Plan *outp = outerPlan(plan);
	Plan *inp = innerPlan(plan);
	List *init_plans = NULL;

	if (plan->initPlan)
		init_plans = materialize_init_plans(pstmt, plan, ctx);
	if (!outp && !inp)
	{
		if (IsA(plan, SubqueryScan))
			outp = ((SubqueryScan*)plan)->subplan;
		else if (is_multi_children_plan(plan))
		{
			materialize_children(pstmt, plan, ctx);
			if (init_plans)
			{
				materialize_plan_lists(init_plans,
					get_children_plan_list(plan));
			}
			return;
		}
		else
		{
			plan->shard_remotescan_refs = NULL;
			return;
		}
	}
	else
	{
		Assert(!IsA(plan, SubqueryScan));
	}

	/*
	  InitPlan finishes execution when outer/inner trees start execution.
	*/
	if (((!outp || !outp->shard_remotescan_refs) ||
		 (!inp || !inp->shard_remotescan_refs)))
	{
		return;
	}

	/*
	  hashjoin *almost* always first cache all inner tuples, but there are
	  exceptions where one row is fetched first in outer plan.
	  Nestloop may choose to materialize its inner tree but always one row is
	  fetched from outer plan first.
	  In such cases if outer tree is Append, only its 1st remotescan child
	  need to be materialized and only if the child conflicts with inner tree.
	*/
	bool mat_1st_only = false;
	if ((IsA(plan, HashJoin) || (IsA(plan, NestLoop) && IsA(inp, Material))) &&
		IsA(outp, Append))
		mat_1st_only = true;
	
	int nmats = 0;

	for (ShardRemoteScanRef *p = outp->shard_remotescan_refs; p; p = p->next)
	{
		for (ShardRemoteScanRef *q = inp->shard_remotescan_refs; q; q = q->next)
		{
			if (q->shardid == p->shardid)
			{
				materialize_remotescans(ctx->mat_target_branch == OUTER ? p :
					(ctx->mat_target_branch == INNER ? q : NULL), mat_1st_only);
				nmats++;
				break;
			}
		}
	}

	/*
	  If there are conflicting descendants in NestLoop, always materialize
	  inner node, otherwise the descendants in inner node may cache&rewind,
	  but the mysql results may be removed by some  descendent in right branch.
	*/
	if (nmats > 0 && IsA(plan, NestLoop) && !PlanSubtreeResultPrecached(inp))
		innerPlan(plan) = materialize_finished_plan(inp);

	if (init_plans)
	{
		List *branch_plans = NULL;
		if (outp && outp->shard_remotescan_refs) branch_plans = lappend(branch_plans, outp);
		if (inp && inp->shard_remotescan_refs) branch_plans = lappend(branch_plans, inp);
		if (branch_plans) materialize_plan_lists(init_plans, branch_plans);
	}
}

static void materialize_remotescans(ShardRemoteScanRef *p, bool mat_1st_only)
{
	ListCell *lc;

	foreach (lc, p->rsrl)
	{
		RemoteScanRef *rsr = (RemoteScanRef *)lfirst(lc);
		if (rsr->materialized || (mat_1st_only && !rsr->is_append_1st)) continue;
		Material *matnode = (Material*)materialize_finished_plan((Plan*)rsr->rs);
		*rsr->pptr = (Plan*)matnode;
		matnode->remote_fetch_all = true;
		matnode->plan.shard_remotescan_refs =
			dupShardRemoteScanRefs(rsr->rs->plan.shard_remotescan_refs);
		rsr->materialized = true;
	}
}


/*
  Plan tree traverse callback. return true to resume traverse,
  return false to stop traverse.
*/
static bool handle_plan_traverse(PlannedStmt *pstmt, Plan *parent_plan,
	Plan **pplan, void *ctx)
{
	Plan *plan = *pplan;
	RemoteScanRef*rsr;
	bool append1st = false;
	Mat_tree_remote_ctx *matctx = (Mat_tree_remote_ctx *)ctx;

	switch(plan->type)
	{
	case T_RemoteScan:
		if (parent_plan && IsA(parent_plan, Append) &&
			linitial(((Append*)parent_plan)->appendplans) == plan)
			append1st = true;
		rsr = makeRemoteScanRef(pstmt, pplan, (RemoteScan*)plan, append1st);
		addRemotescanRefToPlan(plan, rsr);
		break;
	case T_Material:
		((Material*)plan)->remote_fetch_all = true;
		matctx->under_material = false;// its descendants have all been visited.
		plan_merge_accessed_shards(pstmt, plan);
		break;
	default:
		if (!matctx->under_material)
		{
			materialize_branch_conflicting_remotescans(pstmt, plan,
				(Mat_tree_remote_ctx*)ctx);
		}
		plan_merge_accessed_shards(pstmt, plan);
		break;
	}

	return true;
}

static inline bool visit_plan_node(Plan *plan, void *ctx)
{
	Mat_tree_remote_ctx *matctx = (Mat_tree_remote_ctx *)ctx;
	return !(matctx->cur_node_branch == matctx->mat_target_branch &&
			 PlanSubtreeResultPrecached(plan) && !IsA(plan, Material));
	// need to handle Material nodes and traverse down, but don't materialze
	// its decendants.
}

static bool plan_list_traverse(PlannedStmt *pstmt, Plan **plan, List *plans,
	handle_plan_traverse_t callback,
	decide_visit_plan_t decide_visit, void *context, bool initPlans)
{
	bool stop = false;
	ListCell *lc = NULL;
	int nthc = 0;
	int nplans = list_length(plans);
	Plan *cplan = NULL;
	Plan **pouter = &((*plan)->lefttree);
	Mat_tree_remote_ctx *matctx = (Mat_tree_remote_ctx *)context;

	foreach(lc, plans)
	{
		if (++nthc < nplans)
			matctx->cur_node_branch = OUTER;
		else
			matctx->cur_node_branch = INNER;
		if (initPlans)
		{
			/*
			  Every initPlan nodes must be traversed.
			*/
			matctx->cur_node_branch = matctx->mat_target_branch;
			SubPlan    *subplan = (SubPlan *) lfirst(lc);
			ListCell *lc1;
			lc1 = list_nth_cell(pstmt->subplans, subplan->plan_id - 1);
			cplan = (Plan *)lfirst(lc1);
			pouter = (Plan **)&(lfirst(lc1));
		}
		else
		{
			cplan = (Plan *)lfirst(lc);
			pouter = (Plan **)&(lfirst(lc));
		}

		if (decide_visit(cplan, context))
			stop = !plan_tree_traverse(pstmt, *plan, pouter, callback,
						decide_visit, context);
		if (stop) return !stop;
	}

	return stop;
}

/*
  Last order plan tree traverse. return true to resume traverse,
  return false to stop traverse.
*/
static bool
plan_tree_traverse(PlannedStmt *pstmt, Plan *parent_plan, Plan **plan,
	handle_plan_traverse_t callback,
	decide_visit_plan_t decide_visit, void *context)
{
	Plan *outer = outerPlan(*plan);
	Plan *inner = innerPlan(*plan);
	Plan **pouter = &((*plan)->lefttree);
	Mat_tree_remote_ctx *matctx = (Mat_tree_remote_ctx *)context;

	bool stop = false;
	
	if (IsA(*plan, Material)) matctx->under_material = true;

	if (is_multi_children_plan(*plan))
	{
		/*
		  Traverse children plans one by one.
		*/
		List *children_plans = get_children_plan_list(*plan);
		plan_list_traverse(pstmt, plan, children_plans, callback,
			decide_visit, context, false);
		if ((*plan)->initPlan)
			plan_list_traverse(pstmt, plan, (*plan)->initPlan, callback,
				decide_visit, context, true);
		goto self;
	}

	if ((*plan)->initPlan)
		plan_list_traverse(pstmt, plan, (*plan)->initPlan, callback,
			decide_visit, context, true);

	if (IsA(*plan, SubqueryScan))
	{
		Assert(!outer && !inner);
		outer = ((SubqueryScan*)*plan)->subplan;
		pouter = &(((SubqueryScan*)*plan)->subplan);
	}

	/*
	  Must update context before calling decide_visit().
	*/
	matctx->cur_node_branch = OUTER;
	if (outer && decide_visit(outer, context))
	{
		stop = !plan_tree_traverse(pstmt, *plan, pouter, callback,
					decide_visit, context);
	}

	if (stop) return !stop;

	matctx->cur_node_branch = INNER;
	if (inner && decide_visit(inner, context))
	{
		stop = !plan_tree_traverse(pstmt, *plan, &((*plan)->righttree),
			callback, decide_visit, context);
	}

	if (stop) return !stop;
self:
	stop = !callback(pstmt, parent_plan, plan, context);
	return !stop;
}

/*
  Note this isn't the same as ExecMaterializesOutput().
  If a plan node's subtree's result is pre-cached, its all rows is cached to
  tuplestore/tuplesort when the 1st row is fetched, and this means any
  decendant RemoteScan node won't occupy the communication channel when nodes
  out of the subtree is executed, so this plan's decendant remotescan nodes don't
  need to be materialized for execution of nodes out of the plan subtree.
*/
static bool PlanSubtreeResultPrecached(Plan *plan)
{
	bool ret = false;
	switch (plan->type)
	{
	case T_Sort:
	case T_TableFuncScan:
	case T_NamedTuplestoreScan:
	case T_WorkTableScan:
	case T_Material:
	case T_FunctionScan:
		ret = true;
		break;

	/*
	  TODO:
	  RecursiveUnion and CteScanState need extra work, currently they cache
	  tuples from the ctequery only lazily one tuple at a time driven by upper
	  nodes. Need to do eager mode to cache all rows into tuplestore when
	  the 1st row is fetched.
	*/
	case T_RecursiveUnion:
	case T_CteScan:
		break;
	default:
		break;
	}
	return ret;
}

/*
  If we need to read from multiple remote tables from the same shard, since we
  have only one MySQL client connection,
  we would need to read them one after another, which is NOT how the executor
  works. So we have to insert Material nodes if necessary to read all
  qualifying rows at once and cache them for later use. But we want to
  try out best to avoid doing so because of the performance penalty.
*/
void materialize_conflicting_remotescans(PlannedStmt *pstmt)
{
	Mat_tree_remote_ctx ctx;
	ctx.mat_target_branch = OUTER;
	ctx.cur_node_branch = NONE;
	ctx.planned_stmt = pstmt;
	ctx.under_material = false;

	plan_tree_traverse(pstmt, NULL, &pstmt->planTree, handle_plan_traverse,
		visit_plan_node, &ctx);
}

/*
  Return true if the plan has multiple children plans and not using the
  Plan::left_/right_tree members to reference them.
  All other plans either have no children plans or use 
  Plan::left_/right_tree members to reference them.
*/
static bool is_multi_children_plan(Plan *plan)
{
	bool ret;
	switch (plan->type)
	{
	case T_BitmapAnd:
	case T_BitmapOr:
	case T_MergeAppend:
	case T_Append:
	case T_ModifyTable:
		ret = true;
		break;
	default:
		ret = false;
		break;
	}
	return ret;
}

static List*
materialize_init_plans(PlannedStmt *pstmt, Plan *plan, Mat_tree_remote_ctx *ctx)
{
	Assert(plan->initPlan);
	List *plans = make_init_plan_list(pstmt, plan);
	ListCell *lc = NULL;
	foreach(lc, plans)
	{
		Plan *outp = (Plan*)lfirst(lc);
		for (ShardRemoteScanRef *p = outp->shard_remotescan_refs; p; p = p->next)
		{
			ShardRemoteScanRef *q = NULL;
			if ((q = shard_used_in_following_sibling_plans(lc->next, p->shardid)))
			{
				materialize_remotescans(ctx->mat_target_branch == OUTER ? p :
					(ctx->mat_target_branch == INNER ? q : NULL), false);
				break;
			}
		}
	}

	return plans;
}

static void materialize_children(PlannedStmt *pstmt, Plan *plan, Mat_tree_remote_ctx *ctx)
{
	Assert(plan->type == T_MergeAppend || plan->type == T_ModifyTable ||
		   plan->type == T_Append);
	/*
	  Never need to materialize is_multi_children_plan() nodes' children
	  other than the two below because remotescans
	  don't do tidbitmap scan, and table partitions are read one after another
	  in Append node.
	  However upper nodes might still materialize the remote table partitions
	  later which are children of this Append node when its siblings conflict,
	  so we must merge them to the Append node in plan_merge_children().
	*/
	if (plan->type == T_Append) return;

	/*
	  MergeAppend and ModifyTable have to be handled specifically.
	  For MergeAppend node, its children nodes are fetched from the 1st one to
	  the last one, so for any child[i] (i in [0, N-1)),
	  if child[i+j] (j>=1, i+j<N) accesses a shard accessed by child[i], this
	  shard must be materialized.

	  For ModifyTable, the only possible issue is 'insert into ... select from ...',
	  where the data source tables may conflict, and source table accesses may
	  conflict with dest table accesses.
	*/
	ListCell *lc = NULL;

	if (plan->type == T_MergeAppend)
	{
		MergeAppend *merge_append = (MergeAppend *)plan;
		foreach(lc, merge_append->mergeplans)
		{
			Plan *outp = (Plan*)lfirst(lc);
			for (ShardRemoteScanRef *p = outp->shard_remotescan_refs; p; p = p->next)
			{
				ShardRemoteScanRef *q = NULL;
				if ((q = shard_used_in_following_sibling_plans(lc->next, p->shardid)))
				{
					materialize_remotescans(ctx->mat_target_branch == OUTER ? p :
						(ctx->mat_target_branch == INNER ? q : NULL), false);
					break;
				}
			}
		}
	}
	else if (plan->type == T_ModifyTable)
	{
		ModifyTable *mt = ((ModifyTable *)plan);
		if (mt->operation == CMD_INSERT)
		{
			/*
			  materialize data source nodes that conflict with dest shards,
			  There are no 'dest nodes', we find dest shards via mt->resultRelations.
			*/
			ListCell *lc, *lc1;
			List *shardids_list = NULL;

			foreach(lc, mt->resultRelations)
			{
				RangeTblEntry *rte = rt_fetch(lfirst_int(lc), ctx->planned_stmt->rtable);
				if (rte->relkind == RELKIND_PARTITIONED_TABLE)
				{
					// TODO: get the list of baserels' relshardids to check against.
					Relation rel = relation_open(rte->relid, NoLock);
					GetPartitionStorageShards(rel, &shardids_list);
					relation_close(rel, NoLock);
				}
				else
					shardids_list = list_append_unique_oid(shardids_list, rte->relshardid);
			}

			foreach(lc, shardids_list)
			{
				foreach (lc1, mt->plans)
				{
					Plan *plan1 = lfirst(lc1);
					for (ShardRemoteScanRef *srsr = plan1->shard_remotescan_refs; srsr;
						 srsr = srsr->next)
					{
						if (srsr->shardid == lfirst_oid(lc))
						{
							materialize_remotescans(srsr, false);
							break;
						}
					}
				}
			}
		}
	}
}

/*
  If any pair of plans from the two lists conflict, materialize both, because
  for the initPlan case there is no fixed order of execution among plan nodes in
  initPlan list and nodes in left/righttree or children list, and executions
  could be interleaved for parameterized situations.
*/
static void materialize_plan_lists(List *plans1, List *plans2)
{
	ListCell *lc1 = NULL;
	ListCell *lc2 = NULL;

	foreach(lc1, plans1)
	{
		foreach(lc2, plans2)
		{
			Plan *plan1 = lfirst(lc1);
			Plan *plan2 = lfirst(lc2);
			for (ShardRemoteScanRef *srsr1 = plan1->shard_remotescan_refs; srsr1;
				 srsr1 = srsr1->next)
			{
				for (ShardRemoteScanRef *srsr2 = plan2->shard_remotescan_refs; srsr2;
					 srsr2 = srsr2->next)
				{
					if (srsr1->shardid == srsr2->shardid)
					{
						materialize_remotescans(srsr1, false);
						materialize_remotescans(srsr2, false);
						break;
					}
				}
			}
		}
	}
}

/*
  Returns the ShardRemoteScanRef of the 1st sibling plan starting from lc
  which is accessing the same shard as 'shardid'.
*/
static ShardRemoteScanRef *
shard_used_in_following_sibling_plans(ListCell *lc, Oid shardid)
{
	ShardRemoteScanRef *q = NULL;
	for (ListCell *p = lc; p; p = p->next)
	{
		Plan *plan = (Plan*)lfirst(p);
		for (ShardRemoteScanRef *srsr = plan->shard_remotescan_refs; srsr;
			 srsr = srsr->next)
		{
			if (srsr->shardid == shardid)
				return srsr;
		}
	}

	return q;
}


static List *get_children_plan_list(Plan *plan)
{
	List *planlist = NULL;
	switch (plan->type)
	{
	case T_BitmapAnd:
		planlist = ((BitmapAnd*)plan)->bitmapplans;
		break;
	case T_BitmapOr:
		planlist = ((BitmapOr*)plan)->bitmapplans;
		break;
	case T_MergeAppend:
		planlist = ((MergeAppend*)plan)->mergeplans;
		break;
	case T_Append:
		planlist = ((Append*)plan)->appendplans;
		break;
	case T_ModifyTable:
		planlist = ((ModifyTable*)plan)->plans;
		break;
	default:
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Kunlun-db: Node type(%d) doesn't have a list of children nodes.", plan->type)));
		break;
	}

	return planlist;
}

static void plan_merge_children(Plan *plan)
{
	List *childen_plans = get_children_plan_list(plan);
	Assert(plan->type == T_ModifyTable || plan->type == T_Append ||
		   plan->type == T_MergeAppend);
	/*
	  Merge all children plan nodes's ShardRemoteScanRef objects into plan.

	  Never merge all source&dest plan nodes' ShardRemoteScanRef objects of
	  the ModifyTable node into itself, because a ModifyTable node has no
	  parent node to fetch its rows, it's always the top-most node.
	*/
	if (plan->type == T_ModifyTable)
		return;

	if (childen_plans) merge_plan_list(plan, childen_plans);
}

static void merge_plan_list(Plan *plan, List*plans)
{
	ListCell *lc;
	foreach(lc, plans)
	{
		Plan *cplan = lfirst(lc);
		if (cplan->shard_remotescan_refs == NULL) continue;

		if (plan->shard_remotescan_refs == NULL)
		{
			plan->shard_remotescan_refs =
				dupShardRemoteScanRefs(cplan->shard_remotescan_refs);
			continue;
		}

		for (ShardRemoteScanRef *q = cplan->shard_remotescan_refs; q; q = q->next)
		{
			bool found = false;
			for (ShardRemoteScanRef *p = plan->shard_remotescan_refs; p; p = p->next)
			{
				if (q->shardid == p->shardid)
				{
					list_concat_unique_ptr(p->rsrl, q->rsrl);
					found = true;
					break;
				}
			}
	
			if (!found)
			{
				ShardRemoteScanRef *q1 = copyShardRemoteScanRef(q);
				q1->next = plan->shard_remotescan_refs;
				plan->shard_remotescan_refs = q1;
			}
		}
	}
}

static List *make_init_plan_list(PlannedStmt *pstmt, Plan *plan)
{
	List *plans = NULL;
	ListCell *lc = NULL;
	ListCell *lc1 = NULL;
	Plan *cplan = NULL;

	// assemble subplan's plan nodes into a list to work on.
	foreach(lc, plan->initPlan)
	{
		SubPlan *splan = (SubPlan *)lfirst(lc);
		lc1 = list_nth_cell(pstmt->subplans, splan->plan_id - 1);
		cplan = (Plan *)lfirst(lc1);
		plans = lappend(plans, cplan);
	}
	return plans;
}

static void merge_init_plans(PlannedStmt *pstmt, Plan *plan)
{
	List *plans = NULL;
	plans = make_init_plan_list(pstmt, plan);

	merge_plan_list(plan, plans);
}

/*-------------------End of materialize remote scan nodes --------------------*/
bool ReleaseShardConnection(PlanState *ps)
{
	switch(nodeTag(ps))
	{
	case T_RemoteScanState:
		release_shard_conn((RemoteScanState*)ps);
		break;
	default:
		break;
	}
	return false; // keep traversing.
}
