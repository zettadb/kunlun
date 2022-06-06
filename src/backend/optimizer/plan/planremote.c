/*-------------------------------------------------------------------------
 *
 * planremote.c
 *	  Special planning for remote queries.
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
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
	decide_visit_plan_t decide_visit, void *context, bool is_subplan_ref);
static int materialize_plan(Plan *tgt_plan);

enum PlanBranchType{ NONE, OUTER = 1, INNER, OUTER_AND_INNER };

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

	// subplan IDs whose referenced Plan nodes that have been processed.
	Bitmapset *mat_subplans;
} Mat_tree_remote_ctx;

#define MAT_OUTER_STRICT(ctx) ((ctx)->mat_target_branch == OUTER)
#define MAT_INNER_STRICT(ctx) ((ctx)->mat_target_branch == INNER)
#define MAT_OUTER(ctx) ((ctx)->mat_target_branch == OUTER || (ctx)->mat_target_branch == OUTER_AND_INNER)
#define MAT_INNER(ctx) ((ctx)->mat_target_branch == INNER || (ctx)->mat_target_branch == OUTER_AND_INNER)

static void
materialize_branch_conflicting_remotescans(PlannedStmt *pstmt, Plan *plan,
	Mat_tree_remote_ctx *matctx);
static void
materialize_children(PlannedStmt *pstmt, Plan *plan, Mat_tree_remote_ctx *ctx);
static List*
materialize_subplan_list(PlannedStmt *pstmt, Plan *plan, List *plans,
	Mat_tree_remote_ctx *ctx);
static void materialize_plan_lists(List *plans1, List *plans2, enum PlanBranchType bt);
static List *make_tl_subplan_list(PlannedStmt *pstmt, Plan *plan);
static List *make_qual_subplan_list(PlannedStmt *pstmt, Plan *plan);
static List *subplan_list_to_plan_list(PlannedStmt *pstmt, List *subplan_list);

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
	if (rte->relshardid == InvalidOid && rte->relid != InvalidOid &&
		rte->relkind == RELKIND_RELATION && rte->rtekind == RTE_RELATION)
	{
		Relation rel = relation_open(rte->relid, RowExclusiveLock);
		rte->relshardid = rel->rd_rel->relshardid;
		relation_close(rel, NoLock);
	}
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

ShardRemoteScanRef *dupShardRemoteScanRefs(ShardRemoteScanRef *src)
{
	ShardRemoteScanRef *p = src, *head = NULL, *tail = NULL;

	while (p)
	{
		ShardRemoteScanRef *srsr = (ShardRemoteScanRef*)palloc0(sizeof(ShardRemoteScanRef));
		*srsr = *src;
		/*
		  The RemoteScanRef objects are not duplicated, both lists' items
		  point to the same group of RemoteScanRef objects. This is what we
		  need now.
		*/
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
	List *qual_plans = NULL, *tl_plans = NULL;

	if (plan->qual_subplans)
	{
		qual_plans = subplan_list_to_plan_list(pstmt, plan->qual_subplans);
		merge_plan_list(plan, qual_plans);
	}

	if (plan->tl_subplans)
	{
		tl_plans = subplan_list_to_plan_list(pstmt, plan->tl_subplans);
		merge_plan_list(plan, tl_plans);
	}

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
			if (!IsA(plan, RemoteScan)) plan->shard_remotescan_refs = NULL;
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
					p->rsrl = list_concat_unique_ptr(p->rsrl, q->rsrl);
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
	List *branch_plans = NULL;

	if (plan->initPlan)
		init_plans = materialize_subplan_list(pstmt, plan,
			make_init_plan_list(pstmt, plan), ctx);
	
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
					get_children_plan_list(plan), OUTER_AND_INNER);
			}
			goto qual;
		}
		else if (!IsA(plan, RemoteScan))
		{
			plan->shard_remotescan_refs = NULL;
			goto init;
		}
	}
	else
	{
		Assert(!IsA(plan, SubqueryScan));
	}

	/*
	  InitPlan finishes execution when outer/inner trees start execution.
	*/
	if ((!outp || (!outp->shard_remotescan_refs && !IsA(outp, FunctionScan))) ||
		(!inp || (!inp->shard_remotescan_refs && !IsA(inp, FunctionScan))))
		goto init;

	int nmats = 0;
	/*
	  A precached node N's descendants won't conflict with any node outside N's tree.
	*/
	Plan *tgt_plan = NULL, *other_plan = NULL;
	if (MAT_OUTER_STRICT(ctx))
	{
		tgt_plan = outp;
		other_plan = inp;
	}
	else if (MAT_INNER_STRICT(ctx))
	{
		tgt_plan = inp;
		other_plan = outp;
	}

	if (PlanSubtreeResultPrecached(tgt_plan))
		goto branches_done;

	/*
	  A function is so far a blackbox, we have to assume it conflicts with
	  any node.
	*/
	if (IsA(other_plan, FunctionScan))
	{
		nmats = materialize_plan(tgt_plan);
		goto branches_done;
	}

	for (ShardRemoteScanRef *p = outp->shard_remotescan_refs; p; p = p->next)
	{
		for (ShardRemoteScanRef *q = inp->shard_remotescan_refs; q; q = q->next)
		{
			if (q->shardid == p->shardid)
			{
				ShardRemoteScanRef *tgt = NULL;
				if (MAT_OUTER(ctx))
					tgt = p;
				else if (MAT_INNER(ctx))
					tgt = q;

				materialize_remotescans(tgt, false);
				nmats++;
				break;
			}
		}
	}
branches_done:
	/*
	  If there are conflicting descendants in NestLoop, always materialize
	  inner node, otherwise the descendants in inner node may cache&rewind,
	  but the mysql results may be removed by some  descendent in right branch.
	*/
	if (nmats > 0 && IsA(plan, NestLoop) && !IsA(inp, Material))
	{
		if (inp->shard_remotescan_refs)
			materialize_remotescans(inp->shard_remotescan_refs, false);
		innerPlan(plan) = materialize_finished_plan(inp);
	}
init:
	/*
	  some code paths above have not handled initPlan yet.
	*/
	if (outp && outp->shard_remotescan_refs) branch_plans = lappend(branch_plans, outp);
	if (inp && inp->shard_remotescan_refs) branch_plans = lappend(branch_plans, inp);
	if (branch_plans)
	{
		if (init_plans)
			materialize_plan_lists(init_plans, branch_plans, OUTER_AND_INNER);
		init_plans = list_concat_unique_ptr(init_plans, branch_plans);
	}

qual:
	// Handle plans used in qual and projection. Note that projection is
	// executed after qual.
	if (plan->qual_subplans)
	{
		List *qual_plans = subplan_list_to_plan_list(pstmt, plan->qual_subplans);
		materialize_subplan_list(pstmt, plan, qual_plans, ctx);

		if (init_plans)
			materialize_plan_lists(init_plans, qual_plans, OUTER);
		init_plans = list_concat_unique_ptr(init_plans, qual_plans);
	}

	if (plan->tl_subplans)
	{
		List *tl_plans = subplan_list_to_plan_list(pstmt, plan->tl_subplans);
		materialize_subplan_list(pstmt, plan, tl_plans, ctx);

		if (init_plans)
			materialize_plan_lists(init_plans, tl_plans, OUTER);
		init_plans = list_concat_unique_ptr(init_plans, tl_plans);
	}

	if (init_plans && IsA(plan, RemoteScan))
	{
		List *selflist = lappend(NULL, plan);
		materialize_plan_lists(init_plans, selflist, OUTER_AND_INNER);
	}
}

static List *subplan_list_to_plan_list(PlannedStmt *pstmt, List *subplan_list)
{
	List *plan_list = NULL;
	ListCell *lc = NULL, *lc1 = NULL;
	Plan *qplan = NULL;

	foreach(lc, subplan_list)
	{
		SubPlan *splan = (SubPlan *)lfirst(lc);
		lc1 = list_nth_cell(pstmt->subplans, splan->plan_id - 1);
		qplan = (Plan *)lfirst(lc1);
		plan_list = lappend(plan_list, qplan);
	}
	return plan_list;
}

static void materialize_remotescans(ShardRemoteScanRef *p, bool mat_1st_only)
{
	ListCell *lc;

	foreach (lc, p->rsrl)
	{
		RemoteScanRef *rsr = (RemoteScanRef *)lfirst(lc);
		if (rsr->rs->materialized || (mat_1st_only && !rsr->is_append_1st)) continue;
		Material *matnode = (Material*)materialize_finished_plan((Plan*)rsr->rs);
		*rsr->pptr = (Plan*)matnode;
		rsr->rs->materialized = true;
		matnode->remote_fetch_all = true;
		matnode->plan.shard_remotescan_refs =
			dupShardRemoteScanRefs(rsr->rs->plan.shard_remotescan_refs);

		Plan *plan = (Plan *)rsr->rs;
		Plan *pplan = (Plan *)matnode;
		Assert(IsA(plan, RemoteScan));
		if (plan->qual_subplans)
		{
			pplan->qual_subplans = plan->qual_subplans;
		}
		if (plan->tl_subplans)
		{
			/*
			  Let matnode produce plan->targetlist
			pplan->targetlist = plan->targetlist; already done in materialize_finished_plan().
			plan->targetlist = ; get rid of the subplan nodes from tl.
			*/
			pplan->tl_subplans = plan->tl_subplans;
		}
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
	case T_Material:
		((Material*)plan)->remote_fetch_all = true;
		matctx->under_material = false;// its descendants have all been visited.
		/*
		  The mat node may have a big plan tree which need to be materialized.
		*/
		materialize_branch_conflicting_remotescans(pstmt, plan,
			(Mat_tree_remote_ctx*)ctx);
		break;
	case T_RemoteScan:
		if (parent_plan && IsA(parent_plan, Append) &&
			linitial(((Append*)parent_plan)->appendplans) == plan)
			append1st = true;
		rsr = makeRemoteScanRef(pstmt, pplan, (RemoteScan*)plan, append1st);
		addRemotescanRefToPlan(plan, rsr);
		/*
		  FALL-THROUGH
		  RemoteScan node may have correlated subplans referenced in
		  qual and/or targetlist, or have initPlans(uncorrelated plans).
		*/
	default:
		materialize_branch_conflicting_remotescans(pstmt, plan,
			(Mat_tree_remote_ctx*)ctx);
		break;
	}

	plan_merge_accessed_shards(pstmt, plan);
	return true;
}

static inline bool visit_plan_node(Plan *plan, void *ctx)
{
	return true;
	// need to handle Material and precached nodes and traverse down,
	// but don't materialze its decendants.
}

static bool plan_list_traverse(PlannedStmt *pstmt, Plan **plan, List *plans,
	handle_plan_traverse_t callback,
	decide_visit_plan_t decide_visit, void *context, bool is_subplan_ref)
{
	bool stop = false;
	ListCell *lc = NULL;
	int nthc = 0;
	int nplans = list_length(plans);
	Plan *cplan = NULL;
	Plan **pouter = NULL;
	Mat_tree_remote_ctx *matctx = (Mat_tree_remote_ctx *)context;

	foreach(lc, plans)
	{
		if (++nthc < nplans)
			matctx->cur_node_branch = OUTER;
		else
			matctx->cur_node_branch = INNER;
		if (is_subplan_ref)
		{
			/*
			  Every subplan node in initPlan/qual/tl must be traversed,
			  once and only once --- they could be referenced multiple times.
			*/
			matctx->cur_node_branch = matctx->mat_target_branch;
			SubPlan    *subplan = (SubPlan *) lfirst(lc);
			ListCell *lc1;

			if (bms_is_member(subplan->plan_id, matctx->mat_subplans))
			{
				continue;
			}

			lc1 = list_nth_cell(pstmt->subplans, subplan->plan_id - 1);
			cplan = (Plan *)lfirst(lc1);
			if (!cplan) continue;
			pouter = (Plan **)&(lfirst(lc1));
			matctx->mat_subplans = bms_add_member(matctx->mat_subplans,
				subplan->plan_id);
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

	return !stop;
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
		stop = !plan_list_traverse(pstmt, plan, children_plans, callback,
			decide_visit, context, false);
		if (stop) return !stop;
		goto self;
	}


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
	if ((*plan)->initPlan)
		stop = !plan_list_traverse(pstmt, plan, (*plan)->initPlan, callback,
			decide_visit, context, true);
	if (stop) return !stop;

	List *qual_subplans = make_qual_subplan_list(pstmt, *plan);
	if (qual_subplans)
	{
		(*plan)->qual_subplans = qual_subplans;
		stop = !plan_list_traverse(pstmt, plan, qual_subplans, callback,
			decide_visit, context, true);
	}
	if (stop) return !stop;

	List *tl_subplans = make_tl_subplan_list(pstmt, *plan);
	if (tl_subplans)
	{
		(*plan)->tl_subplans = tl_subplans;
		stop = !plan_list_traverse(pstmt, plan, tl_subplans, callback,
			decide_visit, context, true);
	}
	if (stop) return !stop;

	// traverse *plan->qual
	stop = !callback(pstmt, parent_plan, plan, context);
	return !stop;
}

/*
  Note this isn't the same as ExecMaterializesOutput().
  Although plan's descendants' result rows are cached in 'plan', the execution
  of such descendants may still conflict, so they also must be materialized.
*/
static bool PlanSubtreeResultPrecached(Plan *plan)
{
	return false;

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
	ctx.mat_subplans = NULL;

	plan_tree_traverse(pstmt, NULL, &pstmt->planTree, handle_plan_traverse,
		visit_plan_node, &ctx);
	List *root_plans = NULL;
	Plan *root_plan = NULL;
	plan_list_traverse(pstmt, &root_plan, pstmt->subplans, handle_plan_traverse,
		visit_plan_node, &ctx, false);
	root_plans = lappend(root_plans, pstmt->planTree);
	materialize_plan_lists(pstmt->subplans, root_plans, OUTER_AND_INNER);
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

/*
  Materialize conflicting Plan nodes in 'plans', and
  'plans' comes from SubPlan nodes in plan->initPlan/qual/targetlist.
*/
static List*
materialize_subplan_list(PlannedStmt *pstmt, Plan *plan, List *plans,
	Mat_tree_remote_ctx *ctx)
{
	ListCell *lc = NULL;
	foreach(lc, plans)
	{
		Plan *outp = (Plan*)lfirst(lc);
		if (PlanSubtreeResultPrecached(outp)) continue;
		for (ShardRemoteScanRef *p = outp->shard_remotescan_refs; p; p = p->next)
		{
			ShardRemoteScanRef *q = NULL;
			if ((q = shard_used_in_following_sibling_plans(lc->next, p->shardid)))
			{
				materialize_remotescans(MAT_OUTER(ctx) ? p :
					(MAT_INNER(ctx) ? q : NULL), false);
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
	  other than the two below because remotescans don't do tidbitmap scan.
	  For Append's children, they may conflict because when one child's rows
	  is consumed, the remote connection&result isn't released, so next child
	  will get no results.

	  For MergeAppend&Append node, its children nodes are fetched from the
	  1st one to the last one, so for any child[i] (i in [0, N-1)),
	  if child[i+j] (j>=1, i+j<N) accesses a shard accessed by child[i], this
	  shard must be materialized.

	  For ModifyTable, the only possible issue is 'insert into ... select from ...',
	  where the data source tables may conflict, and source table accesses may
	  conflict with dest table accesses.
	*/
	ListCell *lc = NULL;

	if (plan->type == T_MergeAppend || plan->type == T_Append)
	{
		List *cplist = NULL;
		if (plan->type == T_Append) cplist = ((Append*)plan)->appendplans;
		else cplist = ((MergeAppend *)plan)->mergeplans;

		foreach(lc, cplist)
		{
			Plan *outp = (Plan*)lfirst(lc);
			if (PlanSubtreeResultPrecached(outp)) continue;
			for (ShardRemoteScanRef *p = outp->shard_remotescan_refs; p; p = p->next)
			{
				ShardRemoteScanRef *q = NULL;
				if ((q = shard_used_in_following_sibling_plans(lc->next, p->shardid)))
					materialize_remotescans(MAT_OUTER(ctx) ? p :
						(MAT_INNER(ctx) ? q : NULL), false);
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

static int materialize_plan(Plan *tgt_plan)
{
	int nmats = 0;
	for (ShardRemoteScanRef *p = tgt_plan->shard_remotescan_refs;
		 p; p = p->next)
	{
		materialize_remotescans(p, false);
		nmats++;
	}
	return nmats;
}

/*
  If any pair of plans from the two lists conflict, materialize selected ones
  as specified by 'bt', if bt is OUTER/INNER/OUTER_AND_INNER, materialize
  plans1/plans2/plans1&plans2, respectively.
  for the initPlan case there is no fixed order of execution among plan nodes in
  initPlan list and nodes in left/righttree or children list, and executions
  could be interleaved for parameterized situations.
*/
static void materialize_plan_lists(List *plans1, List *plans2, enum PlanBranchType bt)
{
	ListCell *lc1 = NULL;
	ListCell *lc2 = NULL;

	foreach(lc1, plans1)
	{
		foreach(lc2, plans2)
		{
			Plan *plan1 = lfirst(lc1);
			Plan *plan2 = lfirst(lc2);
			if (IsA(plan1, FunctionScan))
				materialize_plan(plan2);
			else if (IsA(plan2, FunctionScan))
				materialize_plan(plan1);
			else
			for (ShardRemoteScanRef *srsr1 = plan1->shard_remotescan_refs; srsr1;
				 srsr1 = srsr1->next)
			{
				for (ShardRemoteScanRef *srsr2 = plan2->shard_remotescan_refs; srsr2;
					 srsr2 = srsr2->next)
				{
					if (srsr1->shardid == srsr2->shardid)
					{
						if (bt == OUTER || bt == OUTER_AND_INNER)
							materialize_remotescans(srsr1, false);
						if (bt == INNER || bt == OUTER_AND_INNER)
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
	static ShardRemoteScanRef FunctionScanPseudoSRR = {0, NULL, NULL};

	for (ListCell *p = lc; p; p = p->next)
	{
		Plan *plan = (Plan*)lfirst(p);
		/*
		  A FunctionScan node conflicts with any node but we don't have valid
		  shard_remotescan_refs for it.
		*/
		if (IsA(plan, FunctionScan))
			return &FunctionScanPseudoSRR;
		if (PlanSubtreeResultPrecached(plan)) continue;
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
					p->rsrl = list_concat_unique_ptr(p->rsrl, q->rsrl);
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

typedef struct SubplanPickerCtx
{
	Plan *plan;
	PlannedStmt *pstmt;
	List *subplans;
} SubplanPickerCtx;

static bool subplan_picker(Node *node, SubplanPickerCtx*ctx)
{
    if (node == NULL) return false;

	SubPlan *splan = NULL;

	switch (nodeTag(node))
	{
	case T_SubPlan:
		splan = (SubPlan *)node;
		ctx->subplans = lappend(ctx->subplans, splan);
		break;
	default:
		return expression_tree_walker(node, subplan_picker, ctx);
		break;
	}
	return false;
}

/*
  Extract subplan nodes from plan->qual expr tree.
*/
static List *make_qual_subplan_list(PlannedStmt *pstmt, Plan *plan)
{
	SubplanPickerCtx ctx;
	ctx.plan = plan;
	ctx.subplans = NULL;
	ctx.pstmt = pstmt;
	expression_tree_walker((Node *)plan->qual, subplan_picker, &ctx);
	return ctx.subplans;
}

/*
  Extract subplan nodes from expr tree of each plan->targetlist node.
*/
static List *make_tl_subplan_list(PlannedStmt *pstmt, Plan *plan)
{
	ListCell *lc = NULL;
	if (!plan->targetlist) return NULL;

	SubplanPickerCtx ctx;
	ctx.plan = plan;
	ctx.subplans = NULL;
	ctx.pstmt = pstmt;
	foreach(lc, plan->targetlist)
	{
		TargetEntry *te = (TargetEntry *)lfirst(lc);
		expression_tree_walker((Node *)te->expr, subplan_picker, &ctx);
	}
	return ctx.subplans;
}
