/*-------------------------------------------------------------------------
 *
 * nodeRemotescan.c
 *	  Support routines for remote scans of relations.
 *
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeRemotescan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecRemoteScan				scans a remote SELECT SQL's result in its original order.
 *		ExecRemoteNext				retrieve next tuple in its original order.
 *		ExecInitRemoteScan			creates and initializes a seqscan node.
 *		ExecEndRemoteScan			releases any storage allocated.
 *		ExecReScanRemoteScan		rescans the relation
 *
 *		ExecRemoteScanEstimate		estimates DSM space needed for parallel scan
 *		ExecRemoteScanInitializeDSM initialize DSM for parallel scan
 *		ExecRemoteScanReInitializeDSM reinitialize DSM for fresh parallel scan
 *		ExecRemoteScanInitializeWorker attach to DSM info in parallel worker
 */
#include "postgres.h"

#include "access/remote_dml.h"
#include "executor/executor.h"
#include "access/relscan.h"
#include "executor/execdebug.h"
#include "executor/nodeRemotescan.h"
#include "executor/remoteScanUtils.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "nodes/remote_input.h"
#include "catalog/pg_enum.h"
#include "utils/builtins.h"
#include "catalog/pg_type.h"
#include "catalog/heap.h"
#include "parser/parsetree.h"
#include "miscadmin.h"

static TupleTableSlot *RemoteNext(RemoteScanState *node);
static void generate_remote_sql(RemoteScanState *rss);

/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		RemoteNext
 *
 *		This is a workhorse for ExecRemoteScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
RemoteNext(RemoteScanState *node)
{
	TupleTableSlot *slot;

	/*
	 * get information from the estate and scan state
	 */
	slot = node->ss.ss_ScanTupleSlot;

	/* Reach the EOF of tuples from connection */
	if (is_stmt_eof(node->handle))
	{
		ExecClearTuple(slot);
		return slot;
	}

	MemoryContext *saved =
         MemoryContextSwitchTo(node->ss.ps.ps_ExprContext->ecxt_per_tuple_memory);

	MYSQL_ROW mysql_row = get_stmt_next_row(node->handle);
	if (mysql_row)
	{
		size_t *lengths;
		enum enum_field_types *fieldtypes;
		
		lengths = get_stmt_row_lengths(node->handle);
		fieldtypes = get_stmt_field_types(node->handle);
		ExecStoreRemoteTuple(node->typeInputInfo,
				     mysql_row, /* tuple to store */
				     lengths,
				     fieldtypes,
				     slot); /* slot to store in */
	}
	else
	{
		Assert(is_stmt_eof(node->handle));
		ExecClearTuple(slot);
	}

	MemoryContextSwitchTo(saved);

	return slot;
}

/*
 * RemoteRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
RemoteRecheck(RemoteScanState *node, TupleTableSlot *slot)
{
	/*
	 * In a remotescan we never know whether remote source rows have changed
	 * or not, we always use our snapshot. For kunlun we use innodb mvcc so
	 * this assumption is always safe and correct.
	 */
	return true;
}

/* ----------------------------------------------------------------
 *		ExecRemoteScan(node)
 *
 *		Scans the relation sequentially and returns the next qualifying
 *		tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecRemoteScan(PlanState *pstate)
{
	RemoteScanState *node = castNode(RemoteScanState, pstate);
	Assert(node->asi);
	if (!node->asi)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: Internal error: Communication channel has been released.")));
	}


	if (!stmt_handle_valid(node->handle))
	{
		/*
		   1st row is to be returned from this remote table.
		   */
		size_t stmtlen = lengthStringInfo(&node->remote_sql);
		node->handle = send_stmt_async(node->asi, MemoryContextStrdup(TopTransactionContext, node->remote_sql.data),
					       stmtlen, CMD_SELECT, true, SQLCOM_SELECT, node->will_rewind);
	}

	return ExecScan(&node->ss,
					(ExecScanAccessMtd) RemoteNext,
					(ExecScanRecheckMtd) RemoteRecheck);
}

typedef struct FindParamChangeContext
{
	Bitmapset *chgParams;
	bool params_changed;
} FindParamChangeContext;

static bool dependent_params_changed(Node *node, FindParamChangeContext *pcc)
{
	if (IsA(node, Param))
	{
		Param *par = (Param *)node;
		if (bms_is_member(par->paramid, pcc->chgParams))
		{
			pcc->params_changed = true;
			return true;
		}
		return false;
	}
	return expression_tree_walker(node, dependent_params_changed, pcc);
}

typedef struct FindParamsContext
{
	bool has_rescan_params;
} FindParamsContext;

static bool has_dependent_params(Node *node, FindParamsContext*fpc)
{
	if (node == NULL)
		return false;
	if (IsA(node, Param))
	{
		Param *par = (Param *)node;
		Assert(par->paramtype != InvalidOid && par->paramid >= 0);
		fpc->has_rescan_params = true;
		return true;
	}
	return expression_tree_walker(node, has_dependent_params, fpc);
}
bool var_picker(Node *node, VarPickerCtx*ctx)
{
	if (node == NULL) return false;

	if (!IsA(node, Var))
	{
		switch (nodeTag(node))
		{
		case T_Const:
		case T_SQLValueFunction:
			ctx->local_evaluables++;
			break;
		case T_Param:
		case T_CaseTestExpr:
		case T_CoerceToDomainValue:
		case T_SetToDefault:
		case T_CurrentOfExpr:
		case T_NextValueExpr:
		case T_RangeTblRef:
		case T_SortGroupClause:
			/*
			 * keep traversing because Vars take precedence.
			 * */
			ctx->local_unevaluables++;
			break;
		default:
			return expression_tree_walker(node, var_picker, ctx);
			break;
		}

		return false;
	}

	Var *var = (Var *)node;
	if (ctx->check_alien_vars && var->varno != ctx->scanrelid)
	{
		ctx->has_alien_cols = true;
		return true;
	}

	if (ctx->target_cols == NULL)
	{
		ctx->target_cols = MemoryContextAllocZero(ctx->mctx, sizeof(void*) * 8);
		ctx->nvar_buf = 8;
		ctx->nvars = 0;
	}

	if (ctx->nvars == ctx->nvar_buf)
	{
		ctx->target_cols = repalloc(ctx->target_cols, sizeof(void*) * ctx->nvar_buf * 2);
		memset(ctx->target_cols + ctx->nvar_buf, 0, sizeof(void*) * ctx->nvar_buf);
		ctx->nvar_buf *= 2;
	}

	ctx->target_cols[ctx->nvars++] = var;
	return false;
}

/*
 * Double the NO. of attrs in tpd by duplicating it. tpd is then pfree'd.
 * */
TupleDesc expandTupleDesc2(TupleDesc tpd)
{
	int nadd = tpd->natts < 4 ? 4 : tpd->natts;

	TupleDesc td1 = CreateTemplateTupleDesc(tpd->natts + nadd, tpd->tdhasoid);
	size_t oldlen = offsetof(struct tupleDesc, attrs) + tpd->natts * sizeof(FormData_pg_attribute);
	memcpy(td1, tpd, oldlen);

	// Note there is a header before the FormData_pg_attribute array.
	memset(((char*)td1) + oldlen, 0, nadd * sizeof(FormData_pg_attribute));
	td1->natts = tpd->natts + nadd;
	pfree(tpd);

	return td1;
}

void reset_var_picker_ctx(VarPickerCtx *vpc)
{
	vpc->nvars = 0;
	vpc->has_alien_cols = false;
	vpc->local_evaluables = 0;
	vpc->local_unevaluables = 0;
	vpc->scanrelid = 0;
	vpc->check_alien_vars = false;
}

void validate_column_reference(Var *colvar, Relation rel)
{
	if (colvar->varattno < 0)
	{
		Form_pg_attribute sysatt = SystemAttributeDefinition(colvar->varattno, true);

		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Kunlun-db: Can't access system attribute(%s) from remote relation %s.",
				 		sysatt ? sysatt->attname.data : "<unknown>",
						rel->rd_rel->relname.data)));
	}

	if (colvar->varattno > rel->rd_att->natts)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: Invalid column(%d) referenced for relation %s, exceeds max column number(%d).",
						colvar->varattno, rel->rd_rel->relname.data,
						rel->rd_att->natts)));
}

/*
 * TargetEntries with expr of T_SQLValueFunction and T_Const can be processed,
 * they don't need to be sent to remote and they can be evaluated.
 * */
static TupleDesc
ExecScanTypeFromTL(EState *estate, RemoteScanState *rss,
	Relation rel, bool skipjunk)
{
	Plan *plan = rss->ss.ps.plan;
	List *targetList = plan->targetlist;
	ListCell   *l;

	ScanTupleGenContext context;
	InitScanTupleGenContext(&context, (PlanState*)rss, skipjunk);
	/*
	 * Alloc scantuples for the target list.
	 * If the target item can be pushed down as a whole, a scanvar is assigned to it;
	 * Otherwise, split the target entry into multiple sub-expressions/columns that
	 * can be pushed down, and assign scanvar to them.
	 */
	int tgtidx = 0;

	foreach(l, targetList)
	{
		tgtidx++;
		TargetEntry *tle = lfirst_node(TargetEntry, l);
		if ((skipjunk && tle->resjunk) || !tle->expr)
			continue;
		(void) alloc_scanvar_for_expr(&context, tle->expr);
	}

	/*
	 * Alloc scantuples for quals which cannot be pushed down
	 */
	StringInfoData buff;
	initStringInfo2(&buff, 256, estate->es_query_cxt);
	List *qual = rss->ss.ps.plan->qual;
	List *local_quals = NIL;
	foreach (l, qual)
	{
		Expr *expr = (Expr*)lfirst(l);
		if (snprint_expr(&buff, expr, &context.rpec) >= 0)
		{
			rss->quals_pushdown = lappend(rss->quals_pushdown, expr);
			continue;
		}
		alloc_scanvar_for_expr(&context, expr);
		local_quals = lappend(local_quals, copyObject(expr));
		resetStringInfo(&buff);
	}

	/*
	 * Note down the real capacity, but also make sure typeInfo->natts is the
	 * number valid attrs. The original design of TupleDesc facility didn't
	 * expect dynamically growth of attrs, hence the complexity.
	 * */
	rss->scandesc_natts_cap = context.tupledesc->natts;

	/*
	 * There is no target in targetlist, this can happen in this particular case:
	 *      select count(*) from t1;
	 * where t1 satisfies both conditions: 
	 * 1. a partitioned table and 
	 * 2. there is no reference of any of t1's columns in any clause of the
	 * SELECT stmt.
	 *
	 * If there is one such reference, e.g:
	 *      select count(*) from t1 group by t1.a;
	 * then the targetlist won't be NIL. In this case we can find the shortest
	 * column from the target relation as target.
	 * */
	if (list_length(context.vars) == 0)
	{
		TupleDescInitEntry(context.tupledesc,
				1, // 1
				"3", // 3 produces everything!
				23, // int4
				-1, // N/A
				0);
		context.tupledesc->natts = 1;
	}
	else
	{
		context.tupledesc->natts = list_length(context.vars);
	}


	/*
	 * Replace target entries and quals that cannot be pushed down with the
	 * new scantuple var
	 */
	foreach(l, targetList)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, l);
		tle->expr = (Expr*)replace_expr_with_scanvar_mutator((Node*)tle->expr, &context);
	}

	List *local_quals_new = NIL;
	foreach(l, local_quals)
	{
		Expr *expr = (Expr*)lfirst(l);
		local_quals_new = lappend(local_quals_new,
				replace_expr_with_scanvar_mutator((Node*)expr, &context));
	}

	List *unpushed_exprs_new = NIL;
	foreach(l, context.unpushable_exprs)
	{
		Expr *expr = (Expr *)lfirst(l);
		unpushed_exprs_new = lappend(unpushed_exprs_new,
								 replace_expr_with_scanvar_mutator((Node *)expr, &context));
	}

	/* Initalize the ExprState with rewrited quals */
	rss->ss.ps.qual = ExecInitQual(local_quals_new, (PlanState *) &rss->ss.ps);

	/* Save the remaining quals for explain */
	rss->orignal_qual = rss->ss.ps.plan->qual;
	rss->ss.ps.plan->qual = local_quals;

	/*
	 * generate_remote_sql() must be called before the remote result's
	 * TupleTableSlot is made by our caller, and for generate_remote_sql() to
	 * work we must set rss->ss.ps.scandesc first.

	 But if param driven, right here we have no param yet so can't generate a
	 remote query string.
	 * */
	rss->scanvars = context.vars;
	rss->scanexprs = context.exprs;
	rss->ss.ps.scandesc = context.tupledesc;
	rss->unpushable_tl = unpushed_exprs_new;

	initStringInfo2(&rss->remote_sql, 512, estate->es_query_cxt);
	if (!rss->param_driven) generate_remote_sql(rss);

	return rss->ss.ps.scandesc;
}

static bool
contain_param_exec(Plan *plan)
{
	FindParamsContext fpc;
	fpc.has_rescan_params = false;
	if (plan->extParam)
	{
		ListCell *lc;
		foreach (lc, plan->qual)
		{
			expression_tree_walker((Node *)lfirst(lc), has_dependent_params, &fpc);
			if (fpc.has_rescan_params)
				break;
		}
	}
	return fpc.has_rescan_params;
}

bool IsRemoteScanTotallyPushdown(RemoteScanState *rss, List *unused_tl)
{
	if (rss->param_driven || list_length(rss->ss.ps.plan->qual) > 0)
		return false;
	ListCell *lc1, *lc2;
	foreach (lc1, rss->unpushable_tl)
	{
		bool found = false;
		Expr *expr = lfirst(lc1);
		foreach (lc2, unused_tl)
		{
			if ((found = equal(expr, lfirst(lc2))))
				break;
		}
		if (!found)
			return false;
	}

	return true;
}


void init_type_input_info(TypeInputInfo **tii, TupleTableSlot *slot,
	EState *estate)
{
	int         natts = slot->tts_tupleDescriptor->natts;

	*tii = MemoryContextAllocZero(estate->es_query_cxt,
			sizeof(TypeInputInfo) * natts);
	(*tii)->mctx = estate->es_query_cxt;

	for (int i = 0; i < natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(slot->tts_tupleDescriptor, i);

		if (att->attisdropped)
			continue;

		myInputInfo(att->atttypid, att->atttypmod, *tii + i);
	}
}


int remote_param_fetch_threshold = 1024*1024*256;
static bool decide_remote_scan_param_driven(RemoteScan *rs)
{
	Plan *plan = (Plan*)rs;
	const size_t estimated_rsize = plan->plan_rows*plan->plan_width;
	FindParamsContext fpc;
	fpc.has_rescan_params = false;

	/*
	  This plan node may use params from other nodes but it doesn't depend on
	  params for rescan.
	*/
	if (!rs->plan.extParam)
		goto end;

	ListCell *lc;
	foreach(lc, rs->plan.qual)
	{
		expression_tree_walker((Node*)lfirst(lc), has_dependent_params, &fpc);
		if (fpc.has_rescan_params)
			break;
	}
end:
	return fpc.has_rescan_params &&
		   (estimated_rsize > mysql_max_packet_size ||
		    estimated_rsize > MaxAllocSize ||
		    estimated_rsize > remote_param_fetch_threshold);
}

/* ----------------------------------------------------------------
 *		ExecInitRemoteScan
 * ----------------------------------------------------------------
 */
RemoteScanState *
ExecInitRemoteScan(RemoteScan *node, EState *estate, int eflags)
{
	RemoteScanState *scanstate;

	/* Make a copy of the plan */
	node = copyObject(node);
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	scanstate = makeNode(RemoteScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;
	scanstate->ss.ps.ExecProcNode = ExecRemoteScan;
	scanstate->typeInputInfo = NULL;
	scanstate->long_exprs = NULL;
	scanstate->long_exprs_bmp = NULL;
	scanstate->check_exists = node->check_exists;
	scanstate->handle = INVALID_STMT_HANLE;

	scanstate->param_driven = decide_remote_scan_param_driven(node);
	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ss.ps);

	/*
	 * Initialize scan relation.
	 *
	 * Get the relation object id from the relid'th entry in the range table,
	 * open that relation and acquire appropriate lock on it.
	 */
	Relation rel = scanstate->ss.ss_currentRelation =
		ExecOpenScanRelation(estate,
							 node->scanrelid,
							 eflags);

	if (eflags & EXEC_FLAG_REMOTE_FETCH_NO_DATA)
	{
		scanstate->fetches_remote_data = false;
	    return scanstate;
	}

	if (!(eflags & EXEC_FLAG_EXPLAIN_ONLY))
		scanstate->fetches_remote_data = true;

	/*
	 * We only fetch target columns, this can save a lot of unneeded data
	 * transfer sometimes. Sometimes the columns used in 'order by' clause
	 * is marked as 'resjunk' if it is not in target list, but we do need
	 * such columns, so we never skip junk.
	 */
	ExecInitScanTupleSlot(estate, &scanstate->ss,
		ExecScanTypeFromTL(estate, scanstate, rel, false));

	/*
	 * Initialize result slot, type and projection.
	 */
	ExecInitResultTupleSlotTL(estate, &scanstate->ss.ps);

	/*
	 * Init the mapping from source data to target list.
	*/
	ExecAssignScanProjectionInfo(&scanstate->ss);

	/*
	  In EXPLAIN stmt, other nodes expect the scan type objects including
	  tupledesc, targetlist, etc. so we have to do above.
	*/
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		goto end;

	init_type_input_info(&scanstate->typeInputInfo,
		scanstate->ss.ss_ScanTupleSlot, estate);

	scanstate->asi = GetAsyncStmtInfo(rel->rd_rel->relshardid);

end:
	/*
	  build_subplan() doesn't add EXEC_FLAG_REWIND for subplans having params,
	  don't modify the generic logic there. for remote scans as long as the
	  node is not top level, assume rewinding possible.
	*/
	if (eflags & EXEC_FLAG_REWIND || contain_param_exec((Plan*)node))
		scanstate->will_rewind = true;
	else
		scanstate->will_rewind = false;

	return scanstate;
}

/* ----------------------------------------------------------------
 *		ExecEndRemoteScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndRemoteScan(RemoteScanState *node)
{
	Relation	relation;

	/*
	 * get information from node
	 */
	relation = node->ss.ss_currentRelation;

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	if (node->fetches_remote_data == false)
		goto end;

	 if (stmt_handle_valid(node->handle))
	 {
		 /* Cancel the statement, no longer need it. */
		 cancel_stmt_async(node->handle);
		 release_stmt_handle(node->handle);
		 node->handle = INVALID_STMT_HANLE;
	 }

	int natts = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor->natts;

	for (int i = 0; i < natts && node->typeInputInfo; i++)
	{
		EnumLabelOid *pelo = 
			node->typeInputInfo[i].enum_label_enties;
		if (pelo)
			pfree(pelo);
	}

	if (node->typeInputInfo)
		pfree(node->typeInputInfo);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);
end:
	/*
	 * close the heap relation.
	 */
	ExecCloseScanRelation(relation);
}

/* ----------------------------------------------------------------
 *						Join Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecReScanRemoteScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanRemoteScan(RemoteScanState *node)
{
	/*
	   This node is inner rel of a join, it's rescanned multiple times,
	   once for each row in outer node's result set.

	   If no changes for dependent params or no dependent params at all, simply
	   rewind by seeking to start of result set.
	   */

	bool reuse_previous = bms_is_empty(node->ss.ps.chgParam) || !node->param_driven;
	if (!reuse_previous)
	{
		FindParamChangeContext pcc;
		pcc.chgParams = node->ss.ps.chgParam;
		pcc.params_changed = false;
		ListCell *lc;
		foreach(lc, node->ss.ps.plan->qual)
		{
			expression_tree_walker((Node*)lfirst(lc), dependent_params_changed, &pcc);
			if (pcc.params_changed)
				break;
		}
		reuse_previous = !pcc.params_changed;
	}

	/*
	 * We need regenerate the tuples when:
	 * (1) parameters changed
	 * (2) some tuples are missing
	 * (3) tuplestore not initialized
	 */
	bool regenerate = !reuse_previous || !stmt_handle_valid(node->handle) || !is_stmt_rewindable(node->handle);

	/* Check if the intial value of will_rewind is correct */
	if (!node->will_rewind)
		node->will_rewind = true;

	if (!regenerate)
	{
		Assert(stmt_handle_valid(node->handle));
		stmt_rewind(node->handle);
	}
	else
	{
		if (stmt_handle_valid(node->handle))
		{
			cancel_stmt_async(node->handle);
			release_stmt_handle(node->handle);
			node->handle = INVALID_STMT_HANLE;
		}

		initStringInfo2(&node->remote_sql, 512, node->ss.ps.state->es_query_cxt);
		generate_remote_sql(node);

	}

	ExecScanReScan((ScanState *) node);
}

/* ----------------------------------------------------------------
 *						Parallel Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecRemoteScanEstimate
 *
 *		Compute the amount of space we'll need in the parallel
 *		query DSM, and inform pcxt->estimator about our needs.
 * ----------------------------------------------------------------
 */
void
ExecRemoteScanEstimate(RemoteScanState *node,
					ParallelContext *pcxt)
{
	Assert(false);
}

/* ----------------------------------------------------------------
 *		ExecRemoteScanInitializeDSM
 *
 *		Set up a parallel heap scan descriptor.
 * ----------------------------------------------------------------
 */
void
ExecRemoteScanInitializeDSM(RemoteScanState *node,
						 ParallelContext *pcxt)
{
	Assert(false);
}

/* ----------------------------------------------------------------
 *		ExecRemoteScanReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void
ExecRemoteScanReInitializeDSM(RemoteScanState *node,
						   ParallelContext *pcxt)
{
	Assert(false);
}

/* ----------------------------------------------------------------
 *		ExecRemoteScanInitializeWorker
 *
 *		Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
void
ExecRemoteScanInitializeWorker(RemoteScanState *node,
							ParallelWorkerContext *pwcxt)
{
	Assert(false);
}

static void generate_remote_sql(RemoteScanState *rss)
{
	StringInfo str = &rss->remote_sql;
	PlannedStmt *pstmt = ((PlanState *)rss)->state->es_plannedstmt;
	Relation rel = rss->ss.ss_currentRelation;
	RemoteScan *rs = (RemoteScan *)rss->ss.ps.plan;
	List *qual = rs->plan.qual;
	ListCell *lc;
	
	RemotePrintExprContext rpec;
	InitRemotePrintExprContext(&rpec, rss->ss.ps.state->es_plannedstmt->rtable);
	rpec.exec_param_quals = rss->param_driven;
	rpec.estate = ((PlanState *)rss)->state;
	rpec.rpec_param_exec_vals = rss->ss.ps.ps_ExprContext->ecxt_param_exec_vals;

	/*
	 * SELECT target index.
	 * */
	int ntgts = 0;
	/* SELECT scan exprs */
	List *scanexprs = rss->scanexprs;
	foreach (lc, scanexprs)
	{
		Expr *expr = (Expr*)lfirst(lc);
		if (ntgts == 0)
			appendStringInfo(str, "select ");
		else
			appendStringInfo(str, ", ");
		if (snprint_expr(str, expr, &rpec) <= 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Kunlun-db: The generated scan expression cannot be pushed down")));
		}
		++ ntgts;
	}
	if (list_length(scanexprs) == 0)
	{
		appendStringInfo(str, "select 3 ");
	}

	const char* table_qname =
		make_qualified_name(rel->rd_rel->relnamespace,
			rel->rd_rel->relname.data, NULL);

	appendStringInfo(str, " from %s ", table_qname);
	
	ntgts = 0;
	foreach(lc, rss->quals_pushdown)
	{
		if (ntgts > 0)
			appendStringInfoString(str, " AND ");
		else
			appendStringInfoString(str, " where ");
		snprint_expr(str, (Expr*)lfirst(lc), &rpec);
		++ ntgts;
	}

	if (rss->check_exists)
		appendStringInfoString(str, " limit 1");

	/* print locks */
	foreach(lc, pstmt->rowMarks)
	{
		PlanRowMark *rc = lfirst_node(PlanRowMark, lc);
		if (rc->rti == rs->scanrelid && rc->strength != LCS_NONE)
		{
			if (rc->strength == LCS_FORUPDATE || rc->strength == LCS_FORNOKEYUPDATE)
			{
				appendStringInfo(str, " FOR UPDATE ");
			}
			else if (rc->strength == LCS_FORSHARE || rc->strength == LCS_FORNOKEYUPDATE)
			{
				appendStringInfo(str, " FOR SHARE ");
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Kunlun-db: Remote shard not support such lock mode.")));
			}

			if (rc->waitPolicy == LockWaitSkip)
			{
				appendStringInfo(str, "SKIP LOCKED");
			}
			else if (rc->waitPolicy == LockWaitError)
			{
				appendStringInfo(str, "NOWAIT");
			}

			break;
		}
	}
}

void ExecStoreRemoteTuple(TypeInputInfo *tii, MYSQL_ROW row,
			  unsigned long *lengths, enum enum_field_types *types, TupleTableSlot *slot)
{
	ExecClearTuple(slot);
	int         natts = slot->tts_tupleDescriptor->natts;
	char *pfld = NULL;

	Assert(tii != NULL);

	for (int i = 0; i < natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(slot->tts_tupleDescriptor, i);

		if (!att->attisdropped && row[i] != NULL)
		{
			bool isnull;
			TypeInputInfo *ptii = tii + i;

			slot->tts_values[i] = myInputFuncCall(ptii, row[i], lengths[i], types[i], &isnull);
			slot->tts_isnull[i] = isnull;
		}
		else
		{
			slot->tts_values[i] = (Datum) 0;
			slot->tts_isnull[i] = true;
		}
	}
	ExecStoreVirtualTuple(slot);
}
