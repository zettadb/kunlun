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
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "catalog/pg_enum.h"
#include "utils/builtins.h"
#include "catalog/pg_type.h"
#include "catalog/heap.h"
#include "parser/parsetree.h"

static TupleTableSlot *RemoteNext(RemoteScanState *node);
static void generate_remote_sql(RemoteScanState *rss);
static void add_qual_cols_to_src(RemoteScanState *rss, Relation rel,
	Expr* expr, StringInfo str);


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
	
	if (!node->asi->mysql_res && node->asi->result_pending)
	{
		/*
		  This happens after a ReScan which really regenerated&resent queries
		  because of param changes.
		*/
		send_multi_stmts_to_multi();
	}

	Assert(node->asi->mysql_res);

	MYSQL_ROW mysql_row = mysql_fetch_row(node->asi->mysql_res);
	unsigned long *lengths = ((mysql_row && node->asi->mysql_res) ?
		mysql_fetch_lengths(node->asi->mysql_res) : NULL);

	if (mysql_row)
		ExecStoreRemoteTuple(node->typeInputInfo, mysql_row,	/* tuple to store */
					         lengths, slot);	/* slot to store in */
	else
	{
		check_mysql_fetch_row_status(node->asi);
		ExecClearTuple(slot);

		/*
		 * Release mysql result from channel if not gonna rewind, in order to
		 * send next stmt to remote storage node. If scanning a partitioned
		 * table, this result must be freed here otherwise the same channel
		 * can't work on next stmt to fetch data from next table of the same
		 * shard.
		 * */
		if (!node->will_rewind)
			free_mysql_result(node->asi);
	}
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

	if (!node->asi->mysql_res && !node->asi->result_pending)
	{
		/*
		  1st row is to be returned from this remote table.
		*/
		size_t stmtlen = lengthStringInfo(&node->remote_sql);
	    append_async_stmt(node->asi, pstrdup(node->remote_sql.data),
	        stmtlen, CMD_SELECT, true, SQLCOM_SELECT);
		int rc = work_on_next_stmt(node->asi);
	    if (rc == 1)
		{
			send_stmt_to_multi_start(node->asi, 1);
		}
		else if (rc < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Kunlun-db: Internal error: mysql result has not been consumed yet.")));
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
	if (var->varno != ctx->scanrelid)
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
	TupleDesc	typeInfo;
	ListCell   *l;
	int			len;
	int			cur_resno = 1;
	char *colname = NULL;
	RemoteScan *rs = (RemoteScan *)plan;

	if (skipjunk)
	   len = ExecCleanTargetListLength(targetList);
	else
		len = ExecTargetListLength(targetList);
	if (len < 3)
	{
		// Alloc enough; will append a column at the end of the func.
		len = 3;
	}

	typeInfo = CreateTemplateTupleDesc(len, false /*hasoid*/);

	StringInfoData colexpr_str;
	RemotePrintExprContext rpec;
	InitRemotePrintExprContext(&rpec, estate->es_plannedstmt->rtable);
	rpec.ignore_param_quals = !rss->param_driven;
	rpec.rpec_param_exec_vals = (rss->ss.ps.ps_ExprContext ?
		rss->ss.ps.ps_ExprContext->ecxt_param_exec_vals : NULL);
	rpec.rpec_param_list_info = estate->es_param_list_info;

	VarPickerCtx vpc;
	memset(&vpc, 0, sizeof(vpc));
	vpc.scanrelid = rs->scanrelid;
	vpc.mctx = estate->es_query_cxt;

	foreach(l, targetList)
	{
		if (cur_resno > typeInfo->natts)
			typeInfo = expandTupleDesc2(typeInfo);

		Node *pnode = lfirst(l);
		Assert(IsA(pnode, TargetEntry));
		TargetEntry *tle = (TargetEntry *)pnode;

		if ((skipjunk && tle->resjunk) || !tle->expr)
			continue;
		/*
		 * Try to make a column name in order to build the result's tupledesc.
		 * */
		if (IsA(tle->expr, Var))
		{
			/*
			 * Fast path for most common case.
			 * */
			Var *colvar = (Var*)(tle->expr);
			validate_column_reference(colvar, rel);

			if (colvar->varattno == 0)
			{
				cur_resno = append_cols_for_whole_var(rel, &typeInfo, cur_resno);
				continue; // whole-row var processed.
			}

			cur_resno = add_unique_col_var(rel, typeInfo, cur_resno, tle, colvar, true);
		}
		else
		{
			reset_var_picker_ctx(&vpc);

			if (IsA(tle->expr, Const))
				vpc.local_evaluables++;
			else
			{
				expression_tree_walker((Node *)tle->expr, var_picker, &vpc);
			}

			if (vpc.has_alien_cols)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Kunlun-db: Columns of other tables can't be in a base table scan.")));
			}

			if (vpc.nvars == 0)
			{
				continue;// no target column to add to the remote SELECT stmt.
			}

			Assert(vpc.nvars > 0);
			/*
			 * There can be other types of nodes, OpExpr, FuncExpr, etc, for
			 * them we simply send the expr string to remote. So non-SQL-standard
			 * exprs will return error from remote node.
			 * */

			/* Whether column def added to typeInfo. */
			initStringInfo2(&colexpr_str, 256, estate->es_query_cxt);
			int seret = snprint_expr(&colexpr_str, tle->expr, &rpec);

			if (seret >= 0 && rpec.num_vals == 1)
			{
				/*
				  The expression can be computed by storage shard, so simply
				  use returned field value, i.e. replace the expr with a Var
				  to refer to the source column.
				  The expr can be serialized to one value, and this value has
				  exact same type as the expr itself. Otherwise we can't do this.
				*/
				Var *var_expr = makeVar(rs->scanrelid, cur_resno,
					exprType((Node *)tle->expr), exprTypmod((Node *)tle->expr),
					exprCollation((Node *)tle->expr), 0);
				tle->expr = (Expr*)var_expr;

				if (lengthStringInfo(&colexpr_str) >= sizeof(NameData))
				{
					rss->long_exprs_bmp = bms_add_member(rss->long_exprs_bmp, cur_resno);
					rss->long_exprs = lappend(rss->long_exprs, pstrdup(colexpr_str.data));
					colexpr_str.data[sizeof(NameData) - 1] = '\0';
				}
				colname = donateStringInfo(&colexpr_str);
				var_expr->varattno = cur_resno;
				/*
				 * Hold the target var name or expr string to generate remote sql.
				 * */
				if (!tle->resname)
					tle->resname = colname;
				TupleDescInitEntry(typeInfo,
								   cur_resno,
								   colname,
								   exprType((Node *) tle->expr),
								   exprTypmod((Node *) tle->expr),
								   0);
				TupleDescInitEntryCollation(typeInfo,
											cur_resno,
											exprCollation((Node *) tle->expr));
				cur_resno++;
			}
			else
			{
				/*
				  The expr has to be computed in computing node, the columns
				  involved has already been extracted, request their field
				  values instead.
				*/
				resetStringInfo(&colexpr_str);
				for (int i = 0; i < vpc.nvars; i++)
				{
					if (cur_resno > typeInfo->natts)
						typeInfo = expandTupleDesc2(typeInfo);
					Var *origvar = vpc.target_cols[i];
					validate_column_reference(origvar, rel);

					if (unlikely(origvar->varattno == 0))
					{
						cur_resno = append_cols_for_whole_var(rel,
							&typeInfo, cur_resno);
						continue; // whole-row var processed.
					}

					cur_resno = add_unique_col_var(rel, typeInfo, cur_resno, tle,
						origvar, false);
				} // FOR vars
			} // ELSE (expr can not be pushed down)
		} // ELSE(not a simple var)
	}

	/*
	 * Note down the real capacity, but also make sure typeInfo->natts is the
	 * number valid attrs. The original design of TupleDesc facility didn't
	 * expect dynamically growth of attrs, hence the complexity.
	 * */
	rss->scandesc_natts_cap = typeInfo->natts;
	if (cur_resno > 1) typeInfo->natts = cur_resno - 1;

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
	if (cur_resno == 1)
	{
		TupleDesc tdrel = rel->rd_att;
		int target_attidx = -1, target_attw = 0x7fffffff;
		Form_pg_attribute atts = tdrel->attrs;

		for (int i = 0; i < tdrel->natts; i++)
		{
			int attw = atts[i].attlen;
			if (attw < 0)
				attw = atts[i].atttypmod;
			if (attw < target_attw)
			{
				target_attw = attw;
				target_attidx = i;
			}
		}

		/* 
		 * Append column to the target TupleDesc. All we need is one column
		 * name, do not append to targetlist in case other parts don't expect
		 * it to exist in this particular case.
		 */
		colname = atts[target_attidx].attname.data;
		TupleDescInitEntry(typeInfo,
						   cur_resno, // 1
						   colname,
						   atts[target_attidx].atttypid,
						   atts[target_attidx].atttypmod,
						   0);
		typeInfo->natts = 1;
	}

	/*
	 * generate_remote_sql() must be called before the remote result's
	 * TupleTableSlot is made by our caller, and for generate_remote_sql() to
	 * work we must set rss->ss.ps.scandesc first.

	 But if param driven, right here we have no param yet so can't generate a
	 remote query string.
	 * */
	rss->ss.ps.scandesc = typeInfo;
	initStringInfo2(&rss->remote_sql, 512, estate->es_query_cxt);
	if (!rss->param_driven) generate_remote_sql(rss);
	typeInfo = rss->ss.ps.scandesc;

	return typeInfo;
}

/*
  Don't fetch the same column twice which may happen simply because it's
  referenced twice in the sql text.
*/
int add_unique_col_var(Relation rel, TupleDesc typeInfo, int cur_resno, TargetEntry *tle,
	Var *colvar, bool set_tle_resname)
{
	char *colname = rel->rd_att->attrs[colvar->varattno - 1].attname.data;
	// avoid duplicates
	bool coladded = false;
	for (int x = 0; x < cur_resno - 1; x++)
	{
		if (strcmp(colname, typeInfo->attrs[x].attname.data) == 0)
		{
			colvar->varattno = x+1;
			coladded = true;
			break;
		}
	}
	if (coladded) return cur_resno;

	/*
	  Establish mapping from the source data columns to the targetlist's
	  columns of this plan node, so that projection can be computed correctly.
	*/
	colvar->varattno = cur_resno;
	/*
	 * Hold the target var name or expr string to generate remote sql.
	 * */
	if (!tle->resname && set_tle_resname)
		tle->resname = colname;
	TupleDescInitEntry(typeInfo,
					   cur_resno,
					   colname,
					   colvar->vartype,
					   colvar->vartypmod,
					   0);
	TupleDescInitEntryCollation(typeInfo,
								cur_resno,
								colvar->varcollid);
	cur_resno++;
	return cur_resno;
}

/*
  whole-row var, append all columns, but don't add a column twice.
  the left N (N is rel->rd_att->natts) targets must be exactly rel's columns
  otherwise record_out() fails to work because it assumes the whole-row type
  is the source table type.
  We could rearrange remote data source targets but for now we don't bother to
  handle such a corner case, the trouble is that we would have to re-wire the
  mapping between tle->expr(var) to the target column of remote query.
*/
int append_cols_for_whole_var(Relation rel, TupleDesc *pp_typeInfo, int cur_resno)
{
	char *colname = NULL;
	int num_vars = cur_resno - 1;
	TupleDesc typeInfo = *pp_typeInfo;
	for (int k = 0; k < rel->rd_att->natts; k++)
	{
		FormData_pg_attribute *patt = TupleDescAttr(rel->rd_att, k);
		colname = patt->attname.data;

		// avoid duplication
		bool var_added = false;
		for (int x = 0; x < num_vars; x++)
		{
			if (strcmp(colname, typeInfo->attrs[x].attname.data) == 0)
			{
				if (x == k)
				{
					var_added = true;
					break;
				}
				else
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Kunlun-db: Must specify whole-row target left-most.")));
			}
		}
		if (var_added) continue;

		if (cur_resno > typeInfo->natts)
		{
			typeInfo = expandTupleDesc2(typeInfo);
			*pp_typeInfo = typeInfo;
		}

		TupleDescInitEntry(typeInfo,
						   cur_resno,
						   colname,
						   patt->atttypid,
						   patt->atttypmod,
						   0);
		TupleDescInitEntryCollation(typeInfo,
									cur_resno,
									patt->attcollation);
		cur_resno++;
	}

	return cur_resno;
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

		Oid			typinput;
		Oid			typioparam;
		TypeInputInfo *ptii = *tii + i;

		getTypeInputInfo(att->atttypid, &typinput, &typioparam);
		ptii->typinput = typinput;
		ptii->typioparam = typioparam;
		ptii->typisenum = type_is_enum_lite(att->atttypid);
		if (ptii->typisenum)
		{
		    ptii->AllEnumLabelOidEntries =
		        GetAllEnumValueOidLabelSorted(att->atttypid, &ptii->nslots);
		}
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
	
	/*
	  build_subplan() doesn't add EXEC_FLAG_REWIND for subplans having params,
	  don't modify the generic logic there. for remote scans as long as the
	  node is not top level, assume rewinding possible.
	*/
	if (eflags & EXEC_FLAG_REWIND)
		scanstate->will_rewind = true;
	else
		scanstate->will_rewind = false;

end:
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

	release_shard_conn(node);

	int natts = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor->natts;

	for (int i = 0; i < natts && node->typeInputInfo; i++)
	{
		EnumLabelOid *pelo = 
			node->typeInputInfo[i].AllEnumLabelOidEntries;
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

void release_shard_conn(RemoteScanState *node)
{
	if (node->fetches_remote_data == false)
		return;

	free_mysql_result(node->asi);
	cleanup_asi_work_queue(node->asi);
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

	if (!node->asi->mysql_res)
	{
		if (node->asi->result_pending)
		{
			Assert(bms_is_empty(node->ss.ps.chgParam));
			//send_multi_stmts_to_multi(); done in RemoteNext().
		}
		else
		{
			/*
			  Can't generate remote sql or send to remote for param-driven
			  nodes, hence no mysql result to use here. Simply generate&send
			  result. This happens at 1st rescan call after this node is
			  created.
			*/
			if (node->param_driven) goto genres;
			else goto end;
		}
	}

	/*
	  It's expensive to rescan remotely, try best to avoid unneeded rescans.
	  node->ss.ps.chgParam here is already well set by executor in
	  UpdateChangedParamSet() to indicate
	  changed params dependent by this node.
	*/
	if (bms_is_empty(node->ss.ps.chgParam) || !node->param_driven)
	{
		mysql_data_seek(node->asi->mysql_res, 0);
		goto end;
	}

	/*
	  Check whether the param driven RemoteScanState node's qual really has
	  some changed params, only refresh result set if so.
	*/
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
	if (!pcc.params_changed)
	{
		mysql_data_seek(node->asi->mysql_res, 0);
		goto end;
	}

	// dependent params have changed, need to regenerate & resend new query
	// string and get new inner result.

	if (node->asi->mysql_res)
		free_mysql_result(node->asi);
	Assert(node->asi->mysql_res == NULL);
genres:
	initStringInfo2(&node->remote_sql, 512, node->ss.ps.state->es_query_cxt);

	generate_remote_sql(node);
	size_t stmtlen = lengthStringInfo(&node->remote_sql);
	append_async_stmt(node->asi, pstrdup(node->remote_sql.data),
					  stmtlen, CMD_SELECT, true, SQLCOM_SELECT);

	int rc = work_on_next_stmt(node->asi);
	if (rc == 1)
	{
		send_stmt_to_multi_start(node->asi, 1);
	}
	else if (rc < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: Internal error: mysql result has not been consumed yet.")));

end:
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
	Relation rel = rss->ss.ss_currentRelation;
	RemoteScan *rs = (RemoteScan *)rss->ss.ps.plan;
	List *qual = rs->plan.qual;
	ListCell *lc;
	/*
	 * SELECT target index.
	 * */
	int ntgts = 0;
	int slen = 0;
	TupleDesc tupdesc = rss->ss.ps.scandesc;
	ListCell *next_long_expr = NULL;

	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = tupdesc->attrs+i;

		/*
		 * The remote target column expression to append to the remote SELECT
		 * stmt. Normally it's a column name, but may also be an expression
		 * containing one or more column names.
		 * */
		const char *colname = attr->attname.data;

		if (!colname || !colname[0])
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Kunlun-db: Invalid target column name(NULL).")));

		if (bms_is_member(i+1, rss->long_exprs_bmp))
		{
			next_long_expr = (next_long_expr ? lnext(next_long_expr) : list_head(rss->long_exprs));
			if (next_long_expr == NULL)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Kunlun-db: Invalid target column long name(NULL).")));
			}

			colname = lfirst(next_long_expr);
		}

		if (i == 0)
			appendStringInfo(str, "select %s", colname);
		else
			appendStringInfo(str, ", %s", colname);
	}

	const char* table_qname =
		make_qualified_name(rel->rd_rel->relnamespace,
			rel->rd_rel->relname.data, NULL);

	StringInfoData str_qual;
	initStringInfo(&str_qual);
	appendStringInfo(&str_qual, " from %s ", table_qname);
	
	ntgts = 0;
	List *rtable = rss->ss.ps.state->es_plannedstmt->rtable;
	List *local_quals = NULL;
	RemotePrintExprContext rpec;
	InitRemotePrintExprContext(&rpec, rtable);
	rpec.ignore_param_quals = !rss->param_driven;
	rpec.rpec_param_exec_vals = rss->ss.ps.ps_ExprContext->ecxt_param_exec_vals;

	foreach(lc, qual)
	{
		slen = str_qual.len;
		/*
		 * These qual items are implicitly AND'ed.
		 * */
		if (ntgts > 0)
			appendStringInfoString(&str_qual, " AND ");
		else
			appendStringInfoString(&str_qual, " where ");
		int seret = snprint_expr(&str_qual, (Expr*)lfirst(lc), &rpec);
		if (seret >= 0)
		{
			ntgts++;
		}
		else
		{
			/*
			 * If we can't serialize the qual item, leave it to computing node
			 * and do the filtering locally. And we also need to associate the
			 * vars used in quals to the remote SELECT result, i.e. target-list
			 * and quals use different Var objects to refer to the same column.
			 * */
			truncateStringInfo(&str_qual, slen);
			add_qual_cols_to_src(rss, rel, (Expr*)lfirst(lc), str);
			local_quals = lappend(local_quals, lfirst(lc));
		}
	}

	appendBinaryStringInfo(str, str_qual.data, str_qual.len);
	pfree(str_qual.data);

	if (rss->check_exists)
		appendStringInfoString(str, " limit 1");

	/*
	 * The qual is added to the remote sql in so we will only get rows matching
	 * the qual from remote node, no extra checks needed for ExecScan().
	 * Except there may be qual items that can't be serialized, which can't be
	 * sent to remote storage node, they have to be used to filter remote
	 * returned rows.
	 * */
	rss->ss.ps.qual =
		ExecInitQual(local_quals, (PlanState *) &rss->ss.ps);
}

/*
 * Convert mysql result of pg's timetz/timestamptz column to pg's expected
 * format. 'val' is intact.
 * @retval a converted timestamp/time tz constant with timezone tail
 * recognized by pg internally. Result is valid before next call, and should NOT
 * be written to in case of buffer overrun.
 * */
inline static char *convert_tz_const_pg(Oid typid, const char *val)
{
	static char timestamptz_buf[64];
	char *str = NULL;
	int ret = 0;

	/*
	 * The type-id is informative enough, we can't prepend 'timestamp with time zone'
	 * or 'time with time zone' otherwise timestamptz_in() throws parse error.
	 * But we do need to append +00 to indicate that the constant's UTC based.
	 * */
	if (typid == TIMETZOID || typid == TIMESTAMPTZOID)
	{
		ret = snprintf(timestamptz_buf, sizeof(timestamptz_buf), "%s+00", val);
		str = timestamptz_buf;
	}

	Assert(ret < sizeof(timestamptz_buf));
	if (ret >= sizeof(timestamptz_buf))
		str = NULL;
	return str;
}

static char *convert_bit_const(MemoryContext mctx, const char *bits, unsigned long fldlen, int nbits)
{
	int len = nbits + 1;

	char *res = MemoryContextAllocZero(mctx, len);

	int i, j;
	unsigned char bit = (1 << (nbits % 8 - 1));

	for (i = 0, j = 0; i < fldlen && j < len - 1; j++)
	{
		res[j] = ((bits[i] & bit) ? '1' : '0');
		bit /= 2;
		if (bit == 0)
		{
			bit = (1 << 7);
			i++;
		}
	}
	return res;
}

void ExecStoreRemoteTuple(TypeInputInfo *tii, MYSQL_ROW row,
	unsigned long *lengths, TupleTableSlot *slot)
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
			Oid			typinput;
			Oid			typioparam;
			TypeInputInfo *ptii = tii + i;

		    typinput = ptii->typinput;
		    typioparam = ptii->typioparam;

			if (ptii->typisenum)
			{
			    slot->tts_values[i] = GetEnumLabelOidCached(
					ptii->AllEnumLabelOidEntries, ptii->nslots, row[i]);
				slot->tts_isnull[i] = false;
			}
			else
			{
				if (att->atttypid == TIMETZOID || att->atttypid == TIMESTAMPTZOID)
					pfld = convert_tz_const_pg(att->atttypid, row[i]);
				else if (att->atttypid == BITOID || att->atttypid == VARBITOID)
				{
					pfld = convert_bit_const(tii->mctx, row[i], lengths[i], att->atttypmod);
				}
				else
					pfld = row[i];

				/*
				  if pfld is empty string and field type isn't string,
				  it means this field is NULL
				*/
				if (pfld[0] == '\0' && !is_string_type(att->atttypid))
				{
					slot->tts_values[i] = (Datum) 0;
					slot->tts_isnull[i] = true;
				}
				else
				{
			    	slot->tts_values[i] =
				    	OidInputFunctionCall(typinput, pfld,
					    					 typioparam, att->atttypmod);
					slot->tts_isnull[i] = false;
				}
			}
		}
		else
		{
			/*
			 * We assign NULL to dropped attributes, NULL values, and missing
			 * values (missing values should be later filled using
			 * slot_fill_defaults).
			 */
			slot->tts_values[i] = (Datum) 0;
			slot->tts_isnull[i] = true;
		}
	}
	ExecStoreVirtualTuple(slot);
}

/*
 * There are more columns we need from remote, as required by the 'expr' which
 * is one 'qual' of the implicitly AND'ed qual items. Append such columns into
 * source tupledesc and remote sql string, and associate the qual's vars to
 * the corresponding column in source tupledesc.
 * */
static void add_qual_cols_to_src(RemoteScanState *rss, Relation rel,
	Expr* expr, StringInfo str)
{
	RemoteScan *rs = (RemoteScan *)rss->ss.ps.plan;
	TupleDesc tupdesc = rss->ss.ps.scandesc;
	VarPickerCtx vpc;
	memset(&vpc, 0, sizeof(vpc));
	vpc.scanrelid = rs->scanrelid;
	vpc.mctx = rss->ss.ps.state->es_query_cxt;

	if (IsA(expr, Const))
	{
		vpc.local_evaluables++;
		return;
	}
	else
	{
		expression_tree_walker((Node *)expr, var_picker, &vpc);
	}

	if (vpc.has_alien_cols)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: Columns of other tables can't be in a base table scan.")));
	}

	int cur_resno = tupdesc->natts + 1;
	const char *vcolname = NULL;
	Form_pg_attribute relattrs = rel->rd_att->attrs;

	// need real capacity for boundary checks
	tupdesc->natts = rss->scandesc_natts_cap;

	for (int i = 0; i < vpc.nvars; i++)
	{
		Var *v = vpc.target_cols[i];
		validate_column_reference(v, rel);
		/*
		  check v->varno is rel, otherwise skip it; 
		  then handle v->varattno == 0 case(whole row col); 
		  check vcolname valid.
		*/
		RangeTblEntry *rte = rt_fetch(v->varno, rss->ss.ps.state->es_plannedstmt->rtable);
		if (rte->relid != rel->rd_id) continue;
		if (v->varattno == 0)
		{
			cur_resno = append_cols_for_whole_var(rel, &tupdesc, cur_resno);
			continue; // whole-row var processed.
		}
		vcolname = relattrs[v->varattno - 1].attname.data;
		if (vcolname == NULL || strlen(vcolname) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Kunlun-db: Internal error: Invalid column(%d) referenced in relation(%d, %s).",
					 		v->varattno, rte->relid, rel->rd_rel->relname.data)));
		for (int j = 0; j < cur_resno-1; j++)
		{
			if (bms_is_member(j+1, rss->long_exprs_bmp))
				continue;

			Form_pg_attribute attr = tupdesc->attrs + j;
			/*
			 * This is v's column, associate it to the column in source
			 * tupledesc.
			 * */
			if (strcmp(attr->attname.data, vcolname) == 0)
			{
				v->varattno = j+1;
				goto found;
			}
		}

		/* 
		 * Add column to tupdesc and str, and associate.
		 * Make sure there is enough slot space for more Attrs first.
		 */
		if (cur_resno > rss->scandesc_natts_cap)
		{
			tupdesc = expandTupleDesc2(tupdesc);
			rss->scandesc_natts_cap = tupdesc->natts;
		}

		TupleDescInitEntry(tupdesc,
						   cur_resno,
						   vcolname,
						   v->vartype,
						   v->vartypmod,
						   0);
		TupleDescInitEntryCollation(tupdesc,
									cur_resno,
									v->varcollid);
		v->varattno = cur_resno;
		cur_resno++;
		appendStringInfo(str, ", %s", vcolname);
found:
		continue;
	}

	tupdesc->natts = cur_resno - 1;
	rss->ss.ps.scandesc = tupdesc;
}

