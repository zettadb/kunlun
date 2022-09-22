/*-------------------------------------------------------------------------
 *
 * meta.c
 *	  remote access method DML statements processing code
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/remote/dml.c
 *
 *
 * INTERFACE ROUTINES
 *		InitRemotePrintExprContext
 * NOTES
 *	  This file contains the routines which implement
 *	  the POSTGRES remote access method used for remotely stored POSTGRES
 *	  relations.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/remote_dml.h"
#include "access/sysattr.h"
#include "catalog/catalog.h"
#include "catalog/partition.h"
#include "catalog/pg_class.h"
#include "commands/dbcommands.h"
#include "executor/executor.h"
#include "executor/nodeRemotescan.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "nodes/print.h"
#include "optimizer/var.h"
#include "parser/parse_oper.h"
#include "parser/parsetree.h"
#include "sharding/sharding_conn.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/partcache.h"
#include "utils/rel.h"
#include "utils/relcache.h"

extern bool honor_nulls_dir;
static Expr *ConvertSpecialVarRecursive(Expr *expr, PlanState *planstate);
static Expr *ConvertSpecialVarNonRecursive(Expr *expr, PlanState *planstate, int child_prefer);

static const char *get_rel_aliasname(EState *estate, Index rteid)
{
	RangeTblEntry *rte = list_nth(estate->es_plannedstmt->rtable, rteid - 1);
	return rte->alias && rte->alias->aliasname ? rte->alias->aliasname : (rte->eref ? rte->eref->aliasname : NULL);
}

void InitRemotePrintExprContext(RemotePrintExprContext *rpec, List *rtable)
{
	memset(rpec, 0, sizeof(*rpec));
	rpec->rtable = rtable;
	rpec->exec_param_quals = false;
}

bool CheckPartitionKeyModified(Relation relation, Index attrno)
{
	bool found = false;
	List *ancestors;
	Form_pg_attribute attr;
	const char *attrname;
	ListCell *lc;

	ancestors = get_partition_ancestors(relation->rd_id);
	ancestors = lappend_oid(ancestors, relation->rd_id);
	attr = TupleDescAttr(RelationGetDescr(relation), attrno - 1);
	attrname = NameStr(attr->attname);

	foreach (lc, ancestors)
	{
		Oid relid = lfirst_oid(lc);
		Relation rel = relation_open(relid, NoLock);
		if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		{
			/*
			 *  Found the attrno of the column in current relation.
			 * NOTE: The order of attributes for a partitioned table may differ from the parent table.
			 */
			found = false;
			TupleDesc desc = RelationGetDescr(rel);
			for (int i = 0; i < desc->natts; ++i)
			{
				attr = TupleDescAttr(desc, attrno - 1);
				if (strcasecmp(attrname, NameStr(attr->attname)) == 0)
				{
					found = true;
					break;
				}
				attrno = (attrno == desc->natts) ? 1 : attrno + 1;
			}
			Assert(found && attrno > 0);

			/* Check the attributes of the partition key*/
			found = false;
			PartitionKey partkey = rel->rd_partkey;
			ListCell *lc2 = list_head(partkey->partexprs);
			ListCell *lc3;
			for (int i = 0; !found && i < partkey->partnatts; ++i)
			{
				/* Partition key is a column*/
				if (partkey->partattrs[i] != InvalidAttrNumber)
				{
					found = (partkey->partattrs[i] == attrno);
				}
				/* Partition key is a function */
				else
				{
					List *used_vars = pull_var_clause(lfirst(lc2), 0);
					foreach (lc3, used_vars)
					{
						if (lfirst_node(Var, lc3)->varattno == attrno)
						{
							found = true;
							break;
						}
					}
					list_free(used_vars);
				}
			}
		}
		relation_close(rel, NoLock);

		if (found)
			break;
	}

	return found;
}

static bool is_mysql_compatible_sortop(int ncols, AttrNumber *sortColidx,
				       Oid *sortOperation, bool *nullfirst, List *childtlist)
{
	Oid lt, eq, gt;
	TargetEntry *tle;

	for (int i = 0; i < ncols; ++i)
	{
		tle = get_tle_by_resno(childtlist, sortColidx[i]);
		get_sort_group_operators(exprType((Node *)tle->expr),
					 true, true, true,
					 &lt, &eq, &gt,
					 NULL);

		/* Check if the corresponding </> operator can be pushed down */
		// if (!is_mysql_operator(lt) || !is_mysql_operator(eq) || !is_mysql_operator(gt))
		// {
		// 	return false;
		// }
	}
	return true;
}

static void
GetSourceRelationWorker(PlanState *planstate, int tableoid_attrno, List **sources)
{
	PlanState *child = NULL;
	Expr *expr;

	expr = list_nth(planstate->plan->targetlist, tableoid_attrno - 1);
	expr = castNode(TargetEntry, expr)->expr;
	Assert(IsA(expr, Var));
	Var *tableoid = (Var *)expr;

	if (IS_SPECIAL_VARNO(tableoid->varno))
	{
		switch (tableoid->varno)
		{
		case OUTER_VAR:
			if (IsA(planstate, ModifyTableState))
			{
				Assert(((ModifyTableState *)planstate)->mt_nplans < 2);
				child = ((ModifyTableState *)planstate)->mt_plans[0];
				GetSourceRelationWorker(child, tableoid->varattno, sources);
			}
			else if (IsA(planstate, AppendState))
			{
				AppendState *append = (AppendState *)planstate;
				for (int i = 0; i < append->as_nplans; ++i)
				{
					child = append->appendplans[i];
					GetSourceRelationWorker(child, tableoid->varattno, sources);
				}
			}
			else if (IsA(planstate, MergeAppendState))
			{
				MergeAppendState *merge = (MergeAppendState *)planstate;
				for (int i = 0; i < merge->ms_nplans; ++i)
				{
					child = merge->mergeplans[i];
					GetSourceRelationWorker(child, tableoid->varattno, sources);
				}
			}
			else
			{
				child = outerPlanState(planstate);
				GetSourceRelationWorker(child, tableoid->varattno, sources);
			}
			break;
		case INNER_VAR:
			child = innerPlanState(planstate);
			GetSourceRelationWorker(child, tableoid->varattno, sources);
			break;
		case REMOTE_VAR:
			Assert(IsA(planstate, RemoteScanState));
			RemoteScan *plan = (RemoteScan *)planstate->plan;
			*sources = lappend_int(*sources, plan->scanrelid);
			break;
		}
	}
	else
	{
		Assert(tableoid->varattno == TableOidAttributeNumber);
		if (IsA(planstate, RemoteScanState))
		{
			RemoteScan *plan = (RemoteScan *)planstate->plan;
			*sources = lappend_int(*sources, plan->scanrelid);
		}
	}
}

static void
GetSourceRelation(ModifyTableState *mtstate, List **sources)
{
	/* Get all of the source by travel the junk tableoid var */
	if (mtstate->resultRelInfo->ri_junkTableoid)
	{
		PlanState *child = mtstate->mt_plans[0];
		AttrNumber attrno = mtstate->resultRelInfo->ri_junkTableoid->jf_junkAttNo;
		GetSourceRelationWorker(child, attrno, sources);
	}
}

bool CanPushdownRemoteUD(PlanState *state, List *unused_tl, int *nleafs, const char **reason)
{
	switch (state->type)
	{
	case T_ModifyTableState:
	{
		Assert(!unused_tl);
		ModifyTableState *mtstate = (ModifyTableState *)state;
		/* Ignore the junkres, we not use it when push query down */
		ResultRelInfo *relinfo = mtstate->resultRelInfo;
		JunkFilter *jf = relinfo->ri_junkFilter;
		AttrNumber junkattr = jf->jf_junkAttNo;
		List *unused = NIL;
		if (AttributeNumberIsValid(junkattr))
		{
			TargetEntry *tle = (TargetEntry *)list_nth(jf->jf_targetList, junkattr - 1);
			unused = lappend(unused, tle->expr);
		}
		/*
		 * We not use inheritance_planner() to generate plan for partitioned table ,
		 * so only one subplan here
		 */
		if (IsRemoteRelationParent(relinfo->ri_RelationDesc))
			Assert(mtstate->mt_nplans < 2);
		
		List *tlist = NIL;
		for (int i = 0; i < mtstate->mt_nplans; ++i)
		{
			PlanState *subplan = mtstate->mt_plans[i];
			tlist = subplan->plan->targetlist;
			if (!CanPushdownRemoteUD(subplan, unused, nleafs ,reason))
				return false;
		}

		/* Check if the partition key is modified */
		if (mtstate->operation == CMD_UPDATE &&
		    IsRemoteRelationParent(relinfo->ri_RelationDesc))
		{
			ListCell *lc;
			List *sources = NIL;
			/* Get all of the source relations */
			GetSourceRelation(mtstate, &sources); 
			foreach (lc, tlist)
			{
				TargetEntry *tle = lfirst_node(TargetEntry, lc);
				if (tle->resjunk || column_name_is_dropped(tle->resname))
					continue;

				Expr *expr = ConvertSpecialVarRecursive(tle->expr, mtstate->mt_plans[0]);
				/* Skip if update is set a=a */
				if (IsA(expr, Var))
				{
					Var *var = (Var *)expr;
					if (var->varattno == tle->resno && list_member_int(sources, var->varno))
						continue;
				}

				if (CheckPartitionKeyModified(relinfo->ri_RelationDesc, tle->resno))
				{
					*reason = "Can not update partition key of remote relation";
					return false;
				}
			}
		}
		break;
	}
	case T_AppendState:
	{
		AppendState *astate = (AppendState *)state;
		for (int i = 0; i < astate->as_nplans; ++i)
		{
			PlanState *child = astate->appendplans[i];
			List *unused = (List *)ConvertSpecialVarNonRecursive((Expr *)unused_tl, state, i);
			if (!CanPushdownRemoteUD(child, unused, nleafs, reason))
				return false;
		}
		break;
	}
	case T_MergeAppendState:
	{
		MergeAppendState *mastate = (MergeAppendState *)state;
		MergeAppend *mappend = (MergeAppend *)mastate->ps.plan;
		if (mastate->ms_nplans != 1 ||
		    !CanPushdownRemoteUD(mastate->mergeplans[0],
					      (List *)ConvertSpecialVarNonRecursive((Expr *)unused_tl, state, 0),
					      nleafs,
					      reason))
			return false;

		List *childtlist = mastate->mergeplans[0]->plan->targetlist;
		if (!is_mysql_compatible_sortop(mappend->numCols,
						mappend->sortColIdx,
						mappend->sortOperators,
						mappend->nullsFirst,
						childtlist))
		{
			*reason = "Canot push down sort";
			return false;
		}
		break;
	}
	case T_SortState:
	{
		Sort *sort = (Sort *)state->plan;
		List *childtlist = outerPlanState(state)->plan->targetlist;
		if (!is_mysql_compatible_sortop(sort->numCols,
						sort->sortColIdx,
						sort->sortOperators,
						sort->nullsFirst, childtlist))
		{
			*reason = "Canot push down sort";
			return false;
		}
		/*pass through*/
	}
	case T_LimitState:
	{
		if (!CanPushdownRemoteUD(outerPlanState(state),
					 (List *)ConvertSpecialVarNonRecursive((Expr *)unused_tl, state, 0),
					 nleafs,
					 reason))
			return false;

		/* Only pushdown sort if there is only one leaf target table */	
		if (*nleafs > 1 && state->type == T_Sort)
		{
			*reason = "Cannot push down sort";
			return false;
		}
		break;
	}
	case T_RemoteScanState:
	{
		(*nleafs)++;
		if (!IsRemoteScanTotallyPushdown((RemoteScanState*) state, unused_tl))
		{
			*reason = "Exression cannot be serialized";
			return false;
		}

		break;
	}
	case T_ResultState:
	{
		if (outerPlanState(state) &&
		    !CanPushdownRemoteUD(outerPlanState(state),
					 (List *)ConvertSpecialVarNonRecursive((Expr *)unused_tl, state, 0),
					 nleafs,
					 reason))
			return false;
		break;
	}
	default:
	{
		*reason = "Plan cannot be serialized";
		return false;
	}
	}
	return true;
}

/* Evaluate the result state*/
static bool
evaluate_resultstate_qual(RemoteUD *remote_updel, ResultState *node)
{
	if (node->resconstantqual)
		return ExecQual(node->resconstantqual, node->ps.ps_ExprContext);
	return true;
}

void RemoteUDSetup(PlanState *planstate, RemoteUD *remote_updel)
{
	switch (planstate->type)
	{
	case T_ModifyTableState:
	{
		/* Only when optimizer generate just one plan for partitioned table, can be here */
		ModifyTableState *mtstate = (ModifyTableState *)planstate;
		int plans = mtstate->mt_nplans;

		for (int i = 0; i < plans; ++i, ++remote_updel)
		{
			/* top plan, initlize all the member */
			remote_updel->planIndex = i;
			remote_updel->index = 0;
			remote_updel->rel = NULL;
			remote_updel->rti = -1;
			remote_updel->from = NULL;
			remote_updel->sortlist = NIL;
			remote_updel->sortop = NIL;
			remote_updel->limit = -1;
			remote_updel->mtstate = mtstate;
			remote_updel->rellist = NIL;
			remote_updel->quallist = NIL;
			remote_updel->relinfo = mtstate->resultRelInfo + i;

			PlanState *child = mtstate->mt_plans[i];

			if (mtstate->operation == CMD_UPDATE)
				remote_updel->tlist = child->plan->targetlist;
			else
				remote_updel->tlist = NULL;
			RemoteUDSetup(child, remote_updel);
		}
		break;
	}
	case T_AppendState:
	{
		AppendState *append = (AppendState *)planstate;
		for (int i = 0; i < append->as_nplans; ++i)
		{
			PlanState *child = append->appendplans[i];
			RemoteUDSetup(child, remote_updel);
		}
		break;
	}
	case T_LimitState:
	{
		LimitState *limit = (LimitState *)planstate;
		if (limit->limitCount)
		{
			bool isNull;
			Datum val = ExecEvalExprSwitchContext(limit->limitCount,
							      limit->ps.ps_ExprContext,
							      &isNull);

			if (!isNull)
				remote_updel->limit = DatumGetInt64(val);
		}
		RemoteUDSetup(outerPlanState(limit), remote_updel);
		break;
	}
	case T_MergeAppendState:
	{
		MergeAppendState *mastate = (MergeAppendState *)planstate;
		MergeAppend *mappend = (MergeAppend *)mastate->ps.plan;
		PlanState *child = mastate->mergeplans[0];
		List *childtlist = child->plan->targetlist;
		for (int i = 0; i < mappend->numCols; ++i)
		{
			AttrNumber resno = mappend->sortColIdx[i];
			Expr *expr = get_tle_by_resno(childtlist, resno)->expr;
			expr = ConvertSpecialVarRecursive(expr, child);
			remote_updel->sortlist = lappend(remote_updel->sortlist, expr);
			remote_updel->sortop = lappend_oid(remote_updel->sortop, mappend->sortOperators[i]);
			remote_updel->sortnullfirst = lappend_int(remote_updel->sortnullfirst, mappend->nullsFirst[i] ? 1 : 0);
		}
		RemoteUDSetup(child, remote_updel);
		break;
	}
	case T_SortState:
	{
		SortState *sortstate = (SortState *)planstate;
		Sort *sort = (Sort *)sortstate->ss.ps.plan;
		List *childtlist = outerPlan(sort)->targetlist;
		for (int i = 0; i < sort->numCols; ++i)
		{
			AttrNumber resno = sort->sortColIdx[i];
			Expr *expr = get_tle_by_resno(childtlist, resno)->expr;
			expr = ConvertSpecialVarRecursive(expr, outerPlanState(sortstate));
			remote_updel->sortlist = lappend(remote_updel->sortlist, expr);
			remote_updel->sortop = lappend_oid(remote_updel->sortop, sort->sortOperators[i]);
			remote_updel->sortnullfirst = lappend_int(remote_updel->sortnullfirst, sort->nullsFirst[i] ? 1 : 0);
		}
		RemoteUDSetup(outerPlanState(sortstate), remote_updel);
		break;
	}
	case T_ResultState:
	{
		if (outerPlanState(planstate) &&
			evaluate_resultstate_qual(remote_updel, (ResultState *)planstate))
		{
			RemoteUDSetup(outerPlanState(planstate), remote_updel);
		}
		break;
	}
	case T_RemoteScanState:
	{
		RemoteScanState *rss = (RemoteScanState *)planstate;
		RemoteScan *rs = (RemoteScan *)planstate->plan;
		remote_updel->rellist = lappend_int(remote_updel->rellist, rs->scanrelid);
		remote_updel->quallist = lappend(remote_updel->quallist, rss->quals_pushdown);
		/* Remember the index of first target RTE */
		if (remote_updel->rti == -1)
			remote_updel->rti = rs->scanrelid;
		break;
	}
	default:
		ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("kunlun-db: unsupported plan node %d", planstate->type)));
	}
}

static void RemoteUDBuild(RemoteUD *remote_updel, RemotePrintExprContext *rpec, StringInfo sql)
{
	NameData dbname, nspname;
	ListCell *lc;

	initStringInfo2(sql, 256, TopTransactionContext);
	get_database_name3(MyDatabaseId, &dbname);
	get_namespace_name3(RelationGetNamespace(remote_updel->rel), &nspname);

	appendStringInfo(sql, "%s %s_$$_%s.%s %s ",
			 remote_updel->mtstate->operation == CMD_UPDATE ? "update " : "delete from ",
			 dbname.data,
			 nspname.data,
			 RelationGetRelationName(remote_updel->rel),
			 get_rel_aliasname(remote_updel->mtstate->ps.state, remote_updel->rti));

	/* Update set clause */
	if (remote_updel->tlist)
	{
		bool first = true;
		int planIndex = remote_updel->planIndex;
		foreach (lc, remote_updel->tlist)
		{
			TargetEntry *tle = lfirst_node(TargetEntry, lc);
			/* skip junk expr, we do not use it in remote sql */
			if (tle->resjunk)
				continue;

			Form_pg_attribute attr = TupleDescAttr(remote_updel->parent->rd_att, tle->resno - 1);
			if (attr->attisdropped ||
			    !bms_is_member(tle->resno - FirstLowInvalidHeapAttributeNumber,
					   remote_updel->parent_rte->updatedCols))
				continue;

			appendStringInfo(sql,
					 " %s %s=", first ? "set" : ",",
					 attr->attname.data);

			if (snprint_expr(sql, expr, rpec) < 0)
				ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("In kunlun-db unable to serialize value expression of target column %s in update statement %s.",
						tle->resname, remote_updel->mtstate->ps.state->es_sourceText)));
			first = false;
		}
	}

	/* Where clause */
	if (list_length(remote_updel->qual) > 0)
	{
		bool first = true;
		foreach (lc, remote_updel->qual)
		{
			Expr *expr = (Expr *)lfirst(lc);
			appendStringInfo(sql, " %s ", first ? "where" : "and");
			if (snprint_expr(sql, expr, rpec) < 0)
				ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("Kunlun-db unable to serialize where clause expression in statement.")));
			first = false;
		}
	}

	/* Orderby clause */
	if (remote_updel->sortlist)
	{
		ListCell *lc1, *lc2, *lc3;
		bool first = true;
		forthree(lc1, remote_updel->sortlist, lc2, remote_updel->sortop, lc3, remote_updel->sortnullfirst)
		{
			Expr *sortexpr = (Expr *)lfirst(lc1);
			Oid sortop = lfirst_oid(lc2);
			Oid oplt_id;
			bool nullfirst = (lfirst_int(lc3) == 1);
			get_sort_group_operators(exprType((Node*)sortexpr),
						 false, false, true,
						 &oplt_id, NULL, NULL,
						 NULL);

			appendStringInfo(sql, " %s ", first ? "order by" : ",");
			
			/* Desc with null first or asc with null last */
			if (nullfirst != (sortop == oplt_id))
			{
				appendStringInfoString(sql, "isnull(");
				if (snprint_expr(sql, sortexpr, rpec) < 0)
				{
					ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("Kunlun-db unable to serialize order by clause expression in statement.")));
				}
				appendStringInfo(sql, ") %s, ", (sortop == oplt_id ? "asc" : "desc"));
			}

			if (snprint_expr(sql, sortexpr, rpec) < 0)
				ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("Kunlun-db unable to serialize order by clause expression in statement.")));

			if (oplt_id == sortop)
				appendStringInfo(sql, " asc");
			else
				appendStringInfo(sql, " desc");

			first = false;
		}
	}

	/* Limit clause */
	if (remote_updel->limit >= 0)
	{
		appendStringInfo(sql, " limit %ld", remote_updel->limit);
	}

	/* Returning clause */
	if (remote_updel->mtstate->pushdown_returning)
	{
		bool first = true;
		foreach (lc, remote_updel->mtstate->pushdown_returning)
		{
			Expr *expr = (Expr *)lfirst(lc);
			appendStringInfo(sql, " %s ", first ? "returning" : ",");
			if (snprint_expr(sql, expr, rpec) < 0)
				ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("Kunlun-db unable to serialize where clause expression in statement.")));
			first = false;
		}
	}
}

bool RemoteUDNext(RemoteUD *remote_updel, RemotePrintExprContext *rpec, StringInfo sql, Oid *shardid)
{
	if (remote_updel->index >= list_length(remote_updel->rellist))
		return false;

	Index rteid = list_nth_int(remote_updel->rellist, remote_updel->index);
	List *rtable = remote_updel->mtstate->ps.state->es_plannedstmt->rtable;
	RangeTblEntry *rte = list_nth(rtable, rteid - 1);
	remote_updel->parent_rte = list_nth(rtable, remote_updel->relinfo->ri_RangeTableIndex - 1);
	remote_updel->parent = relation_open(remote_updel->parent_rte->relid, NoLock);
	remote_updel->rel = relation_open(rte->relid, AccessShareLock);
	remote_updel->qual = list_nth(remote_updel->quallist, remote_updel->index);
	remote_updel->index++;
	*shardid = remote_updel->rel->rd_rel->relshardid;

	RemoteUDBuild(remote_updel, rpec, sql);

	relation_close(remote_updel->parent, NoLock);
	relation_close(remote_updel->rel, AccessShareLock);
	remote_updel->rel = NULL;
	return true;
}

bool RemoteUDEOF(RemoteUD *remote_updel)
{
	return (remote_updel->limit == 0) ||
	       (remote_updel->index >= list_length(remote_updel->rellist));
}

typedef struct ConvertSpecialVarContext
{
	PlanState *planstate; /* The plan node which the special var in */
	int child_prefer;     /* Prefer which child to resolve speical var */
	bool recursive;
} ConvertSpecialVarContext;

static Expr *ConvertSpecialVarMutator(Expr *expr, ConvertSpecialVarContext *context)
{
	if (!expr)
		return NULL;

	if (IsA(expr, Var) &&
	    IS_SPECIAL_VARNO(((Var *)expr)->varno))
	{
		Var *var = (Var *)expr;
		PlanState *planstate = context->planstate;
		PlanState *child = NULL;
		switch (var->varno)
		{
		case OUTER_VAR:
			if (IsA(planstate, ModifyTableState))
			{
				// Assert(((ModifyTableState *)planstate)->mt_nplans < 2);
				child = ((ModifyTableState *)planstate)->mt_plans[0];
			}
			else if (IsA(planstate, AppendState))
			{
				child = ((AppendState *)planstate)->appendplans[context->child_prefer];
			}
			else if (IsA(planstate, MergeAppendState))
			{
				child = ((MergeAppendState *)planstate)->mergeplans[context->child_prefer];
			}
			else
			{
				child = outerPlanState(planstate);
			}
			expr = list_nth(child->plan->targetlist, var->varattno - 1);
			expr = ((TargetEntry *)expr)->expr;
			break;
		case INNER_VAR:
			child = innerPlanState(planstate);
			expr = list_nth(child->plan->targetlist, var->varattno - 1);
			expr = ((TargetEntry *)expr)->expr;
			break;
		case REMOTE_VAR:
			{
				Assert(IsA(planstate, RemoteScanState));
				expr = list_nth(((RemoteScanState *)planstate)->scanexprs, var->varattno - 1);
			}
			break;
		}

		if (context->recursive && expr != (Expr *)var)
		{
			context->planstate = child;
			expr = ConvertSpecialVarMutator(expr, context);
			context->planstate = planstate;
		}

		return expr;
	}

	return (Expr*)expression_tree_mutator((Node*)expr, (Node*(*)())ConvertSpecialVarMutator, context);
}

static Expr *ConvertSpecialVarRecursive(Expr *expr, PlanState *planstate)
{
	ConvertSpecialVarContext context;
	context.planstate = planstate;
	context.child_prefer = 0;
	context.recursive = true;
	return ConvertSpecialVarMutator(expr, &context);
}

static Expr *ConvertSpecialVarNonRecursive(Expr *expr, PlanState *planstate, int child_prefer)
{
	ConvertSpecialVarContext context;
	context.planstate = planstate;
	context.child_prefer = child_prefer;
	context.recursive = false;
	return ConvertSpecialVarMutator(expr, &context);
}
