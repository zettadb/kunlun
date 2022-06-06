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
 *		post_remote_updel_stmt
 * NOTES
 *	  This file contains the routines which implement
 *	  the POSTGRES remote access method used for remotely stored POSTGRES
 *	  relations.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/remote_dml.h"
#include "catalog/catalog.h"
#include "catalog/partition.h"
#include "catalog/pg_class.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "nodes/print.h"
#include "optimizer/var.h"
#include "sharding/sharding_conn.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/partcache.h"
#include "utils/rel.h"
#include "utils/relcache.h"

void InitRemotePrintExprContext(RemotePrintExprContext *rpec, List*rtable)
{
	memset(rpec, 0, sizeof(*rpec));
	rpec->rtable = rtable;
	rpec->ignore_param_quals = true;
}

static bool
is_partkey_modified(Relation relation, Index attrno)
{
	bool found = false;
	List *ancestors;
	Form_pg_attribute attr;
	const char *attrname;
	ListCell *lc;

	ancestors = get_partition_ancestors(relation->rd_id);
	ancestors = lappend_oid(ancestors, relation->rd_id);
	attr = TupleDescAttr(RelationGetDescr(relation), attrno-1);
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
			Assert (found && attrno>0);

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

void post_remote_updel_stmt(ModifyTableState*mtstate, RemoteScan *rs, int i)
{
	static NameData dbname = {'\0'};
	NameData nspname;
	if (dbname.data[0] == '\0') get_database_name3(MyDatabaseId, &dbname);
	CmdType operation = mtstate->operation;
	ResultRelInfo *resultRelInfo = mtstate->resultRelInfo + i;
	RemoteModifyState *rms = mtstate->mt_remote_states + i;

	Assert(rms->asi == NULL);
	initStringInfo2(&rms->remote_dml, 256, TopTransactionContext);
	rms->asi = GetAsyncStmtInfo(resultRelInfo->ri_RelationDesc->rd_rel->relshardid);
	get_namespace_name3(resultRelInfo->ri_RelationDesc->rd_rel->relnamespace, &nspname);
	appendStringInfo(&rms->remote_dml, "%s %s_$$_%s.%s",
		operation == CMD_UPDATE ? "update " : "delete from ",
		dbname.data, nspname.data,
		resultRelInfo->ri_RelationDesc->rd_rel->relname.data);

	ListCell *lc;
	int num_qual = 0;
	int num_tle = 0;
	int len = 0;
	List*rtable = mtstate->ps.state->es_plannedstmt->rtable;
	const char *srctxt = mtstate->ps.state->es_sourceText;

	RemotePrintExprContext rpec;
	InitRemotePrintExprContext(&rpec, rtable);
	// For update&delete, always use params if any, we have to precisely
	// locate the target row.
	rpec.ignore_param_quals = false;
	rpec.rpec_param_exec_vals =
		(mtstate->ps.ps_ExprContext ?
			mtstate->ps.ps_ExprContext->ecxt_param_exec_vals : NULL);
	rpec.rpec_param_list_info = mtstate->ps.state->es_param_list_info;

	if (operation == CMD_UPDATE)
	{
		foreach(lc, rs->plan.targetlist)
		{
			Assert(IsA(lfirst(lc), TargetEntry));
			TargetEntry *tle = (TargetEntry *)lfirst(lc);
			/*
			  skip junk fields. for update/delete, only the ctid field is junk.
			*/
			if (tle->resname && strcmp(tle->resname, "ctid") == 0)
				continue;
			/*
			  tle->resname may be an alias name instead of the target relation's
			  real column name.
			*/
			TupleDesc rel_tupdesc = resultRelInfo->ri_RelationDesc->rd_att;
			Assert(tle->resno > 0 && tle->resno <= rel_tupdesc->natts);
			Form_pg_attribute tgt_attr = TupleDescAttr(rel_tupdesc, tle->resno - 1);

			if (tle->expr && IsA(tle->expr, Var))
			{
				/*
				  Avoid set a=a targets. Regular case when there are no
				  dropped columns in the table.
				*/
				if (((Var*)tle->expr)->varno == resultRelInfo->ri_RangeTableIndex &&
					((Var*)tle->expr)->varattno == tle->resno)
					continue;
				/*
				if (tle->resname &&
					strcmp(tle->resname,
						   get_var_attname((Var*)tle->expr, rtable)) == 0)
				{
					ereport(WARNING,
							(errcode(ERRCODE_SQL_STATEMENT_NOT_YET_COMPLETE),
							 errmsg("Kunlun-db: skipping column self assignment: %s.",
							 		tgt_attr->attname.data)));
					continue;
				}
				*/
			}

			/*
			  Skip dropped columns.
			*/
			if (tle->resname && column_name_is_dropped(tle->resname))
				continue;

			/* Check if update the partition key */
			if (is_partkey_modified(resultRelInfo->ri_RelationDesc, tle->resno))
			{
				ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("Can not update partition key of a remote relation")));
			}

			appendStringInfo(&rms->remote_dml, " %s %s= ",
				num_tle++ == 0 ? "set":", ", tgt_attr->attname.data);

			int len = snprint_expr(&rms->remote_dml, tle->expr, &rpec);
			if (len < 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("In kunlun-db unable to serialize value expression of target column %s in update statement %s.",
						 tle->resname, srctxt)));
		}

		if (num_tle == 0)
		{
			ereport(NOTICE,
					(errcode(ERRCODE_SQL_STATEMENT_NOT_YET_COMPLETE),
					 errmsg("Kunlun-db: No target columns found in update statement %s, query execution skipped.",
					 srctxt)));
			return;
		}
	}

	/*
	  Must accurately update all target rows no more no less in one shot, so
	  the qual must be fully serialized otherwise the update stmt can't be
	  executed.
	  For the same reason above targetentry must be fully serialized too.
	*/
	foreach(lc, rs->plan.qual)
	{
		Expr *expr = (Expr *)lfirst(lc);
		appendStringInfo(&rms->remote_dml, " %s ", num_qual++ == 0 ? "where ":" and ");
		len = snprint_expr(&rms->remote_dml, expr, &rpec);
		if (len < 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("Kunlun-db unable to serialize where clause expression in statement %s.", srctxt)));
	}

	/*
	  there is a returning clause
	*/
	if (mtstate->ps.scandesc)
	{
	    TupleDesc tupdesc = mtstate->ps.scandesc;
	    ListCell *next_long_expr = NULL;
		int nrettgts = 0;

	    for (int i = 0; i < tupdesc->natts; i++)
	    {
	        Form_pg_attribute attr = tupdesc->attrs+i;
	
	        /*
	         * The remote target column expression to append to the remote
			 * returning clause.
	         * Normally it's a column name, but may also be an expression
	         * containing one or more column names.
	         * */
	        const char *colname = attr->attname.data;
	
	        if (!colname)
	            ereport(ERROR,
	                    (errcode(ERRCODE_INTERNAL_ERROR),
	                     errmsg("Invalid target column name(NULL) in kunlun-db.")));
	
	        if (bms_is_member(i+1, mtstate->long_exprs_bmp))
	        {
	            next_long_expr = (next_long_expr ? lnext(next_long_expr) : list_head(mtstate->long_exprs));
	            if (next_long_expr == NULL)
	            {
	                ereport(ERROR,
	                        (errcode(ERRCODE_INTERNAL_ERROR),
	                         errmsg("Invalid target column long name(NULL) in kunlun-db.")));
	            }
	
	            colname = lfirst(next_long_expr);
	        }

			appendStringInfo(&rms->remote_dml, "%s %s", i == 0 ? " returning":",", colname);
			nrettgts++;
	    }

		if (nrettgts == 0)
			ereport(ERROR,
					(errcode(ERRCODE_SQL_STATEMENT_NOT_YET_COMPLETE),
					 errmsg("Kunlun-db: No target columns found in returning clause of statement %s.",
					 srctxt)));
	}

	rms->handle = send_stmt_async(rms->asi, rms->remote_dml.data, rms->remote_dml.len,
				      operation, true, operation == CMD_UPDATE ? SQLCOM_UPDATE : SQLCOM_DELETE, false);
}
