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
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "nodes/print.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "catalog/pg_class.h"
#include "catalog/catalog.h"
#include "sharding/sharding_conn.h"
#include "utils/builtins.h"
#include "nodes/plannodes.h"
#include "nodes/execnodes.h"
#include "access/remote_dml.h"
#include "utils/lsyscache.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"

void InitRemotePrintExprContext(RemotePrintExprContext *rpec, List*rtable)
{
	memset(rpec, 0, sizeof(*rpec));
	rpec->rtable = rtable;
	rpec->ignore_param_quals = true;
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
		RemoteModifyState *rms0 = mtstate->mt_remote_states;
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
	
	        if (bms_is_member(i+1, rms0->long_exprs_bmp))
	        {
	            next_long_expr = (next_long_expr ? lnext(next_long_expr) : list_head(rms0->long_exprs));
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

	append_async_stmt(rms->asi, rms->remote_dml.data, rms->remote_dml.len,
		operation, true, operation == CMD_UPDATE ? SQLCOM_UPDATE : SQLCOM_DELETE);
	int rc = 0;
	if (!rms->asi->result_pending)
	{
		rc = work_on_next_stmt(rms->asi);
		if (rc == 1) send_stmt_to_multi_start(rms->asi, 1);
	}
	if (rc < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db internal error: mysql result has not been consumed yet.")));
}
