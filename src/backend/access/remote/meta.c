/*-------------------------------------------------------------------------
 *
 * meta.c
 *	  remote access method code
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/remote/meta.c
 *
 * NOTES
 *	  This file contains the routines which implement
 *	  the POSTGRES remote access method used for remotely stored POSTGRES
 *	  relations.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "access/tupdesc.h"
#include "access/heapam.h"
#include "catalog/pg_attribute.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_type_d.h"
#include "access/remote_meta.h"
#include "nodes/nodes.h"
#include "utils/memutils.h"
#include "sharding/sharding_conn.h"
#include "sharding/cluster_meta.h"
#include "commands/dbcommands.h"
#include "utils/lsyscache.h"
#include "sharding/mysql/mysqld_error.h"
#include "access/remote_xact.h"
#include "utils/syscache.h"
#include "access/htup_details.h"
#include <sys/time.h>
#include "miscadmin.h"
#include "access/genam.h"
#include "catalog/pg_namespace.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include <unistd.h>
#include "postmaster/bgworker.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include "catalog/index.h"
#include "nodes/print.h"
#include "catalog/indexing.h"
#include "utils/builtins.h"
#include "catalog/dependency.h"

bool enable_remote_relations = true;
bool use_mysql_native_seq;

extern const char *format_type_remote(Oid type_oid);
static void print_str_list(StringInfo str, List *ln, char seperator);
static bool PrintParserTree(StringInfo str, Node*val);
static bool print_expr_list(StringInfo str, List *exprlist);
static void
update_index_attrname(Relation attrelation, Relation targetrelation,
	Oid indid, int16 attnum,
	const char *oldattname, const char *newattname,
	bool check_attname_uniquness);
static void get_indexed_cols(Oid indexId, Datum **keys, int *nKeys);

const char* atsubcmd(AlterTableCmd *subcmd)
{
	const char *strtype = NULL;
	switch (subcmd->subtype)
	{
		case AT_AddColumn:
			strtype = "ADD COLUMN";
			break;
		case AT_AddColumnRecurse:
			strtype = "ADD COLUMN (and recurse)";
			break;
		case AT_AddColumnToView:
			strtype = "ADD COLUMN TO VIEW";
			break;
		case AT_ColumnDefault:
			strtype = "ALTER COLUMN SET DEFAULT";
			break;
		case AT_DropNotNull:
			strtype = "DROP NOT NULL";
			break;
		case AT_SetNotNull:
			strtype = "SET NOT NULL";
			break;
		case AT_SetStatistics:
			strtype = "SET STATS";
			break;
		case AT_SetOptions:
			strtype = "SET OPTIONS";
			break;
		case AT_ResetOptions:
			strtype = "RESET OPTIONS";
			break;
		case AT_SetStorage:
			strtype = "SET STORAGE";
			break;
		case AT_DropColumn:
			strtype = "DROP COLUMN";
			break;
		case AT_DropColumnRecurse:
			strtype = "DROP COLUMN (and recurse)";
			break;
		case AT_AddIndex:
			strtype = "ADD INDEX";
			break;
		case AT_ReAddIndex:
			strtype = "(re) ADD INDEX";
			break;
		case AT_AddConstraint:
			strtype = "ADD CONSTRAINT";
			break;
		case AT_AddConstraintRecurse:
			strtype = "ADD CONSTRAINT (and recurse)";
			break;
		case AT_ReAddConstraint:
			strtype = "(re) ADD CONSTRAINT";
			break;
		case AT_AlterConstraint:
			strtype = "ALTER CONSTRAINT";
			break;
		case AT_ValidateConstraint:
			strtype = "VALIDATE CONSTRAINT";
			break;
		case AT_ValidateConstraintRecurse:
			strtype = "VALIDATE CONSTRAINT (and recurse)";
			break;
		case AT_ProcessedConstraint:
			strtype = "ADD (processed) CONSTRAINT";
			break;
		case AT_AddIndexConstraint:
			strtype = "ADD CONSTRAINT (using index)";
			break;
		case AT_DropConstraint:
			strtype = "DROP CONSTRAINT";
			break;
		case AT_DropConstraintRecurse:
			strtype = "DROP CONSTRAINT (and recurse)";
			break;
		case AT_ReAddComment:
			strtype = "(re) ADD COMMENT";
			break;
		case AT_AlterColumnType:
			strtype = "ALTER COLUMN SET TYPE";
			break;
		case AT_AlterColumnGenericOptions:
			strtype = "ALTER COLUMN SET OPTIONS";
			break;
		case AT_ChangeOwner:
			strtype = "CHANGE OWNER";
			break;
		case AT_ClusterOn:
			strtype = "CLUSTER";
			break;
		case AT_DropCluster:
			strtype = "DROP CLUSTER";
			break;
		case AT_SetLogged:
			strtype = "SET LOGGED";
			break;
		case AT_SetUnLogged:
			strtype = "SET UNLOGGED";
			break;
		case AT_AddOids:
			strtype = "ADD OIDS";
			break;
		case AT_AddOidsRecurse:
			strtype = "ADD OIDS (and recurse)";
			break;
		case AT_DropOids:
			strtype = "DROP OIDS";
			break;
		case AT_SetTableSpace:
			strtype = "SET TABLESPACE";
			break;
		case AT_SetRelOptions:
			strtype = "SET RELOPTIONS";
			break;
		case AT_ResetRelOptions:
			strtype = "RESET RELOPTIONS";
			break;
		case AT_ReplaceRelOptions:
			strtype = "REPLACE RELOPTIONS";
			break;
		case AT_EnableTrig:
			strtype = "ENABLE TRIGGER";
			break;
		case AT_EnableAlwaysTrig:
			strtype = "ENABLE TRIGGER (always)";
			break;
		case AT_EnableReplicaTrig:
			strtype = "ENABLE TRIGGER (replica)";
			break;
		case AT_DisableTrig:
			strtype = "DISABLE TRIGGER";
			break;
		case AT_EnableTrigAll:
			strtype = "ENABLE TRIGGER (all)";
			break;
		case AT_DisableTrigAll:
			strtype = "DISABLE TRIGGER (all)";
			break;
		case AT_EnableTrigUser:
			strtype = "ENABLE TRIGGER (user)";
			break;
		case AT_DisableTrigUser:
			strtype = "DISABLE TRIGGER (user)";
			break;
		case AT_EnableRule:
			strtype = "ENABLE RULE";
			break;
		case AT_EnableAlwaysRule:
			strtype = "ENABLE RULE (always)";
			break;
		case AT_EnableReplicaRule:
			strtype = "ENABLE RULE (replica)";
			break;
		case AT_DisableRule:
			strtype = "DISABLE RULE";
			break;
		case AT_AddInherit:
			strtype = "ADD INHERIT";
			break;
		case AT_DropInherit:
			strtype = "DROP INHERIT";
			break;
		case AT_AddOf:
			strtype = "OF";
			break;
		case AT_DropOf:
			strtype = "NOT OF";
			break;
		case AT_ReplicaIdentity:
			strtype = "REPLICA IDENTITY";
			break;
		case AT_EnableRowSecurity:
			strtype = "ENABLE ROW SECURITY";
			break;
		case AT_DisableRowSecurity:
			strtype = "DISABLE ROW SECURITY";
			break;
		case AT_ForceRowSecurity:
			strtype = "FORCE ROW SECURITY";
			break;
		case AT_NoForceRowSecurity:
			strtype = "NO FORCE ROW SECURITY";
			break;
		case AT_GenericOptions:
			strtype = "SET OPTIONS";
			break;
		case AT_AttachPartition:
			strtype = "ATTACH PARTITION";
			break;
		default:
			strtype = "unrecognized";
			break;
	}
	return strtype;
}

/*
  Find a domain type's root base type.
*/
Oid find_root_base_type(Oid typid0)
{
	Oid typid = typid0;

	while (true)
	{
		HeapTuple tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
		if (!HeapTupleIsValid(tup)) /* should not happen */
			elog(ERROR, "cache lookup failed for type %u", typid);
		/*
		  If user added customized basic types, kunlun or mysql won't be able
		  to handle them and will report error, but here we don't know whether
		  the basic type is such an unsupported type and thus we don't care.
		*/
		Form_pg_type tform = (Form_pg_type) GETSTRUCT(tup);
		if (tform->typtype == TYPTYPE_BASE && tform->typbasetype == InvalidOid)
		{
			ReleaseSysCache(tup);
			return typid;
		}

		if (tform->typtype != TYPTYPE_DOMAIN)
		{
			ReleaseSysCache(tup);
			return InvalidOid;
		}

		Assert(tform->typtype != TYPTYPE_BASE &&
			   tform->typbasetype != InvalidOid);
		typid = tform->typbasetype;
		ReleaseSysCache(tup);
	}

	Assert(false); // never reached
	return InvalidOid;
}

/*
TODO:
enforce table uniqueness constraint added in 'alter table' stmt, and multiple
ways of validation (immediate, deferred, etc) of such constraints. This probably
has been transformed to a 'create index' stmt so we need to make sure existing
code work in this case. validation is a challenge, try not pull data up, e.g.
by creating unique index in storage shards.

Note: column uniqueness can only be specified as table constraint except in
'alter table add column' which we have handled.
*/

static void print_str_list(StringInfo str, List *ln, char seperator)
{
	ListCell   *clist;
	int i = 0;

	foreach(clist, ln)
	{
		if (i++ > 0)
			appendStringInfoChar(str, seperator);
		Value *valnode = (Value*)lfirst(clist);
		Assert(valnode->type == T_String);
		appendStringInfoString(str, (const char *)valnode->val.str);
	}
}

/*
  @retval true if successful, false on failure.
*/
static bool print_expr_list(StringInfo str, List *exprlist)
{
	ListCell   *clist;
	int i = 0;

	foreach(clist, exprlist)
	{
		if (i++ > 0)
			appendStringInfoChar(str, ',');
		if (!PrintParserTree(str, (Node*)lfirst(clist)))
			return false;
	}
	return true;
}

/*
  @retval true if successful, false on failure.
*/
static bool PrintParserTree(StringInfo str, Node*val)
{
	bool ret = true;

	if (IsA(val, A_Const))
	{
		A_Const *cval = (A_Const*)val;
		switch (cval->val.type)
		{
	    case T_Integer:
			appendStringInfo(str, "%d", intVal(&cval->val));
			break;
	    case T_Float:
			appendStringInfo(str, "%g", floatVal(&cval->val));
			break;
	    case T_String:
			appendStringInfo(str, "'%s'", strVal(&cval->val));
			break;
	    case T_BitString:
			appendStringInfo(str, "'%s'", strVal(&cval->val));
			break;
	    case T_Null:
			appendStringInfoString(str, "NULL");
			break;
		default:
			Assert(false);
			ret = false;
			break;
		}
	}
	else if (IsA(val, FuncCall))
	{
		FuncCall *fc = (FuncCall*)val;

		print_str_list(str, fc->funcname, '.');
		appendStringInfoChar(str, '(');
		ret = print_expr_list(str, fc->args);
		appendStringInfoChar(str, ')');
		/*
		dzw: for now we don't need to handle mysql grammer(func calls, exprs)
		when outputing a parser tree.

		else if (mysql_has_func(fcname))
		{
			appendStringInfoString(str, fcname);
			appendStringInfoChar(str, '(');
			while (true)
			{
				PrintParserTree(str, linitial(fc->args));
			}
			appendStringInfoChar(str, ')');
		}
		*/
		/*
		  We can handle nextval() and mysql functions.
		*/
	}
	else if (IsA(val, A_Expr))
	{
		A_Expr *expr = (A_Expr *)val;
		const char *expr_name = strVal(linitial(expr->name));
		switch(expr->kind)
		{
		case AEXPR_OP:
			if (expr->lexpr)
				ret = PrintParserTree(str, expr->lexpr);
			if (!ret) goto end;

			print_str_list(str, expr->name, '.');
			if (expr->rexpr)
				ret = PrintParserTree(str, expr->rexpr);
			if (!ret) goto end;

			break;                   /* normal operator */
		case AEXPR_OP_ANY:
		case AEXPR_OP_ALL:
			if (expr->lexpr)
				ret = PrintParserTree(str, expr->lexpr);
			if (!ret) goto end;

			print_str_list(str, expr->name, '.');
			if (expr->kind == AEXPR_OP_ANY)
				appendStringInfoString(str, " ANY(");
			else
				appendStringInfoString(str, " ALL(");
			if (expr->rexpr)
				ret = PrintParserTree(str, expr->rexpr);
			if (!ret) goto end;

			appendStringInfoChar(str, ')');
			break;               /* scalar op ALL (array) */
		case AEXPR_DISTINCT:
			if (expr->lexpr)
				ret = PrintParserTree(str, expr->lexpr);
			if (!ret) goto end;

			appendStringInfoString(str, " IS DISTINCT FROM ");
			if (expr->rexpr)
				ret = PrintParserTree(str, expr->rexpr);
			if (!ret) goto end;

			break;             /* IS DISTINCT FROM - name must be "=" */
		case AEXPR_NOT_DISTINCT:
			if (expr->lexpr)
				ret = PrintParserTree(str, expr->lexpr);
			if (!ret) goto end;

			appendStringInfoString(str, " IS NOT DISTINCT FROM ");
			if (expr->rexpr)
				ret = PrintParserTree(str, expr->rexpr);
			if (!ret) goto end;

			break;         /* IS NOT DISTINCT FROM - name must be "=" */
		case AEXPR_NULLIF:
			break;               /* NULLIF - name must be "=" */
		case AEXPR_OF:
			break;                   /* IS [NOT] OF - name must be "=" or "<>" */
		case AEXPR_IN:
			if (expr->lexpr)
				ret = PrintParserTree(str, expr->lexpr);
			if (!ret) goto end;

			appendStringInfo(str, " %s IN ", expr_name[0] == '=' ? "":"NOT");
			if (expr->rexpr)
				ret = PrintParserTree(str, expr->rexpr);
			if (!ret) goto end;

			break;                   /* [NOT] IN - name must be "=" or "<>" */
		case AEXPR_LIKE:
			if (expr->lexpr)
				ret = PrintParserTree(str, expr->lexpr);
			if (!ret) goto end;

			appendStringInfo(str, " %s LIKE ", expr_name[0] == '~' ? "":"NOT");
			if (expr->rexpr)
				ret = PrintParserTree(str, expr->rexpr);
			if (!ret) goto end;

			break;                 /* [NOT] LIKE - name must be "~~" or "!~~" */
		case AEXPR_ILIKE:
			if (expr->lexpr)
				ret = PrintParserTree(str, expr->lexpr);
			if (!ret) goto end;

			appendStringInfo(str, " %s ILIKE ", expr_name[0] == '~' ? "":"NOT");
			if (expr->rexpr)
				ret = PrintParserTree(str, expr->rexpr);
			if (!ret) goto end;

			break;                /* [NOT] ILIKE - name must be "~~*" or "!~~*" */
		case AEXPR_SIMILAR:
			if (expr->lexpr)
				ret = PrintParserTree(str, expr->lexpr);
			if (!ret) goto end;

			appendStringInfo(str, " %s SIMILAR TO ", expr_name[0] == '~' ? "":"NOT");
			if (expr->rexpr)
				ret = PrintParserTree(str, expr->rexpr);
			if (!ret) goto end;

			break;              /* [NOT] SIMILAR - name must be "~" or "!~" */
		case AEXPR_BETWEEN:
		case AEXPR_NOT_BETWEEN:
			if (expr->lexpr)
				ret = PrintParserTree(str, expr->lexpr);
			if (!ret) goto end;

			appendStringInfo(str, " %s BETWEEN ", expr->kind == AEXPR_NOT_BETWEEN ? "NOT":"");
			Assert (expr->rexpr && IsA(expr->rexpr, List));
			ret = PrintParserTree(str, linitial((List*)expr->rexpr));
			if (!ret) goto end;

			appendStringInfoString(str, " AND ");
			ret = PrintParserTree(str, lsecond((List*)expr->rexpr));
			if (!ret) goto end;

			break;
		case AEXPR_BETWEEN_SYM:
		case AEXPR_NOT_BETWEEN_SYM:
			if (expr->lexpr)
				PrintParserTree(str, expr->lexpr);
			if (!ret) goto end;

			appendStringInfo(str, " %s BETWEEN SYMMETRIC ", expr->kind == AEXPR_NOT_BETWEEN_SYM ? "NOT":"");
			Assert (expr->rexpr && IsA(expr->rexpr, List));
			ret = PrintParserTree(str, linitial((List*)expr->rexpr));
			if (!ret) goto end;

			appendStringInfoString(str, " AND ");
			ret = PrintParserTree(str, lsecond((List*)expr->rexpr));
			if (!ret) goto end;

			break;
		default:
			break;
		}
	}
	else if (IsA(val, ColumnRef))
	{
		ColumnRef*colref = (ColumnRef*)val;
		print_str_list(str, colref->fields, ',');
	}
	else if (IsA(val, A_Star))
	{
		appendStringInfoChar(str, '*');
	}
	else if (IsA(val, List))
	{
		List *exprlist = (List*)val;
		ret = print_expr_list(str, exprlist);
	}
	else
		ret = false;

end:
	return ret;
}

void build_column_data_type(StringInfo str, Oid typid,
	int32 typmod, Oid collation)
{
	if (VARCHAROID == typid && typmod == -1)
		appendStringInfo(str, "%s", format_type_remote(TEXTOID)); // pg extension
	else
	{
		const char *typname = format_type_remote(typid);
		if (!typname)
			ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
					errmsg("Kunlun-db: Not supported type (%u).", typid)));
		else
			appendStringInfo(str, "%s", typname);
	}

	if (typmod != -1)
	{
		if (typid == NUMERICOID)
		{
			int precision = ((typmod - VARHDRSZ) >> 16) & 0xffff;
			int scale = (typmod - VARHDRSZ) & 0xffff;
			if (precision > 65)
				ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
						errmsg("Kunlun-db: Remote storage node requires NUMERIC precision <= 65")));
			if (scale > 30)
				ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
						errmsg("Kunlun-db: Remote storage node requires NUMERIC scale <= 30")));

			if (scale > 0)
				appendStringInfo(str, "(%d,%d)", precision, scale);
			else
				appendStringInfo(str, "(%d)", precision);
		}
		else
		{
			appendStringInfo(str, "(%d)", typmod);
		}

	}
	else if (typid == NUMERICOID)
	{
		appendStringInfo(str, "(%d, %d)", 65, 20);
	}
	else if  (typid == CASHOID)
	{
		appendStringInfo(str, "(%d,%d)", 65, 8); // money is transformed to numeric(65,8).
	}

	if (collation != InvalidOid)
	{
		/*
		 * Map C and POSIX collations to UTF8_bin, any other collate specs
		 * go to 'default' charset&collation, i.e. utf8 and its default
		 * collation in mysql.
		 * */
		if (collation == 950) // "C"
			appendStringInfoString(str, " COLLATE utf8_bin");
		else if (collation == 951) // "POSIX"
			appendStringInfo(str, " COLLATE utf8_bin");
	}
}

/*
When we rename a heap relation's column name from CN1 to CN2, its index
relations' column names are not updated together,
in pg_attribute the index rel's column names are not updated, it's still CN1.
This is original pg's bug.
We need to update such rows and the Relation handle will be invalidated
automatically.

One 'alter table rename column' stmt alwasy only rename one column.
*/
void update_colnames_indices(Relation attrelation, Relation targetrelation,
	int attnum, const char *oldattname, const char *newattname)
{
	ListCell   *ind;
	List *indl = RelationGetIndexList(targetrelation);
	foreach (ind, indl)
	{
		Oid indid = lfirst_oid(ind);
		Datum *keys;
		int nkeys;

		get_indexed_cols(indid, &keys, &nkeys);
		for (int i = 0; i < nkeys; i++)
		{
			if (keys[i] == attnum)
			{
				update_index_attrname(attrelation, targetrelation, indid, i+1,
					oldattname, newattname, false);
				break; // an index never references a column more than once.
			}
		}
	}
}

/*
  Fetch from pg_index.indkey the indexed column numbers.
*/
static void get_indexed_cols(Oid indexId, Datum **keys, int *nKeys)
{
	bool isnull;
	Datum       cols;

	Assert(keys && nKeys);
	Assert(indexId != InvalidOid);

	/* Build including column list (from pg_index.indkeys) */
	HeapTuple indtup = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexId));
	if (!HeapTupleIsValid(indtup))
		elog(ERROR, "cache lookup failed for index %u", indexId);

	cols = SysCacheGetAttr(INDEXRELID, indtup,
						   Anum_pg_index_indkey, &isnull);
	if (isnull)
	{
		ReleaseSysCache(indtup);
		elog(ERROR, "null indkey for index %u", indexId);
	}

	deconstruct_array(DatumGetArrayTypeP(cols),
					  INT2OID, 2, true, 's',
					  keys, NULL, nKeys);
	ReleaseSysCache(indtup);
}

static void
update_index_attrname(Relation attrelation, Relation targetrelation,
	Oid indid, int16 attnum,
	const char *oldattname, const char *newattname,
	bool check_attname_uniquness)
{
	if (attnum <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Kunlun-db: cannot rename system column \"%s\"",
						oldattname)));
	HeapTuple atttup = SearchSysCache2(ATTNUM,
						 ObjectIdGetDatum(indid), Int16GetDatum(attnum));
	if (!HeapTupleIsValid(atttup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("Kunlun-db: column \"%s\" does not exist",
						oldattname)));

	Form_pg_attribute attform = (Form_pg_attribute) GETSTRUCT(atttup);
	/*
	  Concurrent rename stmts would be blocked by current transaction which is
	  the winner for the update of the target attr row of main table.
	*/
	Assert(strcmp(attform->attname.data, oldattname) == 0);
	if (strcmp(attform->attname.data, oldattname))
	{
		ReleaseSysCache(atttup);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("Kunlun-db: column \"%s\" (%d) does not exist in index relation (%u), the attname for attnum(%d) is %s",
						oldattname, attnum, indid, attnum, attform->attname.data)));
	}

	/* new name should not already exist in main table.
	   normally the same op against main table
	   has already checked for uniqueness and the new name is already occupied,
	   and recheck will fail instead.
	if (check_attname_uniquness)
	{
		(void) check_for_column_name_collision(targetrelation, newattname, false);
	}
	*/

	/* apply the update */
	namestrcpy(&(attform->attname), newattname);

	CatalogTupleUpdate(attrelation, &atttup->t_self, atttup);
	ReleaseSysCache(atttup);
}

