/*-------------------------------------------------------------------------
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/printtup.h"
#include "access/remotetup.h"
#include "access/sysattr.h"
#include "catalog/heap.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_map.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_proc_map.h"
#include "commands/sequence.h"
#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "optimizer/clauses.h"
#include "parser/parsetree.h"
#include "pgtime.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/arrayaccess.h"

bool remote_print_warning = true;

#undef APPEND_CHAR
#undef APPEND_EXPR
#undef APPEND_STR
#undef APPEND_STR_FMT

#define APPEND_STR_CHAR(str, c)	\
	(rpec->noprint ? 1 : appendStringInfoChar(str, c))

#define APPEND_STR_INFO(str, ...) \
	(rpec->noprint ? 1 : appendStringInfo(str, __VA_ARGS__))

#define APPEND_STR_INFO_STR(str, s) \
	(rpec->noprint ? 1 : appendStringInfoString(str, s))

#define APPEND_CHAR(c)                        \
	do                                    \
	{                                     \
		APPEND_STR_CHAR(str, c); \
		nw++;                         \
	} while (0)

#define APPEND_EXPR(expr)                                        \
	do                                                       \
	{                                                        \
		nw1 = snprint_expr(str, ((Expr *)(expr)), rpec); \
		if (nw1 < 0)                                     \
			return nw1;                              \
		nw += nw1;                                       \
	} while (0)

#define APPEND_STR(str0)                                   \
	do                                                 \
	{                                                  \
		nw1 = APPEND_STR_INFO_STR(str, (str0)); \
		nw += nw1;                                 \
	} while (0)

#define APPEND_STR_FMT(fmt, str0)                       \
	do                                              \
	{                                               \
		nw1 = APPEND_STR_INFO(str, fmt, str0); \
		nw += nw1;                              \
	} while (0)

#define APPEND_STR_FMT2(fmt, arg0, arg1)                      \
	do                                                    \
	{                                                     \
		nw1 = APPEND_STR_INFO(str, fmt, arg0, arg1); \
		nw += nw1;                                    \
	} while (0)

#define APPEND_FUNC2(funcname, arg1, arg2)                       \
	do                                                       \
	{                                                        \
		nw1 = APPEND_STR_INFO_STR(str, funcname);     \
		APPEND_STR_CHAR(str, '(');                  \
		nw += nw1 + 3;                                   \
		nw1 = snprint_expr(str, ((Expr *)(arg1)), rpec); \
		if (nw1 < 0)                                     \
			return nw1;                              \
		nw += nw1;                                       \
		APPEND_STR_CHAR(str, ',');                  \
		nw1 = snprint_expr(str, ((Expr *)(arg2)), rpec); \
		if (nw1 < 0)                                     \
			return nw1;                              \
		nw += nw1;                                       \
		APPEND_STR_CHAR(str, ')');                  \
	} while (0)

#define APPEND_FUNC1(funcname, arg1)                             \
	do                                                       \
	{                                                        \
		nw1 = APPEND_STR_INFO_STR(str, funcname);     \
		APPEND_STR_CHAR(str, '(');                  \
		nw += nw1 + 2;                                   \
		nw1 = snprint_expr(str, ((Expr *)(arg1)), rpec); \
		if (nw1 < 0)                                     \
			return nw1;                              \
		nw += nw1;                                       \
		APPEND_STR_CHAR(str, ')');                  \
	} while (0)

// the 3rd argument is a string.
#define APPEND_FUNC3_3s(funcname, arg1, arg2, arg3)              \
	do                                                       \
	{                                                        \
		nw1 = APPEND_STR_INFO_STR(str, funcname);     \
		APPEND_STR_CHAR(str, '(');                  \
		nw += nw1 + 4;                                   \
		nw1 = snprint_expr(str, ((Expr *)(arg1)), rpec); \
		if (nw1 < 0)                                     \
			return nw1;                              \
		nw += nw1;                                       \
		APPEND_STR_CHAR(str, ',');                  \
		nw1 = snprint_expr(str, ((Expr *)(arg2)), rpec); \
		if (nw1 < 0)                                     \
			return nw1;                              \
		nw += nw1;                                       \
		APPEND_STR_CHAR(str, ',');                  \
		nw1 = APPEND_STR_INFO_STR(str, arg3);         \
		nw += nw1;                                       \
		APPEND_STR_CHAR(str, ')');                  \
	} while (0)

#define CONST_STR_LEN(conststr) conststr,(sizeof(conststr)-1)

#define MATCH_FUNC2(n, p1, p2) \
	(strcmp(func, n) == 0 && nargs == 2 && argtypes[0] == p1 && argtypes[1] == p2)

#define MATCH_FUNC3(n, p1, p2, p3) \
	(strcmp(func, n) == 0 && nargs == 3 && argtypes[0] == p1 && argtypes[1] == p2 && argtypes[2] == p3)

static const char *
get_var_attname(const Var *var, const List *rtable, bool fullname, char *buff, size_t buffsize);
static int eval_nextval_expr(StringInfo str, RemotePrintExprContext *rpec, NextValueExpr *nve);
static int append_expr_list(StringInfo str, List *l, int skip, RemotePrintExprContext *rpec);
static ParamExternData* eval_extern_param_val(ParamListInfo paramInfo, int paramId);
int snprint_const_type_value(StringInfo str, bool isnull, Oid type,
			   Datum value, RemotePrintExprContext *rpec);

int output_const_type_value(StringInfo str, bool isnull, Oid type, Datum value);

inline static const char *get_rel_aliasname(RangeTblEntry *rte)
{
    return rte->alias && rte->alias->aliasname ? rte->alias->aliasname :
    	(rte->eref ? rte->eref->aliasname : NULL);
}

inline static bool type_is_array_category(Oid typid)
{
	char typcat;
	bool preferred;
	get_type_category_preferred(typid, &typcat, &preferred);
	return typcat == TYPCATEGORY_ARRAY;
}

static int
snprint_mysql_func(StringInfo str, RemotePrintExprContext *rpec, const char *format, List *args)
{
	const char *p = format;
	char *endptr;
	int nw = 0, nw1;
	while (*p)
	{
		/* %s means placeholder of args */
		if (*p == '$' && isdigit(*(p + 1)))
		{
			++p;
			int n = strtol(p, &endptr, 10);
			if (n > list_length(args))
			{
				elog(ERROR, "Arguments number not matched(%d) for '%s'", list_length(args), format);
			}
			if ((nw1 = snprint_expr(str, (Expr *)list_nth(args, n-1), rpec)) < 0)
				return -1;
			nw += nw1;
			p = endptr;
		}
		else
		{
			++nw;
			APPEND_STR_CHAR(str, *p);
			++p;
		}
	}

	return nw;
}

static bool
get_proc_format(Oid funcid, char *buff, size_t len)
{
	HeapTuple tup1, tup2;
	bool found = false;

	tup1 = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
	if (HeapTupleIsValid(tup1))
	{
		Form_pg_proc proc_tup = (Form_pg_proc)GETSTRUCT(tup1);
		tup2 = SearchSysCache3(PROCMAP,
				       PointerGetDatum(&proc_tup->proname),
				       PointerGetDatum(&proc_tup->proargtypes),
				       ObjectIdGetDatum(proc_tup->pronamespace));
		if (HeapTupleIsValid(tup2))
		{
			if (((Form_pg_proc_map)GETSTRUCT(tup2))->enable)
			{
				bool isnull;
				Datum format = SysCacheGetAttr(PROCMAP,
							       tup2, Anum_pg_proc_map_mysql, &isnull);
				if (isnull == false)
				{
					char *str = TextDatumGetCString(format);
					(void)strncpy(buff, str, len);
					found = true;
					pfree(str);
				}
			}

			ReleaseSysCache(tup2);
		}
		ReleaseSysCache(tup1);
	}

	return found;
}


/**
 * @brief Check if mysql support this proc 
 */
static bool is_mysql_proc(Oid funcoid)
{
	return get_proc_format(funcoid, NULL, 0);
}

/**
 * @brief Check if mysql support this operator
 */
static bool is_mysql_operator(Oid oproid)
{
	HeapTuple tuple;
	bool result = false;;

	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(oproid));
	if (HeapTupleIsValid(tuple))
	{
		Form_pg_operator optuple = (Form_pg_operator)GETSTRUCT(tuple);
		result = is_mysql_proc(optuple->oprcode);
		ReleaseSysCache(tuple);
	}

	return result;
}

static int 
snprint_op_expr(StringInfo str, RemotePrintExprContext *rpec, OpExpr *expr)
{
	bool err = false;
	size_t saved = str ? str->len : 0;
	char buff[128];
	HeapTuple tp;
	Form_pg_operator optup;

	tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(expr->opno));
	Assert(HeapTupleIsValid(tp));

	PG_TRY();
	{
		optup = (Form_pg_operator)GETSTRUCT(tp);
		if (get_proc_format(optup->oprcode, buff, sizeof(buff)))
		{
			err = (snprint_mysql_func(str, rpec, buff, expr->args) < 0);
		}
		else
		{
			err = true;
			if (remote_print_warning)
			{
				elog(WARNING, "Serialize operator '%s' (%u, %u) to remote function/operator failed",
				     get_opname(expr->opno), optup->oprleft, optup->oprright);
			}
		}
	}
	PG_CATCH();
	{
		err = true;
	}
	PG_END_TRY();
	ReleaseSysCache(tp);

	if (err)
	{
		if (str)
		{
			str->len = saved;
			if (str->data)
				str->data[saved] = '\0';
		}
		return -1;
	}
	return str ? (str->len - saved) : 1;
}

static int
snprint_func_expr(StringInfo str, RemotePrintExprContext *rpec, FuncExpr *expr)
{
	bool err;
	int saved = str ? str->len : 0;
	char buff[128];

	PG_TRY();
	{
		if (get_proc_format(expr->funcid, buff, sizeof(buff)))
		{
			err = (snprint_mysql_func(str, rpec, buff, expr->args) < 0);
		}
		else
		{
			err = true;
			if (remote_print_warning)
			{
				elog(WARNING, "Serialize function '%s' (%d) to remote operator/function failed",
				     get_func_name(expr->funcid), expr->funcid);
			}
		}
	}
	PG_CATCH();
	{
		err = true;
	}
	PG_END_TRY();

	if (err)
	{
		if (str)
		{
			str->len = saved;
			if (str->data)
				str->data[saved] = '\0';
		}
		return -1;
	}
	return str ? (str->len - saved) : 1;
}

static bool
contain_non_const_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Const))
		return false;
	if (IsA(node, List))
		return expression_tree_walker(node, contain_non_const_walker, context);
	/* Otherwise, abort the tree traversal and return true */
	return true;
}

static bool
function_is_safe(Oid funcid, bool estimate)
{
	char		provolatile = func_volatile(funcid);

	/*
	 * Ordinarily we are only allowed to simplify immutable functions. But for
	 * purposes of estimation, we consider it okay to simplify functions that
	 * are merely stable; the risk that the result might change from planning
	 * time to execution time is worth taking in preference to not being able
	 * to estimate the value at all.
	 */
	if (provolatile == PROVOLATILE_IMMUTABLE)
		return true;
	if (estimate && provolatile == PROVOLATILE_STABLE)
		return true;
	return false;
}

static int
eval_snprint_func(StringInfo str, RemotePrintExprContext *rpec, FuncExpr *func)
{
	int nw, nw1;
	Oid resulttype = func->funcresulttype;

	if (func->funcretset == false &&	/* not returning set of tuples */
	    contain_non_const_walker((Node *)func->args, NULL) == false && /* have no non-const arguments */
	    my_output_funcoid(resulttype, NULL) != InvalidOid && /* result can be pushed down */
	    function_is_safe(func->funcid, true)) /* function is safe to be pushed */
	{
		if (rpec->noprint)
		{
			nw = 1;
		}
		else
		{
			Node *value;
			PlannerInfo foo;
			PlannerGlobal glob;
			glob.boundParams = NULL;
			foo.glob = &glob;

			if ((value = estimate_expression_value(&foo, (Node *)func)) && value != (Node *)func)
			{
				nw = snprint_expr(str, (Expr *)value, rpec);
			}
		}
	}
	else
	{
		nw = -1;
	}

	return nw;
}

static int
snprint_cast_numeric(StringInfo str, RemotePrintExprContext *rpec, FuncExpr *expr)
{
	int typmod = -1;
	bool valid_prec = false;
	int precision = 0;
	int scale = 0;
	int nw=0, nw1;
	if (list_length(expr->args) >1)
	{
		Expr *typmod_expr = (Expr *)lsecond(expr->args);
		if (IsA(typmod_expr, Const))
		{
			typmod = ((Const *)typmod_expr)->constvalue;
			if (typmod > (int32)(VARHDRSZ))
			{
				precision = ((typmod - VARHDRSZ) >> 16) & 0xffff;
				scale = (typmod - VARHDRSZ) & 0xffff;
				valid_prec = (scale < 0 || scale > 30 || precision <= 0 || precision > 65);
			}
		}
	}
	
	APPEND_STR(" CAST(");
	APPEND_EXPR(linitial(expr->args));
	APPEND_STR(" as DECIMAL");
	if (valid_prec)
	{
		if (scale > 0)
			APPEND_STR_FMT2("(%d,%d)", precision, scale);
		else
			APPEND_STR_FMT("(%d)", precision);
	}
	else
		APPEND_STR("(65,20)");

	APPEND_STR(") ");

	return nw;
}

static int
snprint_cast(StringInfo str, RemotePrintExprContext *rpec, FuncExpr *expr)
{
	int nw = 0, nw1;
	const char *castfn;
	Node *precision;
	char *func;
	int nargs;
	Oid *argtypes;

	if (expr->funcformat == COERCE_IMPLICIT_CAST)
	{
		APPEND_EXPR(linitial(expr->args));
	}
	else if (expr->funcformat == COERCE_EXPLICIT_CAST)
	{
		/*
		   Handle speicial type cast with given precision:
		   numeric(numeric, int4);
		   varchar(varchar int4 bool);
		   bpchar(bpchar int4 bool);
		 */

		func = get_func_name(expr->funcid);
		get_func_signature(expr->funcid, &argtypes, &nargs);

		if (MATCH_FUNC2("numeric", NUMERICOID, INT4OID))
		{
			nw = snprint_cast_numeric(str, rpec, expr);
		}
		else if (MATCH_FUNC3("varchar", VARCHAROID, INT4OID, BOOLOID) ||
			 MATCH_FUNC3("bpchar", BPCHAROID, INT4OID, BOOLOID))
		{
			if ((castfn = mysql_can_cast(expr->funcresulttype)) != NULL &&
			    IsA((precision = lsecond(expr->args)), Const))
			{
				APPEND_STR(" CAST(");
				APPEND_EXPR(linitial(expr->args));
				APPEND_STR_FMT(" as %s", castfn);

				APPEND_CHAR('(');
				APPEND_STR_FMT("%u", DatumGetInt32(((Const *)precision)->constvalue) - VARHDRSZ);
				APPEND_CHAR(')');
				APPEND_STR(") ");
			}
			else
			{
				nw = -1;
			}
		}
		else
		{
			nw = -1;
		}
		pfree(func);
		pfree(argtypes);
	}
	else
	{
		nw = -1;
	}

	return nw;
}

static int
snprint_var(StringInfo str, RemotePrintExprContext *rpec, Var* var)
{
	bool done = false;
	int nw = 0, nw1;

	/* Do not support wholerow var */
	if (var->varattno == 0)
		return -1;

	/* No print, trust it is a valid var */
	if (rpec->noprint)
		return 1;
	/**
	 * ON CONFLICT DO UPDATE SET col = EXCLUDED.col
	 * =>
	 * ON DUPLICATE KEY UPDATE col = VALUE(col)
	 */
	if (var->varno == INNER_VAR && rpec->excluded_table_columns)
	{
		if (var->varattno > 0 &&
		    var->varattno <= list_length(rpec->excluded_table_columns))
		{
			Value *colname = list_nth(rpec->excluded_table_columns, var->varattno - 1);
			APPEND_STR(" VALUES(");
			APPEND_STR(colname->val.str);
			APPEND_STR(") ");
			done = true;
		}
	}
	else
	{
		char buff[NAMEDATALEN + 16];
		const char *varname = get_var_attname(var, rpec->rtable, false, buff, sizeof(buff));
		if (varname)
		{
			done = true;
			APPEND_STR(varname);
		}
	}

	return done ? nw : -1;
}

static int
snprint_bool_expr(StringInfo str, RemotePrintExprContext *rpec, BoolExpr *expr)
{
	int nw = 0, nw1;
	ListCell *lc;
	Expr *cond;

	APPEND_CHAR('(');
	foreach (lc, expr->args)
	{
		cond = (Expr *)lfirst(lc);

		/* Perform the appropriate step type */
		switch (expr->boolop)
		{
		case AND_EXPR:
			APPEND_EXPR(cond);
			if (lnext(lc))
				APPEND_STR(" AND ");
			break;
		case OR_EXPR:
			APPEND_EXPR(cond);
			if (lnext(lc))
				APPEND_STR(" OR ");
			break;
		case NOT_EXPR:
			APPEND_STR("NOT ");
			APPEND_EXPR(cond);
			break;
		default:
			nw = -1;
			break;
		}
	}
	APPEND_CHAR(')');

	return nw;
}

static int
snprint_nulltest_expr(StringInfo str, RemotePrintExprContext *rpec, NullTest *expr)
{
	int nw=0, nw1;
	const char *teststr = 0;
	if (expr->nulltesttype == IS_NULL)
	{
		teststr = " IS NULL";
	}
	else if (expr->nulltesttype == IS_NOT_NULL)
	{
		teststr = " IS NOT NULL";
	}
	else
	{
		elog(ERROR, "unrecognized nulltesttype: %d",
		     (int)expr->nulltesttype);
	}
	APPEND_CHAR('(');
	APPEND_EXPR(expr->arg);
	APPEND_STR(teststr);
	APPEND_CHAR(')');

	return nw;
}
static int
snprint_test_expr(StringInfo str, RemotePrintExprContext *rpec,  BooleanTest *expr)
{
	int nw = 0, nw1;
	const char *teststr = 0;
	switch (expr->booltesttype)
	{
	case IS_TRUE:
		teststr = " IS TRUE";
		break;
	case IS_NOT_TRUE:
		teststr = " IS NOT TRUE";
		break;
	case IS_FALSE:
		teststr = " IS FALSE";
		break;
	case IS_NOT_FALSE:
		teststr = " IS NOT FALSE";
		break;
	case IS_UNKNOWN:
		teststr = " IS NULL";
		break;
	case IS_NOT_UNKNOWN:
		teststr = " IS NOT NULL";
		break;
	default:
		elog(ERROR, "unrecognized booltesttype: %d",
		     (int)expr->booltesttype);
	}
	APPEND_CHAR('(');
	APPEND_EXPR(expr->arg);
	APPEND_STR(teststr);
	APPEND_CHAR(')');

	return nw;
}

static int
snprint_param_expr(StringInfo str, RemotePrintExprContext *rpec, Param *param)
{
	int nw = 0, nw1;
	bool isnull;
	Oid paramtype;
	Datum pval;

	if  (my_output_funcoid(param->paramtype, NULL) == InvalidOid)
		return -1;

	if (param->paramkind == PARAM_EXEC)
	{
		/* Check if internal parameter is not ok or caller expects no internal params*/
		if (!rpec->rpec_param_exec_vals || !rpec->exec_param_quals)
			return -2;
		ParamExecData *exec_data = rpec->rpec_param_exec_vals + param->paramid;
		Assert(exec_data);
		
		SubPlanState *node = (SubPlanState*)exec_data->execPlan;
		if (node)
		{
			/* Check if it's a parameterized subplsn */
			if (!rpec->estate || list_length(node->subplan->parParam) > 0)
				return -2;

			ExecSetParamPlan(exec_data->execPlan,
					GetPerTupleExprContext(rpec->estate));
		}
		isnull = exec_data->isnull;
		paramtype = param->paramtype;
		pval = exec_data->value;
	}
	else if (param->paramkind == PARAM_EXTERN)
	{
		/* External parameters is not ready yet, print as a placeholder '?'*/
		if (!rpec->rpec_param_list_info)
		{
			// APPEND_STR("?");
			// return nw;
			return -1;
		}
		ParamExternData *extern_data = eval_extern_param_val(rpec->rpec_param_list_info, param->paramid);
		Assert(extern_data != NULL);
		isnull = extern_data->isnull;
		paramtype = extern_data->ptype;
		pval = extern_data->value;
	}
	else
	{
		// PARAM_SUBLINK and PARAM_MULTIEXPR params are
		// always converted to PARAM_EXEC during planning.
		Assert(false);
	}
	
	nw = snprint_const_type_value(str, isnull, paramtype, pval, rpec);
	return nw;
}

static int
snprint_minmax_expr(StringInfo str, RemotePrintExprContext *rpec, MinMaxExpr *expr)
{
	int nw = 0, nw1;

	if (expr->op == IS_GREATEST)
		APPEND_STR(" GREATEST(");
	else if (expr->op == IS_LEAST)
		APPEND_STR(" LEAST(");

	if ((nw1 = append_expr_list(str, expr->args, 0, rpec)) < 0)
		return nw1;
	nw += nw1;
	APPEND_CHAR(')');

	return nw;
}

static int
snprint_coalesce_expr(StringInfo str, RemotePrintExprContext *rpec, CoalesceExpr *expr)
{
	int nw=0, nw1;
	APPEND_STR(" Coalesce(");
	if ((nw1 = append_expr_list(str, expr->args, 0, rpec)) < 0)
		return nw1;
	nw += nw1;
	APPEND_CHAR(')');

	return nw;
}

static int
snprint_case_expr(StringInfo str, RemotePrintExprContext *rpec, CaseExpr *expr)
{
	int nw = 0, nw1;
	bool equal_op = false;
	ListCell *lc = NULL;

	APPEND_STR(" CASE ");
	if (expr->arg)
	{
		APPEND_EXPR(expr->arg);
		equal_op = (expr->casetype != InvalidOid);
	}

	foreach (lc, expr->args)
	{
		CaseWhen *cw = (CaseWhen *)lfirst(lc);

		APPEND_STR(" WHEN ");
		if (equal_op)
		{
			if (IsA(cw->expr, OpExpr))
			{
				/**
				 * According to comments in CaseExpr definition, cw->expr must
				 * be a CaseTestExpr = compexpr expression in this case.
				 * However, there are
				 * situations where pg doesn't handle correctly, e.g. when ce->arg
				 * is a NullTest node, and we'd have to error out.
				 */
				OpExpr *eqexpr = (OpExpr *)cw->expr;
				APPEND_EXPR(lsecond(eqexpr->args));
			}
			else
			{
				/* Should be testexpr = caseexpr */
				return -1;
			}
		}
		else
		{
			APPEND_EXPR(cw->expr);
		}

		APPEND_STR(" THEN ");
		APPEND_EXPR(cw->result);
	}

	if (expr->defresult)
	{
		APPEND_STR(" ELSE ");
		APPEND_EXPR(expr->defresult);
	}

	APPEND_STR(" END ");

	return nw;
}

static int
snprint_scalararrayop(StringInfo str, RemotePrintExprContext *rpec, ScalarArrayOpExpr *scoe)
{
	/**
	 * 'col IN (expr1, expr2, ..., exprN)' is valid SQL expr, but
	 * 'col comp-opr ANY(expr1, expr2, ..., exprN)'
	 * isn't, for both mysql and pg.
	 *
	 * For mysql-8.0.19 and newer, we can convert it to
	 * 'col comp-opr ANY(VALUES ROW(expr1), ROW(expr2),..., ROW(exprN))',
	 * especially the 'exprN' here can be a list, such as '1,2,3', to
	 * do a 'row subquery' as in mysql's terms, so we can accept 1-D or 2D
	 * array here when we upgrade to newer mysql.
	 * but for current kunlun-storage(based on mysql-8.0.18), we have to
	 * reject such exprs so we only send 'col IN(expr list)' grammar.
	 *
	 * pg supports 'col comp-opr ANY(array-constructor)' grammar so
	 * that ' a > ANY(ARRAY[1,2,3])' is valid for pg but this is not
	 * standard SQL and this is how we would recv such an unsupported expr
	 * here because if the subquery is an select, pg will do semijoin.
	 */
	int nw=0, nw1;
	const char *opname = get_opname(scoe->opno);
	const char *opstr = NULL;
	if (scoe->useOr && strcmp(opname, "=") == 0)
		opstr = " IN";
	else if (!scoe->useOr && strcmp(opname, "<>") == 0)
		opstr = " NOT IN";
	else
		return -1;
	
	APPEND_EXPR(linitial(scoe->args));
	APPEND_STR(opstr);
	if ((nw1 = append_expr_list(str, scoe->args, 1, rpec)) < 0)
	{
		return nw1;
	}
	nw += nw1;

	return nw;
}

static int
snprint_nextval(StringInfo str, RemotePrintExprContext *rpec, NextValueExpr *expr)
{
	int nw = 0, nw1;
	if (rpec->consume_sequence)
		nw += eval_nextval_expr(str, rpec, expr);
	else
		nw = -1;

	return nw;
}

static int
snprint_rowexpr(StringInfo str, RemotePrintExprContext *rpec, RowExpr *rowexpr)
{
	// int nw = 0;
	// if ((nw = append_expr_list(str, rowexpr->args, 0, rpec)) >= 0)
	// {
	// 	rpec->num_vals = list_length(rowexpr->args);
	// }

	// return nw;
	return -1;
}

static int
snprint_list_expr(StringInfo str, RemotePrintExprContext *rpec, List *list)
{
	int nw=0, nw1;
	APPEND_CHAR('(');
	if ((nw1 = append_expr_list(str, list, 0, rpec)) < 0)
		return nw1;
	nw += nw1;
	APPEND_CHAR(')');

	return nw;
}

static int
snprint_agg_expr(StringInfo str, RemotePrintExprContext *rpec, Aggref *aggref)
{
	return -1;	
}

static int
snprint_placeholdervar(StringInfo str, RemotePrintExprContext *rpec, PlaceHolderVar *phv)
{
	int nw = 0, nw1;
	APPEND_EXPR(phv->phexpr);

	return nw;
}

static int
snprint_sqlvalue_func(StringInfo str, SQLValueFunction *svfo, RemotePrintExprContext *rpec)
{
	int nw = -1;
	
	switch (svfo->op)
	{
	case SVFOP_CURRENT_DATE:
	case SVFOP_CURRENT_TIME:
	case SVFOP_CURRENT_TIME_N:
	case SVFOP_CURRENT_TIMESTAMP:
	case SVFOP_CURRENT_TIMESTAMP_N:
	case SVFOP_LOCALTIME:
	case SVFOP_LOCALTIME_N:
	case SVFOP_LOCALTIMESTAMP:
	case SVFOP_LOCALTIMESTAMP_N:
	case SVFOP_CURRENT_ROLE:
	case SVFOP_CURRENT_USER:
	case SVFOP_USER:
	case SVFOP_SESSION_USER:
	case SVFOP_CURRENT_CATALOG:
	case SVFOP_CURRENT_SCHEMA:
	{
		Node *value;
		PlannerInfo foo;
		PlannerGlobal glob;
		glob.boundParams = NULL;
		foo.glob = &glob;

		if ((value = estimate_expression_value(&foo, (Node *)svfo)) && value != (Node *)svfo)
		{
			nw = snprint_expr(str, (Expr *)value, rpec);
		}
		break;
	}
	default:
		nw = -1;
		break;
	}

	return nw;
}

static int
snprint_coerce_io(StringInfo str, CoerceViaIO *expr, RemotePrintExprContext *rpec)
{
	char typcategory;
	bool typisprefered;
	bool valid = false;
	Oid typoid = expr->resulttype;
	get_type_category_preferred(expr->resulttype, &typcategory, &typisprefered);

	/* Coerce to string or from string ?*/
	bool tostring = false;
	if (typcategory == TYPCATEGORY_STRING)
	{
		typoid = exprType((Node *)expr->arg);
		tostring = true;
	}
	else
	{
		/* Check it is coerce converted from string */
		Oid argtypoid = exprType((Node *)expr->arg);
		get_type_category_preferred(argtypoid, &typcategory, &typisprefered);
		if (typcategory != TYPCATEGORY_STRING)
			return -1;
	}

	/* Check if the type can be coerce to/from string */
	HeapTuple tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typoid));
	if (HeapTupleIsValid(tup))
	{
		Form_pg_type typTup = (Form_pg_type)GETSTRUCT(tup);
		HeapTuple tp = SearchSysCache2(TYPEMAP,
					       PointerGetDatum(&typTup->typname),
					       ObjectIdGetDatum(typTup->typnamespace));

		if (HeapTupleIsValid(tp))
		{
			Form_pg_type_map typmapTup = (Form_pg_type_map)GETSTRUCT(tp);
			valid = tostring ? typmapTup->coerciontostr : typmapTup->coercionfromstr;
			ReleaseSysCache(tp);
		}
		ReleaseSysCache(tup);
	}

	int nw = 0, nw1;
	if (!valid)
	{
		nw = -1;
	}
	else if (tostring)
	{
		//  in case of "123::text = ' 123'""
		APPEND_STR("cast(");
		APPEND_EXPR(expr->arg);
		APPEND_STR(" as char)");
	}
	else
	{
		const char *sztype = mysql_can_cast(typoid);
		if (sztype)
		{
			APPEND_STR("cast(");
			APPEND_EXPR(expr->arg);
			APPEND_STR_FMT(" as %s)", sztype);
		}
		else
		{
			nw = -1;
		}
	}

	return nw;
}

int snprint_expr(StringInfo str, const Expr *expr, RemotePrintExprContext *rpec)
{
	/* in case of infinite recursive calls. */
	check_stack_depth();

	int nw = 0, nw1 = 0;

	if (expr == NULL)
		return 0;

	switch(nodeTag(expr))
	{
	case T_Var:
	{
		nw = snprint_var(str, rpec, (Var *)expr);
		break;
	}
	case T_Const:
	{
		const Const *c = (const Const *)expr;
		nw = snprint_const_type_value(str, c->constisnull, c->consttype, c->constvalue, rpec);
		break;
	}
	case T_OpExpr:
	{
		nw = snprint_op_expr(str, rpec, (OpExpr *)expr);
		break;
	}
	case T_FuncExpr:
	{
		FuncExpr *e = (FuncExpr *)expr;
		if ((nw1 = snprint_func_expr(str, rpec, e)) >= 0)
		{
			nw += nw1;
		}
		else if (e->funcformat == COERCE_IMPLICIT_CAST || e->funcformat == COERCE_EXPLICIT_CAST)
		{
			nw = snprint_cast(str, rpec, e);
		}
		/* may be is a const expression, try to eval and print it */
		else if ((nw1 = eval_snprint_func(str, rpec, e)) >= 0)
		{
			nw = nw1;
		}
		else
		{
			nw = -1;
		}
		break;
	}
	case T_BoolExpr:
	{
		nw = snprint_bool_expr(str, rpec, (BoolExpr *)expr);
		break;
	}
	case T_BooleanTest:
	{
		nw = snprint_test_expr(str, rpec, (BooleanTest *)expr);
		break;
	}
	case T_NullTest:
	{
		nw = snprint_nulltest_expr(str, rpec, (NullTest *)expr);
		break;
	}
	case T_Param:
	{
		nw = snprint_param_expr(str, rpec, (Param *)expr);
		break;
	}
	case T_RelabelType:
	{
		nw = snprint_expr(str, ((RelabelType *)expr)->arg, rpec);
		break;
	}
	case T_CoerceViaIO:
	{
		nw = snprint_coerce_io(str, ((CoerceViaIO *)expr), rpec);
		break;
	}
	case T_SQLValueFunction:
	{
		nw = snprint_sqlvalue_func(str, (SQLValueFunction*)expr, rpec);
		break;
	}
	case T_MinMaxExpr:
	{
		nw = snprint_minmax_expr(str, rpec, (MinMaxExpr *)expr);
		break;
	}
	case T_CoalesceExpr:
	{
		nw = snprint_coalesce_expr(str, rpec, (CoalesceExpr *)expr);
		break;
	}
	case T_CaseExpr:
	{
		nw = snprint_case_expr(str, rpec, (CaseExpr *)expr);
		break;
	}
	case T_ScalarArrayOpExpr:
	{
		nw = snprint_scalararrayop(str, rpec, (ScalarArrayOpExpr *)expr);
		break;
	}
	case T_NextValueExpr:
	{
		nw = snprint_nextval(str, rpec, (NextValueExpr *)expr);
		break;
	}
	case T_RowExpr:
	{
		nw = snprint_rowexpr(str, rpec, (RowExpr *)expr);
		break;
	}
	case T_TargetEntry:
	{
		nw = snprint_expr(str, ((TargetEntry *)expr)->expr, rpec);
		break;
	}
	case T_List:
	{
		nw = snprint_list_expr(str, rpec, (List *)expr);
		break;
	}
	case T_Aggref:
	{
		nw = snprint_agg_expr(str, rpec, (Aggref *)expr);
		break;
	}
	case T_PlaceHolderVar:
	{
		nw = snprint_placeholdervar(str, rpec, (PlaceHolderVar *)expr);
		break;
	}
	default:
	{
		/**
		 * '[not] between...and...' and '[not] in' operators are transformed to
		 * logical operators (using AND and OR) so they are already supported here.
		 * but 'LIKE' is transformed to pg's ~~ operator, we need to prevent
		 * that so that mysql can understand it.
		 * */
		/**
		 * TODO: translate pg native function name to mysql function name,
		 * for those mysql doesn't have, implement them in a plugin as udf
		 * and install to mysql instance. also there are still many string/datetime
		 * operators which need to be converted to mysql function names/operators.
		 *
		 * Top priorities:
		 * string, datetime, numeric functions/operators
		 * regex operator mapping
		 */
		nw = -1;
		break;
	}
	}
	
	return nw;
}

bool is_expr_printable(const Expr *expr, RemotePrintExprContext *rpec)
{
	bool saved_noprint = rpec->noprint;
	rpec->noprint = true;
	bool ret = (snprint_expr(NULL, expr, rpec) > 0);
	rpec->noprint = saved_noprint;

	return ret;
}

static int
append_expr_list(StringInfo str, List *l, int skip, RemotePrintExprContext *rpec)
{
	int nw = 0, nw1 = 0, cnt = 0;
	ListCell *lc;
	foreach (lc, l)
	{
		if (cnt++ < skip)
			continue;
		APPEND_EXPR(lfirst(lc));
		if (lnext(lc))
		{
			APPEND_CHAR(',');
		}
	}
	return nw;
}

static int
eval_nextval_expr(StringInfo str,  RemotePrintExprContext *rpec, NextValueExpr *nve)
{
	int64 newval = nextval_internal(nve->seqid, false);
	int nw = 0, nw1 = 0;

	switch (nve->typeId)
	{
	case INT2OID:
		APPEND_STR_FMT("%d", (int16)newval);
		break;
	case INT4OID:
		APPEND_STR_FMT("%d", (int32)newval);
		break;
	case INT8OID:
		APPEND_STR_FMT("%ld", (int64)newval);
		break;
	default:
		elog(ERROR, "unsupported sequence type %u", nve->typeId);
	}

	return nw;
}

static ParamExternData *
eval_extern_param_val(ParamListInfo paramInfo, int paramId)
{
	if (likely(paramInfo &&
		   paramId > 0 && paramId <= paramInfo->numParams))
	{
		ParamExternData *prm;
		ParamExternData *prmdata = palloc0(sizeof(ParamExternData));

		if (paramInfo->paramFetch != NULL)
			prm = paramInfo->paramFetch(paramInfo, paramId, false, prmdata);
		else
			prm = &paramInfo->params[paramId - 1];
		return prm;
	}
	return NULL;
}

int
output_const_type_value(StringInfo str, bool isnull, Oid type, Datum value)
{
	RemotePrintExprContext rpec;
	InitRemotePrintExprContext(&rpec, NULL);
	return snprint_const_type_value(str, isnull, type, value, &rpec);
}

Oid my_output_funcoid(Oid typid, bool *typIsVarlena)
{
	HeapTuple tup1, tup2;
	Form_pg_type typTup;
	Oid funoid = InvalidOid;

	tup1 = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
	if (HeapTupleIsValid(tup1))
	{
		typTup = (Form_pg_type)GETSTRUCT(tup1);

		if (typIsVarlena)
			*typIsVarlena = (!typTup->typbyval) && (typTup->typlen == -1);

		tup2 = SearchSysCache2(TYPEMAP,
				       PointerGetDatum(&typTup->typname),
				       ObjectIdGetDatum(typTup->typnamespace));
		/* the type is mapped to mysql type directly */
		if (HeapTupleIsValid(tup2))
		{
			funoid = ((Form_pg_type_map)GETSTRUCT(tup2))->myoutput;
			ReleaseSysCache(tup2);
		}
		/* is enum */
		else if (typTup->typcategory == TYPCATEGORY_ENUM)
		{
			NameData name;
			strcpy(name.data, "my_enum_out");
			CatCList *list = SearchSysCacheList1(PROCNAMEARGSNSP, PointerGetDatum(&name));
			if (!list || list->n_members != 1)
				elog(ERROR, "Cache lookup failed for proc class (%s)", name.data);
			funoid = HeapTupleGetOid(&list->members[0]->tuple);
			ReleaseSysCacheList(list);
		}
		/* domain type*/
		else if (typTup->typtype == TYPTYPE_DOMAIN && typTup->typbasetype != InvalidOid)
		{
			funoid = my_output_funcoid(typTup->typbasetype, typIsVarlena);
		}
		ReleaseSysCache(tup1);
	}

	return funoid;
}
/*
  @retval NO. of bytes appended to 'str'. return -2 if the const type
  is not supported in mysql.
*/
int snprint_const_type_value(StringInfo str, bool isnull, Oid type,
			   Datum value, RemotePrintExprContext *rpec)
{
	int nw = 0, nw1 = 0;
	if (isnull)
	{
		APPEND_STR("NULL");
		return nw1;
	}

	Oid typoutput;
	char *outputstr = NULL;

	typoutput = my_output_funcoid(type, NULL);
	if (typoutput != InvalidOid)
	{
		int extra_float_digits_saved = extra_float_digits;
		/* add more digits as possible*/
		extra_float_digits = 3;

		// Some types of partial values in pg cannot be converted to mysql's format.
		// and may throw exception
		PG_TRY();
		{
			outputstr = OidOutputFunctionCall(typoutput, value);
			APPEND_STR(outputstr);
			pfree(outputstr);
		}
		PG_CATCH();
		{
			nw = -1;
		}
		PG_END_TRY();

		extra_float_digits = extra_float_digits_saved;
	}
	else if (type_is_array_category(type))
	{
		int16 typlen;
		bool typbyval;
		char typalign;
		AnyArrayType *v = DatumGetAnyArrayP(value);
		Oid element_type = AARR_ELEMTYPE(v);
		array_iter iter;

		int ndim = AARR_NDIM(v);
		int *dims = AARR_DIMS(v);
		int nitems = ArrayGetNItems(ndim, dims);
		int i = 0;

		/* MySQL only accept 1-D array */
		if (ndim > 1 || type_is_array_category(element_type))
			return -1;
		
		/* Information about the element type*/	
		get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);
		
		/* Save the origin len */
		size_t saved = str ? str->len : 0;

		APPEND_CHAR('(');
		
		array_iter_setup(&iter, v);
		for (i = 0; i < nitems; ++i)
		{
			Datum itemvalue;
			bool isnull;

			if (i > 0)
				APPEND_CHAR(',');

			/* Get source element */
			itemvalue = array_iter_next(&iter, &isnull, i, typlen, typbyval, typalign);

			/* Print the element item */
			nw1 = snprint_const_type_value(str, isnull, element_type, itemvalue, rpec);
			if (nw1 <= 0)
			{
				if (str)
				{
					str->len = saved;
					if (str->data)
						str->data[saved] = '\0';
				}
				nw = -1;
				break;
			}
		}
		APPEND_CHAR(')');
	}
	else
	{
		nw = -1;
	}

	return nw;
}

static const char *get_var_attname(const Var *var, const List *rtable, bool fullname, char *buff, size_t buffsize)
{
	const char *relname, *attname;
	switch (var->varno)
	{
	case INNER_VAR:
		relname = "INNER";
		attname = "?";
		break;
	case OUTER_VAR:
		relname = "OUTER";
		attname = "?";
		break;
	case INDEX_VAR:
		relname = "INDEX";
		attname = "?";
		break;
	default:
	{
		RangeTblEntry *rte;
		Assert(var->varno > 0 &&
		       (int)var->varno <= list_length(rtable));
		rte = rt_fetch(var->varno, rtable);
		relname = rte->eref->aliasname;

		/* print tableoid just as the rel oid value */
		if (var->varattno == TableOidAttributeNumber)
		{
			snprintf(buff, buffsize, "%u", rte->relid);
			attname = buff;
		}
		else if (var->varattno < 0)
		{
			Form_pg_attribute sysatt = SystemAttributeDefinition(var->varattno, true);
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("Kunlun-db: Can't access system attribute(%s) from remote tables.",
					       sysatt ? sysatt->attname.data : "<unknown>")));
		}
		// never print whole-var as * because for executor we always
		// need specific columns and their types.
		else if (var->varattno == 0)
		{
			attname = NULL;
		}
		else
		{
			if (rte->rtekind == RTE_RELATION)
				attname = get_attname(rte->relid, var->varattno, false);
			else
				attname = get_rte_attribute_name(rte, var->varattno);
			int retl;
			if (fullname)
				retl = snprintf(buff, buffsize, "%s.%s",
						get_rel_aliasname(rte), attname);
			else
				retl = snprintf(buff, buffsize, "%s", attname);
			Assert(retl < buffsize);
			attname = buff;
		}
	}
	break;
	}

	return attname;
}
