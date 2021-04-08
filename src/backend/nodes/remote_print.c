/*-------------------------------------------------------------------------
 *
 * remote_print.c
 *	  remote print routines
 *
 *
 * Copyright (c) 2019 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * IDENTIFICATION
 *	  src/backend/nodes/remote_print.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "pgtime.h"
#include "miscadmin.h"
#include "access/printtup.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "nodes/print.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "access/remotetup.h"

static bool mysql_has_func(const char *fn);
static int SQLValueFuncName(SQLValueFunction *svfo, StringInfo str);
static int append_expr_list(StringInfo str, List *l, RemotePrintExprContext *rpec);

#undef APPEND_CHAR
#undef APPEND_EXPR
#undef APPEND_STR
#undef APPEND_STR_FMT

#define APPEND_CHAR(c) do { \
		appendStringInfoChar(str, c);\
		nw++;   \
} while (0)

#define APPEND_EXPR(expr) do {  \
	nw1 = snprint_expr(str, ((Expr*)(expr)), rpec);\
	if (nw1 < 0)        \
		return nw1;     \
	nw += nw1;          \
} while (0)

#define APPEND_STR(str0)  do {   \
	nw1 = appendStringInfoString(str, (str0)); \
	nw += nw1;          \
} while (0)

#define APPEND_STR_FMT(fmt, str0)  do {   \
	nw1 = appendStringInfo(str, fmt, str0); \
	nw += nw1;          \
} while (0)

#define APPEND_FUNC2(funcname, arg1, arg2) do {\
	nw1 = appendStringInfoString(str, funcname);  \
	appendStringInfoChar(str, '('); \
	nw += nw1+3;  \
	nw1 = snprint_expr(str, ((Expr*)(arg1)), rpec);\
	if (nw1 < 0) return nw1;    \
	nw += nw1;      \
	appendStringInfoChar(str, ','); \
	nw1 = snprint_expr(str, ((Expr*)(arg2)), rpec);\
	if (nw1 < 0) return nw1;    \
	nw += nw1;      \
	appendStringInfoChar(str, ')'); \
}while (0)

#define APPEND_FUNC1(funcname, arg1) do {\
	nw1 = appendStringInfoString(str, funcname);  \
	appendStringInfoChar(str, '('); \
	nw += nw1+2;  \
	nw1 = snprint_expr(str, ((Expr*)(arg1)), rpec);\
	if (nw1 < 0) return nw1;    \
	nw += nw1;      \
	appendStringInfoChar(str, ')'); \
}while (0)

// the 3rd argument is a string.
#define APPEND_FUNC3_3s(funcname, arg1, arg2, arg3) do {\
	nw1 = appendStringInfoString(str, funcname);  \
	appendStringInfoChar(str, '('); \
	nw += nw1+4;  \
	nw1 = snprint_expr(str, ((Expr*)(arg1)), rpec);\
	if (nw1 < 0) return nw1;    \
	nw += nw1;      \
	appendStringInfoChar(str, ','); \
	nw1 = snprint_expr(str, ((Expr*)(arg2)), rpec);\
	if (nw1 < 0) return nw1;    \
	nw += nw1;      \
	appendStringInfoChar(str, ','); \
	nw1 = appendStringInfoString(str, arg3);    \
	nw += nw1;      \
	appendStringInfoChar(str, ')'); \
}while (0)

/**
 * Sorted list of mysql functions, as of mysql-8.0.15.
 */
static const char *mysql_funcs[] = {
	"ABS",
	"ACOS",
	"ADDTIME",
	"AES_DECRYPT",
	"AES_ENCRYPT",
	"ANY_VALUE",
	"ASIN",
	"ATAN",
	"ATAN2",
	"BENCHMARK",
	"BIN",
	"BIN_TO_UUID",
	"BIT_COUNT",
	"BIT_LENGTH",
	"CEIL",
	"CEILING",
	"CHARACTER_LENGTH",
	"CHAR_LENGTH",
	"COERCIBILITY",
	"COMPRESS",
	"CONCAT",
	"CONCAT_WS",
	"CONNECTION_ID",
	"CONV",
	"CONVERT_TZ",
	"COS",
	"COT",
	"CRC32",
	"CURRENT_ROLE",
	"DATEDIFF",
	"DATE_FORMAT",
	"DAYNAME",
	"DAYOFMONTH",
	"DAYOFWEEK",
	"DAYOFYEAR",
	"DEGREES",
	"ELT",
	"EXP",
	"EXPORT_SET",
	"EXTRACTVALUE",
	"FIELD",
	"FIND_IN_SET",
	"FLOOR",
	"FOUND_ROWS",
	"FROM_BASE64",
	"FROM_DAYS",
	"FROM_UNIXTIME",
	"GET_LOCK",
	"GREATEST",
	"GTID_SUBSET",
	"GTID_SUBTRACT",
	"HEX",
	"IFNULL",
	"INET6_ATON",
	"INET6_NTOA",
	"INET_ATON",
	"INET_NTOA",
	"INSTR",
	"IS_FREE_LOCK",
	"IS_IPV4",
	"IS_IPV4_COMPAT",
	"IS_IPV4_MAPPED",
	"IS_IPV6",
	"ISNULL",
	"IS_USED_LOCK",
	"IS_UUID",
	"JSON_ARRAY",
	"JSON_ARRAY_APPEND",
	"JSON_ARRAY_INSERT",
	"JSON_CONTAINS",
	"JSON_CONTAINS_PATH",
	"JSON_DEPTH",
	"JSON_EXTRACT",
	"JSON_INSERT",
	"JSON_KEYS",
	"JSON_LENGTH",
	"JSON_MERGE",
	"JSON_MERGE_PATCH",
	"JSON_MERGE_PRESERVE",
	"JSON_OBJECT",
	"JSON_PRETTY",
	"JSON_QUOTE",
	"JSON_REMOVE",
	"JSON_REPLACE",
	"JSON_SEARCH",
	"JSON_SET",
	"JSON_STORAGE_FREE",
	"JSON_STORAGE_SIZE",
	"JSON_TYPE",
	"JSON_UNQUOTE",
	"JSON_VALID",
	"LAST_DAY",
	"LAST_INSERT_ID",
	"LCASE",
	"LEAST",
	"LENGTH",
	"LN",
	"LOAD_FILE",
	"LOCATE",
	"LOG",
	"LOG10",
	"LOG2",
	"LOWER",
	"LPAD",
	"LTRIM",
	"MAKEDATE",
	"MAKE_SET",
	"MAKETIME",
	"MASTER_POS_WAIT",
	"MBRCONTAINS",
	"MBRCOVEREDBY",
	"MBRCOVERS",
	"MBRDISJOINT",
	"MBREQUALS",
	"MBRINTERSECTS",
	"MBROVERLAPS",
	"MBRTOUCHES",
	"MBRWITHIN",
	"MD5",
	"MONTHNAME",
	"NAME_CONST",
	"NULLIF",
	"OCT",
	"OCTET_LENGTH",
	"ORD",
	"PERIOD_ADD",
	"PERIOD_DIFF",
	"PI",
	"POW",
	"POWER",
	"QUOTE",
	"RADIANS",
	"RAND",
	"RANDOM_BYTES",
	"REGEXP_INSTR",
	"REGEXP_LIKE",
	"REGEXP_REPLACE",
	"REGEXP_SUBSTR",
	"RELEASE_ALL_LOCKS",
	"RELEASE_LOCK",
	"REVERSE",
	"ROLES_GRAPHML",
	"ROTATE_SYSTEM_KEY",
	"ROUND",
	"RPAD",
	"RTRIM",
	"SEC_TO_TIME",
	"SHA",
	"SHA1",
	"SHA2",
	"SIGN",
	"SIN",
	"SLEEP",
	"SOUNDEX",
	"SPACE",
	"SQRT",
	"ST_AREA",
	"ST_ASBINARY",
	"ST_ASGEOJSON",
	"ST_ASTEXT",
	"ST_ASWKB",
	"ST_ASWKT",
	"STATEMENT_DIGEST",
	"STATEMENT_DIGEST_TEXT",
	"ST_BUFFER",
	"ST_BUFFER_STRATEGY",
	"ST_CENTROID",
	"ST_CONTAINS",
	"ST_CONVEXHULL",
	"ST_CROSSES",
	"ST_DIFFERENCE",
	"ST_DIMENSION",
	"ST_DISJOINT",
	"ST_DISTANCE",
	"ST_DISTANCE_SPHERE",
	"ST_ENDPOINT",
	"ST_ENVELOPE",
	"ST_EQUALS",
	"ST_EXTERIORRING",
	"ST_GEOHASH",
	"ST_GEOMCOLLFROMTEXT",
	"ST_GEOMCOLLFROMTXT",
	"ST_GEOMCOLLFROMWKB",
	"ST_GEOMETRYCOLLECTIONFROMTEXT",
	"ST_GEOMETRYCOLLECTIONFROMWKB",
	"ST_GEOMETRYFROMTEXT",
	"ST_GEOMETRYFROMWKB",
	"ST_GEOMETRYN",
	"ST_GEOMETRYTYPE",
	"ST_GEOMFROMGEOJSON",
	"ST_GEOMFROMTEXT",
	"ST_GEOMFROMWKB",
	"ST_INTERIORRINGN",
	"ST_INTERSECTION",
	"ST_INTERSECTS",
	"ST_ISCLOSED",
	"ST_ISEMPTY",
	"ST_ISSIMPLE",
	"ST_ISVALID",
	"ST_LATFROMGEOHASH",
	"ST_LATITUDE",
	"ST_LENGTH",
	"ST_LINEFROMTEXT",
	"ST_LINEFROMWKB",
	"ST_LINESTRINGFROMTEXT",
	"ST_LINESTRINGFROMWKB",
	"ST_LONGFROMGEOHASH",
	"ST_LONGITUDE",
	"ST_MAKEENVELOPE",
	"ST_MLINEFROMTEXT",
	"ST_MLINEFROMWKB",
	"ST_MPOINTFROMTEXT",
	"ST_MPOINTFROMWKB",
	"ST_MPOLYFROMTEXT",
	"ST_MPOLYFROMWKB",
	"ST_MULTILINESTRINGFROMTEXT",
	"ST_MULTILINESTRINGFROMWKB",
	"ST_MULTIPOINTFROMTEXT",
	"ST_MULTIPOINTFROMWKB",
	"ST_MULTIPOLYGONFROMTEXT",
	"ST_MULTIPOLYGONFROMWKB",
	"ST_NUMGEOMETRIES",
	"ST_NUMINTERIORRING",
	"ST_NUMINTERIORRINGS",
	"ST_NUMPOINTS",
	"ST_OVERLAPS",
	"ST_POINTFROMGEOHASH",
	"ST_POINTFROMTEXT",
	"ST_POINTFROMWKB",
	"ST_POINTN",
	"ST_POLYFROMTEXT",
	"ST_POLYFROMWKB",
	"ST_POLYGONFROMTEXT",
	"ST_POLYGONFROMWKB",
	"STRCMP",
	"STR_TO_DATE",
	"ST_SIMPLIFY",
	"ST_SRID",
	"ST_STARTPOINT",
	"ST_SWAPXY",
	"ST_SYMDIFFERENCE",
	"ST_TOUCHES",
	"ST_TRANSFORM",
	"ST_UNION",
	"ST_VALIDATE",
	"ST_WITHIN",
	"ST_X",
	"ST_Y",
	"SUBSTRING_INDEX",
	"SUBTIME",
	"TAN",
	"TIMEDIFF",
	"TIME_FORMAT",
	"TIME_TO_SEC",
	"TO_BASE64",
	"TO_DAYS",
	"TO_SECONDS",
	"UCASE",
	"UNCOMPRESS",
	"UNCOMPRESSED_LENGTH",
	"UNHEX",
	"UNIX_TIMESTAMP",
	"UPDATEXML",
	"UPPER",
	"UUID",
	"UUID_SHORT",
	"UUID_TO_BIN",
	"VALIDATE_PASSWORD_STRENGTH",
	"VERSION",
	"WAIT_FOR_EXECUTED_GTID_SET",
	"WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS",
	"WEEKDAY",
	"WEEKOFYEAR",
	"YEARWEEK"
};


inline static int func_name_cmp(const void *n1, const void*n2)
{
	char *s1 = (char *)n1;
	char **ps2 = (char **)n2;
	if (s1 == NULL || *ps2 == NULL)
		return s1 > *ps2 ? 1 : (s1 == *ps2 ? 0 : -1);
	return strcasecmp(s1, *ps2);
}

static inline bool mysql_has_func(const char *fn)
{
	void *pos = bsearch(fn, mysql_funcs, sizeof(mysql_funcs)/sizeof(void*),
		sizeof(void*), func_name_cmp);
	return pos != NULL;
}

/*
 * See if a type is one of the string types.
 * */
inline static bool is_str_type(Oid typid)
{
	const static Oid strtypids[] = {18, 25, 1002, 1009, 1014, 1015, 1042, 1043, 1263, 2275};
	for (int i = 0; i < sizeof(strtypids)/sizeof(Oid); i++)
		if (typid == strtypids[i])
			return true;

	return false;
}

inline static bool is_expr_integer(Node *expr)
{
	int typeid = exprType(expr);
	return typeid == INT2OID || typeid == INT4OID || typeid == INT8OID;
}

/*
  @retval NO. of bytes appended to 'str'. return -2 if the const type
  is not supported in mysql.
*/
static int output_const_type_value(StringInfo str, bool isnull, Oid type,
	Datum value)
{
	int nw = 0, nw1 = 0;
	if (isnull)
	{
		APPEND_STR("NULL");
		return nw1;
	}

	if (format_type_remote(type) == NULL)
		return -2;

	Oid			typoutput;
	bool		typIsVarlena;
	char	   *outputstr = NULL;
	getTypeOutputInfo(type, &typoutput, &typIsVarlena);
	
	/*
	 * Always produce date/time/timestamp/internval/timetz/timestamptz values
	 * using ISO, YMD date style/order, ISO interval style, in UTC+0 timezone.
	 * Temporarily modify the 3 session vars to do so and restore
	 * them after done.
	 * */
	pg_tz *gmt_tz = pg_tzset("GMT"), *origtz = NULL;
	int orig_datestyle = -1, orig_dateorder = -1, orig_intvstyle = -1;
	if (orig_datestyle == -1 && is_date_time_type(type))
	{
		orig_datestyle = DateStyle;
		orig_dateorder = DateOrder;
		origtz = session_timezone;
		orig_intvstyle = IntervalStyle;
	
		DateStyle = USE_ISO_DATES;
		DateOrder = DATEORDER_YMD;
		IntervalStyle = INTSTYLE_ISO_8601;
		session_timezone = gmt_tz;
	}
	
	outputstr = OidOutputFunctionCall(typoutput, value);
	
	if (orig_datestyle != -1)
	{
		DateStyle = orig_datestyle;
		DateOrder = orig_dateorder;
		IntervalStyle = orig_intvstyle;
		session_timezone = origtz;
	}
	outputstr = pg_to_mysql_const(type, outputstr);
	const bool ics = const_output_needs_quote(type);
	/*
	 * In pg str const always wrapped in single quotes('), if there were '
	 * in the string they must have been escaped otherwise lexer already
	 * errored out.
	 * */
	if (ics)
		APPEND_CHAR('\'');
	APPEND_STR(outputstr);
	if (ics)
		APPEND_CHAR('\'');
	pfree(outputstr);

	return nw1 + ics ? 2 : 0;
}


const char *get_var_attname(const Var *var, const List *rtable)
{
	const char	   *relname, *attname;
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
					   (int) var->varno <= list_length(rtable));
				rte = rt_fetch(var->varno, rtable);
				relname = rte->eref->aliasname;
				attname = get_rte_attribute_name(rte, var->varattno);
			}
			break;
	}

	return attname;
}


/*
 * print expr into str.
 * Returns NO. of bytes appended to str; now never returns -1; return -2
 * if the expr of one of its components can/should not be serialized, and in this
 * case 'str' is half written, so caller should note down its old length for
 * truncation on error;
 * See/use get_rule_expr() to handle more types of exprs.
 *
 * This function must recognize non-standard functions and operators, i.e. which
 * are not supported by MySQL, and return -2 in this case. The pg_operator and
 * pg_functions should have had a boolean column 'standard' to indicate whether
 * the operator/function is in SQL standard.
 * */
int
snprint_expr(StringInfo str, const Expr *expr, RemotePrintExprContext *rpec)
{
	int nw = 0, nw1 = 0;
	const List *rtable = rpec->rtable;

	if (expr == NULL || str->maxlen == 0 || !str->data)
	{
		return 0;
	}

	if (IsA(expr, Var))
	{
		const Var  *var = (const Var *) expr;
		/*
		 * Here we don't print the relation name, because it can be a
		 * partition table name. We know for sure that the column name is
		 * qualified and valid in both computing node and storage node.
		 * */
		APPEND_STR(get_var_attname(var, rtable));
	}
	else if (IsA(expr, Const))
	{
		const Const *c = (const Const *) expr;
		if ((nw1 = output_const_type_value(str, c->constisnull, c->consttype,
				c->constvalue)) < 0)
			return nw1;
		nw += nw1;
	}
	else if (IsA(expr, OpExpr))
	{
		const OpExpr *e = (const OpExpr *) expr;
		char	   *opname;
#define OPC_EQ(i, c) (opname[(i)] == (c))
		APPEND_CHAR('(');
		opname = get_opname(e->opno);

		if (opname[0] == '+' || opname[0] == '=' || opname[0] == '-' || opname[0] == '*'|| opname[0] == '%')
			goto commons;

		if (OPC_EQ(0, '~') && OPC_EQ(1, '~') && OPC_EQ(2, '\0')) //strcmp(opname, "~~") == 0)
			opname = "LIKE";
		if (OPC_EQ(0, '!') && OPC_EQ(1, '~') && OPC_EQ(2, '~') && OPC_EQ(3, '\0')) //strcmp(opname, "!~~") == 0)
			opname = "NOT LIKE";
		else if (OPC_EQ(0, '|') && OPC_EQ(1, '|') && OPC_EQ(2, '\0')) //strcmp(opname, "||") == 0)
		{
			opname = "concat";
			APPEND_FUNC2(opname, get_leftop((const Expr *) e), get_rightop((const Expr *) e));
			goto op_expr_done;
		}
		else if (OPC_EQ(0, '^') && OPC_EQ(1, '\0'))//strcmp(opname, "^") == 0)
		{
			opname = "power";
			APPEND_FUNC2(opname, get_leftop((const Expr *) e), get_rightop((const Expr *) e));
			goto op_expr_done;
		}
		else if (OPC_EQ(0, '|') && OPC_EQ(1, '/') && OPC_EQ(2, '\0'))//strcmp(opname, "|/") == 0)
		{
			opname = "sqrt";
			APPEND_FUNC1(opname, get_leftop((const Expr *) e));
			goto op_expr_done;
		}
		else if (OPC_EQ(0, '|') && OPC_EQ(1, '|') && OPC_EQ(2, '/') && OPC_EQ(3, '\0'))//strcmp(opname, "||/") == 0)
		{
			return -2;
		}
		else if (OPC_EQ(0, '@') && OPC_EQ(1, '\0'))//strcmp(opname, "@") == 0)
		{
			opname = "abs";
			APPEND_FUNC1(opname, get_leftop((const Expr *) e));
			goto op_expr_done;
		}
		else if (OPC_EQ(0, '#') && OPC_EQ(1, '\0'))//strcmp(opname, "#") == 0)
			opname = "^";
		else if (strcasecmp(opname, "SIMILAR TO") == 0)
		{
			opname = "regexp_like";
			APPEND_FUNC2(opname, get_leftop((const Expr *) e), get_rightop((const Expr *) e));
			goto op_expr_done;
		}
		else if (OPC_EQ(0, '~') && OPC_EQ(1, '\0'))//strcmp(opname, "~") == 0)
		{
			if (list_length(e->args) == 1)
			{
				Node *arg1 = get_leftop((const Expr *) e);
				Oid arg1typ = exprType(arg1);
				if (arg1typ == MACADDROID || arg1typ == MACADDR8OID || arg1typ == INETOID)
					return -2;
				APPEND_FUNC1(opname, arg1);
				goto op_expr_done;
			}

			opname = "regexp_like";
			APPEND_FUNC3_3s(opname, get_leftop((const Expr *) e), get_rightop((const Expr *) e), "'c'");
			goto op_expr_done;
		}
		else if (OPC_EQ(0, '~') && OPC_EQ(1, '*') && OPC_EQ(2, '\0'))//strcmp(opname, "~*") == 0)
		{
			opname = "regexp_like";
			APPEND_FUNC3_3s(opname, get_leftop((const Expr *) e), get_rightop((const Expr *) e), "'i'");
			goto op_expr_done;
		}
		else if (OPC_EQ(0, '!') && OPC_EQ(1, '~') && OPC_EQ(2, '\0'))//strcmp(opname, "!~") == 0)
		{
			opname = "NOT REGEXP_LIKE";
			APPEND_FUNC3_3s(opname, get_leftop((const Expr *) e), get_rightop((const Expr *) e), "'c'");
			goto op_expr_done;
		}
		else if (OPC_EQ(0, '!') && OPC_EQ(1, '~') && OPC_EQ(2, '*') && OPC_EQ(3, '\0'))//strcmp(opname, "!~*") == 0)
		{
			opname = "NOT REGEXP_LIKE";
			APPEND_FUNC3_3s(opname, get_leftop((const Expr *) e), get_rightop((const Expr *) e), "'i'");
			goto op_expr_done;
		}
		else if (OPC_EQ(0, '/') && OPC_EQ(1, '\0') && //strcmp(opname, "/") == 0 &&
				 is_expr_integer(get_leftop((const Expr *) e)) &&
				 is_expr_integer(get_rightop((const Expr *) e)))
		{
			opname = "DIV";
		}

		if ((opname[0] == '<' || opname[0] == '>') && !((opname[0] == '<' && opname[1] == '>')))
		{
			/*
			 * enum values in mysql can only do = or != comparison, anything
			 * else should be rejected. but such checks are too expensive, so
			 * instead we do this:
			 * Given <,<=, >, >= operators, if one argument is enum, then we
			 * can't serialize it because mysql does enum lt/gt comparison as
			 * strings not as enum sort values.
			 * */
			Oid ltypid = exprType(get_leftop((const Expr *) e));
			Oid rtypid = exprType(get_rightop((const Expr *) e));
			if (type_is_enum_lite(ltypid) || type_is_enum_lite(rtypid) ||
				ltypid == INETOID || rtypid == INETOID)
				return -2;
		}

		/*
		 * MySQL doesn't support inet operands for oprs <<, <<=, <, <=, >>, >>=, >, >=, &&,+,- and unary ~
		 * or macaddr/macaddr8 operands for oprs  &, |, unary ~
		 * */
		if (opname[0] == '&' && opname[1] == '&' && opname[2] == '\0')
		{
			Oid ltypid = exprType(get_leftop((const Expr *) e));
			Oid rtypid = exprType(get_rightop((const Expr *) e));
			if (ltypid == INETOID || rtypid == INETOID)
				return -2;
		}
		if ((opname[0] == '&' || opname[0] == '|') && opname[1] == '\0')
		{
			Oid ltypid = exprType(get_leftop((const Expr *) e));
			Oid rtypid = exprType(get_rightop((const Expr *) e));
			if (ltypid == MACADDROID || rtypid == MACADDROID ||
				ltypid == MACADDR8OID || rtypid == MACADDR8OID)
				return -2;
		}
commons:
		if ((opname[0] == '+' || opname[0] == '-') && opname[1] == '\0')
		{
			Oid ltypid = exprType(get_leftop((const Expr *) e));
			Oid rtypid = exprType(get_rightop((const Expr *) e));
			if (ltypid == INETOID || rtypid == INETOID)
				return -2;
		}

		/*
		 * MySQL can't do substract to produce interval values, have to do it
		 * in computing node.
		 * */
		if (opname[0] == '-' && opname[1] == '\0' &&
			(is_interval_opr_type(exprType(get_leftop((const Expr *) e))) ||
			 is_interval_opr_type(exprType(get_rightop((const Expr *) e)))))
			return -2;
#if 0
		else if (strcasecmp(opname, "") == 0)
			opname = "";
#endif
		if (list_length(e->args) > 1)
		{
			APPEND_EXPR(get_leftop((const Expr *) e));
			APPEND_STR_FMT(" %s ", ((opname != NULL) ? opname : "(invalid operator)"));
			APPEND_EXPR(get_rightop((const Expr *) e));
		}
		else
		{
			/* we print prefix and postfix ops the same... */
			APPEND_STR_FMT(" %s ", (opname != NULL) ? opname : "(invalid operator)");
			APPEND_EXPR(get_leftop((const Expr *) e));
		}
op_expr_done:
		APPEND_CHAR(')');
	}
	else if (IsA(expr, FuncExpr))
	{
		const FuncExpr *e = (const FuncExpr *) expr;
		char	   *funcname;

		funcname = get_func_name(e->funcid);
		if (!mysql_has_func(funcname))
		{
			/*
			  no need for such functions, let mysql do local type conversion
			  if needed.
			*/
			if (is_type_conversion_func(e->funcid))
			{
				APPEND_EXPR(linitial(e->args));
				goto end;
			}
			else
				goto unsupported;
		}

		APPEND_STR_FMT(" %s(", (funcname != NULL) ? funcname : "(invalid function)");

		if ((nw1 = append_expr_list(str, e->args, rpec)) < 0)
			return nw1;
		nw += nw1;
		APPEND_CHAR(')');
	}
	else if (IsA(expr, BoolExpr))
	{
		BoolExpr   *boolexpr = (BoolExpr *) expr;
		int			nargs = list_length(boolexpr->args);
		int			off;
		ListCell   *lc;

		off = 0;
		APPEND_CHAR('(');
		foreach(lc, boolexpr->args)
		{
			Expr	   *arg = (Expr *) lfirst(lc);

			/* Perform the appropriate step type */
			switch (boolexpr->boolop)
			{
				case AND_EXPR:
					Assert(nargs >= 2);
					APPEND_EXPR(arg);
					if (lnext(lc))
						APPEND_STR(" AND ");
					break;
				case OR_EXPR:
					Assert(nargs >= 2);
					APPEND_EXPR(arg);
					if (lnext(lc))
						APPEND_STR(" OR ");
					break;
				case NOT_EXPR:
					Assert(nargs == 1);
					APPEND_STR("NOT ");
					APPEND_EXPR(arg);
					break;
				default:
					goto unsupported;
					break;
			}

			off++;
		}
		APPEND_CHAR(')');
	}
	else if (IsA(expr, BooleanTest))
	{
		BooleanTest *btest = (BooleanTest *) expr;
		const char *pbteststr = 0;
		switch (btest->booltesttype)
		{
			case IS_TRUE:
				pbteststr = " IS TRUE";
				break;
			case IS_NOT_TRUE:
				pbteststr = " IS NOT TRUE";
				break;
			case IS_FALSE:
				pbteststr = " IS FALSE";
				break;
			case IS_NOT_FALSE:
				pbteststr = " IS NOT FALSE";
				break;
			case IS_UNKNOWN:
				pbteststr = " IS NULL";
				break;
			case IS_NOT_UNKNOWN:
				pbteststr = " IS NOT NULL";
				break;
			default:
				elog(ERROR, "unrecognized booltesttype: %d",
					 (int) btest->booltesttype);
		}
		APPEND_CHAR('(');
		APPEND_EXPR(btest->arg);
		APPEND_STR(pbteststr);
		APPEND_CHAR(')');
	}
	else if (IsA(expr, NullTest))
	{
		const char *pnteststr = 0;
		NullTest   *ntest = (NullTest *) expr;
		if (ntest->nulltesttype == IS_NULL)
		{
			pnteststr = " IS NULL";
		}
		else if (ntest->nulltesttype == IS_NOT_NULL)
		{
			pnteststr = " IS NOT NULL";
		}
		else
		{
			elog(ERROR, "unrecognized nulltesttype: %d",
				 (int) ntest->nulltesttype);
		}
		APPEND_CHAR('(');
		APPEND_EXPR(ntest->arg);
		APPEND_STR(pnteststr);
		APPEND_CHAR(')');
	}
	else if (IsA(expr, Param) && !rpec->ignore_param_quals)
	{
		Param *param = (Param *)expr;
		ParamExecData *ped = rpec->rpec_param_exec_vals + param->paramid;

		if ((nw1 = output_const_type_value(str, ped->isnull, param->paramtype,
				ped->value)) < 0)
			return nw1;
		nw += nw1;
	}
	else if (IsA(expr, RelabelType))
	{
		RelabelType *rt = (RelabelType*)expr;
		APPEND_EXPR(rt->arg);
	}
	else if (IsA(expr, CoerceViaIO))
	{
		CoerceViaIO*cvi = (CoerceViaIO*)expr;
		APPEND_EXPR(cvi->arg);
	}
	else if (IsA(expr, SQLValueFunction))
	{
		SQLValueFunction *svf  = (SQLValueFunction *)expr;
		if ((nw1 = SQLValueFuncName(svf, str)) < 0)
			return nw1;
		nw += nw1;
	}
	else if (IsA(expr, MinMaxExpr))
	{
		MinMaxExpr*mme  = (MinMaxExpr*)expr;
		if (mme->op == IS_GREATEST)
			APPEND_STR(" GREATEST(");
		else if (mme->op == IS_LEAST)
			APPEND_STR(" LEAST(");
		else
			return -2;
		if ((nw1 = append_expr_list(str, mme->args, rpec)) < 0)
			return nw1;
		nw += nw1;
		APPEND_CHAR(')');
	}
	else if (IsA(expr, CoalesceExpr))
	{
		CoalesceExpr *ce = (CoalesceExpr *)expr;
		APPEND_STR(" Coalesce(");
		if ((nw1 = append_expr_list(str, ce->args, rpec)) < 0)
			return nw1;
		nw += nw1;
		APPEND_CHAR(')');
	}
	else if (IsA(expr, CaseExpr))
	{
		CaseExpr *ce = (CaseExpr *)expr;
		APPEND_STR(" CASE ");
		bool eqs = false;
		ListCell *lc = NULL;

		if (ce->arg)
		{
			APPEND_EXPR(ce->arg);
			eqs = (ce->casetype != InvalidOid);
		}

		foreach(lc, ce->args)
		{
			CaseWhen *cw = (CaseWhen *)lfirst(lc);

			APPEND_STR(" WHEN ");
			if (!eqs)
				APPEND_EXPR(cw->expr);
			else if (IsA(cw->expr, OpExpr))
			{
				/*
				  According to comments in CaseExpr definition, cw->expr must
				  be a CaseTestExpr = compexpr expression in this case.
				  However, there are
				  situations where pg doesn't handle correctly, e.g. when ce->arg
				  is a NullTest node, and we'd have to error out.
				*/
				OpExpr *eqexpr = (OpExpr*)cw->expr;
				/*Assert(IsA(linitial(eqexpr->args), CaseTestExpr));
				The CaseTestExpr node maybe wrapped into a RelabelType node,
				or other nodes.
				*/
				APPEND_EXPR(lsecond(eqexpr->args));
			}
			else
				return -2;

			APPEND_STR(" THEN ");
			APPEND_EXPR(cw->result);
		}

		if (ce->defresult)
		{
			APPEND_STR(" ELSE ");
			APPEND_EXPR(ce->defresult);
		}

		APPEND_STR(" END ");

	}
	else if (IsA(expr, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *scoe = (ScalarArrayOpExpr *)expr;
		const char *opname = get_opname(scoe->opno);
		APPEND_STR(opname);
		if (scoe->useOr)
			APPEND_STR(" ANY(");
		else
			APPEND_STR(" ALL(");
		if ((nw1 = append_expr_list(str, scoe->args, rpec)) < 0)
			return nw1;
		nw += nw1;
		APPEND_CHAR(')');
	}
	else
	{
unsupported:
		return -2;
	}
end:
	return nw;
}

#define CONST_STR_LEN(conststr) conststr,(sizeof(conststr)-1)
static int SQLValueFuncName(SQLValueFunction *svfo, StringInfo str)
{
	int nw = -1;

	switch (svfo->op)
	{
	case SVFOP_CURRENT_DATE:
		nw = appendBinaryStringInfo2(str, CONST_STR_LEN(" CURRENT_DATE "));
		break;
	case SVFOP_CURRENT_TIME:
		nw = appendBinaryStringInfo2(str, CONST_STR_LEN(" CURRENT_TIME "));
		break;
	case SVFOP_CURRENT_TIME_N:
		nw = appendStringInfo(str, " CURRENT_TIME(%d) ", svfo->typmod);
		break;
	case SVFOP_CURRENT_TIMESTAMP:
		nw = appendBinaryStringInfo2(str, CONST_STR_LEN(" CURRENT_TIMESTAMP "));
		break;
	case SVFOP_CURRENT_TIMESTAMP_N:
		nw = appendStringInfo(str, " CURRENT_TIMESTAMP(%d) ", svfo->typmod);
		break;
	case SVFOP_LOCALTIME:
		nw = appendBinaryStringInfo2(str, CONST_STR_LEN(" LOCALTIME "));
		break;
	case SVFOP_LOCALTIME_N:
		nw = appendStringInfo(str, " LOCALTIME(%d) ", svfo->typmod);
		break;
	case SVFOP_LOCALTIMESTAMP:
		nw = appendBinaryStringInfo2(str, CONST_STR_LEN(" LOCALTIMESTAMP "));
		break;
	case SVFOP_LOCALTIMESTAMP_N:
		nw = appendStringInfo(str, " LOCALTIMESTAMP(%d) ", svfo->typmod);
		break;
	case SVFOP_CURRENT_ROLE:
		nw = appendBinaryStringInfo2(str, CONST_STR_LEN(" CURRENT_ROLE "));
		break;
	case SVFOP_CURRENT_USER:
		nw = appendBinaryStringInfo2(str, CONST_STR_LEN(" CURRENT_USER "));
		break;
	case SVFOP_USER:
		nw = appendBinaryStringInfo2(str, CONST_STR_LEN(" USER "));
		break;
	case SVFOP_SESSION_USER:
		nw = appendBinaryStringInfo2(str, CONST_STR_LEN(" SESSION_USER "));
		break;
	case SVFOP_CURRENT_CATALOG:
		nw = appendBinaryStringInfo2(str, CONST_STR_LEN(" CURRENT_CATALOG "));
		break;
	case SVFOP_CURRENT_SCHEMA:
		nw = appendBinaryStringInfo2(str, CONST_STR_LEN(" CURRENT_SCHEMA "));
		break;
	default:
		nw = -2;
		break;
	}
	return nw;
};

static int append_expr_list(StringInfo str, List *l, RemotePrintExprContext *rpec)
{
	int nw = 0, nw1 = 0;
	ListCell *lc;
	foreach(lc, l)
	{
		APPEND_EXPR(lfirst(lc));
		if (lnext(lc))
		{
			APPEND_CHAR(',');
		}
	}
	return nw;
}
