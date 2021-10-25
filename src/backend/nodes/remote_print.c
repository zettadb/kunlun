/*-------------------------------------------------------------------------
 *
 * remote_print.c
 *	  remote print routines
 *
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
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
#include "catalog/heap.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "nodes/print.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "access/remotetup.h"
#include "commands/sequence.h"

static bool mysql_has_func(const char *fn);
static int SQLValueFuncValue(SQLValueFunction *svfo, StringInfo str);
static int append_expr_list(StringInfo str, List *l, RemotePrintExprContext *rpec);
static int eval_nextval_expr(StringInfo str, NextValueExpr *nve);
static int convert_numeric_func(Oid funcid, List *args, StringInfo str, RemotePrintExprContext *rpec);
static const char *get_var_attname(const Var *var, const List *rtable);
static SQLValueFunction*
makeSQLValueFunction(SQLValueFunctionOp op, int32 typmod);
static ParamExternData * eval_extern_param_val(ParamListInfo paramInfo, int paramId);

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

#define APPEND_STR_FMT2(fmt, arg0, arg1)  do {   \
	nw1 = appendStringInfo(str, fmt, arg0, arg1); \
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

#define NUMERIC_FUNC_ID_START 1718
#define NUMERIC_FUNC_ID_MAX 1739
#define NUMERIC_FUNC_OPER_ID_MAX 1729

static inline bool is_numeric_func(Oid funcid)
{
	if (funcid >= NUMERIC_FUNC_ID_START && funcid <= NUMERIC_FUNC_ID_MAX)
		return true;
	static Oid numeric_other_funcids[] =
	{1703, 1704,1705,1706,1707,1711,1712,1764,1766,1767,1771,1915,1973,1980,2167};
	for (int i = 0; i < sizeof(numeric_other_funcids)/sizeof(Oid); i++)
		if (numeric_other_funcids[i] == funcid)
			return true;
	return false;
}

static inline int func_id_cmp(const void *v1, const void *v2)
{
	Oid o1 = *(Oid*)v1;
	Oid o2 = *(Oid*)v2;
	return o1 > o2 ? 1 : (o1 == o2 ? 0 : -1);
}


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

inline static bool type_is_array_category(Oid typid)
{
	char typcat;
	bool preferred;
	get_type_category_preferred(typid, &typcat, &preferred);
	return typcat == TYPCATEGORY_ARRAY;
}

/*
 * See if a type is one of the string types.
 * */
inline static bool is_str_type(Oid typid)
{
	const static Oid strtypids[] = //{18, 25, 1002, 1009, 1014, 1015, 1042, 1043, 1263, 2275};
	{CHAROID, TEXTOID, CHARARRAYOID, NAMEARRAYOID, TEXTARRAYOID, BPCHARARRAYOID,
	 VARCHARARRAYOID, CSTRINGARRAYOID, BPCHAROID, VARCHAROID, CSTRINGOID};
	// BPCHAROID, VARCHAROID, CSTRINGOID, CHAROID, TEXTOID
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
	if (format_type_remote(type) == NULL)
	{
		if (type_is_array_category(type))
		{
			/*
			  pg uses an array to store the const value list used in exprs like IN.
			  Only allow 1-D array, replace the {} with () because mysql
			  only accept 1-D () list.
			  For now such an array constant is always used only for a
			  'col IN(exprlist)' expression. But in future we may do more, as
			  detailed in the handler for ScalarArrayOpExpr in snprint_expr.
			*/
			char *p = outputstr, *q = NULL;
			if ((p = strchr(p, '{')))
			{
				q = p;
				if ((p = strchr(p + 1, '{')))
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("Only 1-D array constant is supported in Kunlun-db.")));
				p = q;
				if ((p = strchr(p + 1, '}')) == NULL)
					ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
							errmsg("Invalid array constant: %s, unmatchcing brackets.", outputstr)));
				*p = ')';
				*q = '(';
			}
			else
				ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("Invalid array constant: %s, brackets not found.", outputstr)));
		}
	}
	{
		DateStyle = orig_datestyle;
		DateOrder = orig_dateorder;
		IntervalStyle = orig_intvstyle;
		session_timezone = origtz;
	}
	char *old_val = outputstr;
	outputstr = pg_to_mysql_const(type, outputstr);
	const int ics = const_output_needs_quote(type);
	/*
	 * In pg str const always wrapped in single quotes('), if there were '
	 * in the string they must have been escaped otherwise lexer already
	 * errored out.
	 * */
	if (ics == 1)
		APPEND_CHAR('\'');
	APPEND_STR(outputstr);
	if (ics == 1)
		APPEND_CHAR('\'');

	pfree(old_val);

	return nw1 + ics ? 2 : 0;
}


static const char *get_var_attname(const Var *var, const List *rtable)
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
				if (var->varattno < 0)
				{
					Form_pg_attribute sysatt = SystemAttributeDefinition(var->varattno, true);
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("Kunlun-db: Can't access system attribute(%s) from remote tables.",
								   sysatt ? sysatt->attname.data : "<unknown>")));
				}
				// never print whole-var as * because for executor we always
				// need specific columns and their types.
				else if (var->varattno == 0)
					return NULL;
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

	/*
	  Normally an expr will be serialized into one expr here, except a few
	  like RowExpr as shown below.
	*/
	rpec->num_vals = 1;

	if (IsA(expr, Var))
	{
		const Var  *var = (const Var *) expr;
		/*
		 * Here we don't print the relation name, because it can be a
		 * partition table name. We know for sure that the column name is
		 * qualified and valid in both computing node and storage node.
		 * */
		const char *varname = get_var_attname(var, rtable);
		if (varname) APPEND_STR(varname);
		else return -2;
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
			  We can let mysql do implicit type conversions locally, but
			  we have to require mysql to do explicit type conversions.
			*/
			Oid totypid = InvalidOid;
			const char *castfn = NULL;

			if (e->funcformat == COERCE_IMPLICIT_CAST)
			{
				APPEND_EXPR(linitial(e->args));
				goto end;
			}
			else if ((e->funcformat == COERCE_EXPLICIT_CAST ||
					  (is_type_conversion_func(e->funcid, &totypid) &&
					   totypid == e->funcresulttype)) &&
					 (castfn = mysql_can_cast(e->funcresulttype)) != NULL &&
					 e->funcid != 1703/*NUMERIC cast is handled below */)
			{
				APPEND_STR(" CAST(");
				APPEND_EXPR(linitial(e->args));
				APPEND_STR_FMT(" as %s", castfn);
				if (list_length(e->args) > 1)
				{
					// for existing conversion funcs, the 2nd arg is always the (N).
					APPEND_CHAR('(');
					APPEND_EXPR(lsecond(e->args));
					APPEND_CHAR(')');
				}
				APPEND_STR(") ");
				goto end;
			}
			else if (is_numeric_func(e->funcid))
			{
				int cnfret = convert_numeric_func(e->funcid, e->args, str, rpec);
				if (cnfret > 0)
				{
					nw += cnfret;
					goto end;
				}
				else if (cnfret == 0)
					goto unsupported;
				else
				{
					nw += (-cnfret);
					funcname = "";// funcname has been printed in convert_numeric_func.
				}
			}
			else if (e->funcid == 1299 || e->funcid == 2647 ||
					 e->funcid == 2648 || e->funcid == 2649) //ADAPT pg_proc.dat
			{
				/*
				  We need to specially handle NOW(). It's semantically identical to
				  CURRENT_TIMESTAMP, we want to keep this true so we produce the
				  const value in computing nodes instead of sending it to
				  storage shards in case the format and timestamp value were
				  slightly different. We also support the 3 aliases of NOW() : 
				  transaction_timestamp(), statement_timestamp, clock_timestamp().
				  pg doc says there are still TODAY(), TOMORROW(), YESTERDAY(),
				  but pg_proc.dat doesn't have these funcs.
				*/
				SQLValueFunction *svf = makeSQLValueFunction(SVFOP_CURRENT_TIMESTAMP, -1);
				if ((nw1 = SQLValueFuncValue(svf, str)) < 0)
					return nw1;
				nw += nw1;
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
		ParamExecData *ped = NULL;
		ParamExternData *pxd = NULL;
		bool isnull;
		Oid paramtype;
		Datum pval;

		if (param->paramkind == PARAM_EXEC)
		{
			ped = rpec->rpec_param_exec_vals + param->paramid;
			Assert(ped);
			isnull = ped->isnull;
			paramtype = param->paramtype;
			pval = ped->value;
		}
		else if (param->paramkind == PARAM_EXTERN)
		{
			pxd = eval_extern_param_val(rpec->rpec_param_list_info, param->paramid);
			Assert(pxd != NULL);
			isnull = pxd->isnull;
			paramtype = pxd->ptype;
			pval = pxd->value;
		}
		else
			Assert(false);//PARAM_SUBLINK and PARAM_MULTIEXPR params are
			// always converted to PARAM_EXEC during planning.

		if ((nw1 = output_const_type_value(str, isnull, paramtype, pval)) < 0)
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
		if ((nw1 = SQLValueFuncValue(svf, str)) < 0)
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
		APPEND_EXPR(linitial(scoe->args));

		/*
		APPEND_STR(opname);
		if (scoe->useOr)
			APPEND_STR(" ANY");
		else
			APPEND_STR(" ALL");
		*/

		/*
		  'col IN (expr1, expr2, ..., exprN)' is valid SQL expr, but
		  'col comp-opr ANY(expr1, expr2, ..., exprN)'
		  isn't, for both mysql and pg.

		  For mysql-8.0.19 and newer, we can convert it to 
		  'col comp-opr ANY(VALUES ROW(expr1), ROW(expr2),..., ROW(exprN))',
		  especially the 'exprN' here can be a list, such as '1,2,3', to
		  do a 'row subquery' as in mysql's terms, so we can accept 1-D or 2D
		  array here when we upgrade to newer mysql.
		  but for current kunlun-storage(based on mysql-8.0.18), we have to
		  reject such exprs so we only send 'col IN(expr list)' grammar.

		  pg supports 'col comp-opr ANY(array-constructor)' grammar so
		  that ' a > ANY(ARRAY[1,2,3])' is valid for pg but this is not
		  standard SQL and this is how we would recv such an unsupported expr
		  here because if the subquery is an select, pg will do semijoin.
		*/
		if (!(scoe->useOr && strcmp(opname, "=") == 0))
			return -2;

		APPEND_STR(" IN");
		rpec->skip_n = 1;
		if ((nw1 = append_expr_list(str, scoe->args, rpec)) < 0)
		{
			rpec->skip_n = 0;
			return nw1;
		}
		rpec->skip_n = 0;
		nw += nw1;
	}
	else if (IsA(expr, NextValueExpr))
	{
		NextValueExpr *nve = (NextValueExpr *)expr;
		nw += eval_nextval_expr(str, nve);
	}
	else if (IsA(expr, RowExpr))
	{
		RowExpr *rowexpr = (RowExpr *)expr;
		if ((nw1 = append_expr_list(str, rowexpr->args, rpec)) < 0)
			return nw1;
		nw += nw1;
		rpec->num_vals = list_length(rowexpr->args);
	}
	else
	{
unsupported:
		return -2;
	}
end:
	return nw;
}

static SQLValueFunction*
makeSQLValueFunction(SQLValueFunctionOp op, int32 typmod)
{
    SQLValueFunction *svf = makeNode(SQLValueFunction);

    svf->op = op; 
    /* svf->type will be filled during parse analysis */
    svf->typmod = typmod;
    svf->location = -1;
    return svf;
}

#define CONST_STR_LEN(conststr) conststr,(sizeof(conststr)-1)
static int SQLValueFuncValue(SQLValueFunction *svfo, StringInfo str_res)
{
	int nw = -1;
	static char sql_func[64];
	bool is_datetime = false;

	switch (svfo->op)
	{
	case SVFOP_CURRENT_DATE:
		nw = snprintf(sql_func, sizeof(sql_func), "SELECT CURRENT_DATE ");
		is_datetime = true;
		break;
	case SVFOP_CURRENT_TIME:
		nw = snprintf(sql_func, sizeof(sql_func), "SELECT CURRENT_TIME ");
		is_datetime = true;
		break;
	case SVFOP_CURRENT_TIME_N:
		nw = snprintf(sql_func, sizeof(sql_func), "SELECT CURRENT_TIME(%d) ",
			svfo->typmod);
		is_datetime = true;
		break;
	case SVFOP_CURRENT_TIMESTAMP:
		nw = snprintf(sql_func, sizeof(sql_func), "SELECT CURRENT_TIMESTAMP ");
		is_datetime = true;
		break;
	case SVFOP_CURRENT_TIMESTAMP_N:
		nw = snprintf(sql_func, sizeof(sql_func), "SELECT CURRENT_TIMESTAMP(%d) ",
			svfo->typmod);
		is_datetime = true;
		break;
	case SVFOP_LOCALTIME:
		nw = snprintf(sql_func, sizeof(sql_func), "SELECT LOCALTIME ");
		is_datetime = true;
		break;
	case SVFOP_LOCALTIME_N:
		nw = snprintf(sql_func, sizeof(sql_func), "SELECT LOCALTIME(%d) ",
			svfo->typmod);
		is_datetime = true;
		break;
	case SVFOP_LOCALTIMESTAMP:
		nw = snprintf(sql_func, sizeof(sql_func), "SELECT LOCALTIMESTAMP ");
		is_datetime = true;
		break;
	case SVFOP_LOCALTIMESTAMP_N:
		nw = snprintf(sql_func, sizeof(sql_func), "SELECT LOCALTIMESTAMP(%d) ",
			svfo->typmod);
		is_datetime = true;
		break;
	case SVFOP_CURRENT_ROLE:
		nw = snprintf(sql_func, sizeof(sql_func), "SELECT CURRENT_ROLE ");
		break;
	case SVFOP_CURRENT_USER:
		nw = snprintf(sql_func, sizeof(sql_func), "SELECT CURRENT_USER ");
		break;
	case SVFOP_USER:
		nw = snprintf(sql_func, sizeof(sql_func), "SELECT USER ");
		break;
	case SVFOP_SESSION_USER:
		nw = snprintf(sql_func, sizeof(sql_func), "SELECT SESSION_USER ");
		break;
	case SVFOP_CURRENT_CATALOG:
		nw = snprintf(sql_func, sizeof(sql_func), "SELECT CURRENT_CATALOG ");
		break;
	case SVFOP_CURRENT_SCHEMA:
		nw = snprintf(sql_func, sizeof(sql_func), "SELECT CURRENT_SCHEMA ");
		break;
	default:
		nw = -2;
		break;
	}

	if (nw >= sizeof(sql_func))
	{
		nw = -2;
		goto end;
	}

	// execute the sql function and return the value.
	SPI_connect();

	// always use standard date/time/ts values in UTC+0.
	pg_tz *gmt_tz = pg_tzset("GMT"), *origtz = NULL;
	int orig_datestyle = -1, orig_dateorder = -1, orig_intvstyle = -1;
	if (is_datetime)
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
	
	int ret = SPI_execute(sql_func, true, 0);
    if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
        elog(ERROR, "SPI_execute failed for function %s: error code %d", sql_func, ret);
	}

    if (SPI_processed != 1)
	{
		SPI_finish();
        elog(ERROR, "SPI_execute returned %lu rows executing function %s, but 1 row is expected.",
			 SPI_processed, sql_func);
	}

	char *val = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
	if (is_datetime) val = pg_to_mysql_const(TIMESTAMPTZOID, val);
	nw = appendStringInfo(str_res, "'%s'", val);

	if (orig_datestyle != -1)
	{
		DateStyle = orig_datestyle;
		DateOrder = orig_dateorder;
		IntervalStyle = orig_intvstyle;
		session_timezone = origtz;
	}
	SPI_finish();
end:
	return nw;
}

static int append_expr_list(StringInfo str, List *l, RemotePrintExprContext *rpec)
{
	int nw = 0, nw1 = 0, cnt = 0;
	ListCell *lc;
	foreach(lc, l)
	{
		if (cnt++ < rpec->skip_n) continue;
		APPEND_EXPR(lfirst(lc));
		if (lnext(lc))
		{
			APPEND_CHAR(',');
		}
	}
	return nw;
}

static int eval_nextval_expr(StringInfo str, NextValueExpr *nve)
{
    int64       newval = nextval_internal(nve->seqid, false);
	int nw = 0, nw1 = 0;

    switch (nve->typeId)
    {    
        case INT2OID:
            APPEND_STR_FMT("%d", (int16) newval);
			break;
        case INT4OID:
            APPEND_STR_FMT("%d", (int32) newval);
            break;
        case INT8OID:
            APPEND_STR_FMT("%ld", (int64) newval);
            break;
        default:
            elog(ERROR, "unsupported sequence type %u", nve->typeId);
    }

	return nw;
}

/*
  Numeric functions, the IDs are manually assigned in
  include/catalog/pg_proc.dat, and this table is in fmgrtab.c auto-generated
  by a script. If in future versions the numbering changes we must modify the
  NUMERIC_FUNC_ID_START and NUMERIC_FUNC_ID_MAX macros.

  { 1718, "numeric_eq", 2, true, false, numeric_eq },
  { 1719, "numeric_ne", 2, true, false, numeric_ne },
  { 1720, "numeric_gt", 2, true, false, numeric_gt },
  { 1721, "numeric_ge", 2, true, false, numeric_ge },
  { 1722, "numeric_lt", 2, true, false, numeric_lt },
  { 1723, "numeric_le", 2, true, false, numeric_le },
  { 1724, "numeric_add", 2, true, false, numeric_add },
  { 1725, "numeric_sub", 2, true, false, numeric_sub },
  { 1726, "numeric_mul", 2, true, false, numeric_mul },
  { 1727, "numeric_div", 2, true, false, numeric_div },
  { 1728, "numeric_mod", 2, true, false, numeric_mod },
  { 1729, "numeric_mod", 2, true, false, numeric_mod },
  { 1730, "numeric_sqrt", 1, true, false, numeric_sqrt },
  { 1731, "numeric_sqrt", 1, true, false, numeric_sqrt },
  { 1732, "numeric_exp", 1, true, false, numeric_exp },
  { 1733, "numeric_exp", 1, true, false, numeric_exp },
  { 1734, "numeric_ln", 1, true, false, numeric_ln },
  { 1735, "numeric_ln", 1, true, false, numeric_ln },
  { 1736, "numeric_log", 2, true, false, numeric_log },
  { 1737, "numeric_log", 2, true, false, numeric_log },
  { 1738, "numeric_power", 2, true, false, numeric_power },
  { 1739, "numeric_power", 2, true, false, numeric_power },
*/

/*
  >0 args have been serialized, return NO. of bytes appended to str;
  < 0 if numeric function name has been printed, caller need to print
  	arglist, and -retval is NO. of bytes written.
  == 0 if snprint_expr error.
*/
static int convert_numeric_func(Oid funcid, List *args, StringInfo str,
	RemotePrintExprContext *rpec)
{
	typedef struct funcid_name
	{
		Oid funcid;
		const char *funcname;
	} funcid_name;
/*
 { 1703, "numeric", 2, true, false, numeric }, return args[0]
  { 1704, "numeric_abs", 1, true, false, numeric_abs },
  { 1705, "numeric_abs", 1, true, false, numeric_abs },
  { 1706, "numeric_sign", 1, true, false, numeric_sign },
  { 1707, "numeric_round", 2, true, false, numeric_round },
  { 1711, "numeric_ceil", 1, true, false, numeric_ceil },
  { 1712, "numeric_floor", 1, true, false, numeric_floor },
  { 1915, "numeric_uplus", 1, true, false, numeric_uplus },
   { 1973, "numeric_div_trunc", 2, true, false, numeric_div_trunc },
  { 1980, "numeric_div_trunc", 2, true, false, numeric_div_trunc },
  { 2167, "numeric_ceil", 1, true, false, numeric_ceil },
  { 2169, "numeric_power", 2, true, false, numeric_power },
  { 111, "numeric_fac", 1, true, false, numeric_fac }, mysql has no factorial func, not supported
   { 1376, "numeric_fac", 1, true, false, numeric_fac },

     { 1764, "numeric_inc", 1, true, false, numeric_inc },
    { 1766, "numeric_smaller", 2, true, false, numeric_smaller },
  { 1767, "numeric_larger", 2, true, false, numeric_larger },
  { 1771, "numeric_uminus", 1, true, false, numeric_uminus },
*/
	static funcid_name funcmap [] = {
		{ 1704, "abs"},
		{ 1705, "abs"},
		{ 1706, "sign"},
		{ 1707, "round"},
		{ 1711, "ceil"},
		{ 1712, "floor"},
		{ 1718, " = "},
		{ 1719, " != "},
		{ 1720, " > "},
		{ 1721, " >= "},
		{ 1722, " < "},
		{ 1723, " <= "},
		{ 1724, " + "},
		{ 1725, " - "},
		{ 1726, " * "},
		{ 1727, " / "},
		{ 1728, " % "},
		{ 1729, " % "},
		{ 1730, " sqrt "},
		{ 1731, " sqrt "},
		{ 1732, " exp "},
		{ 1733, " exp "},
		{ 1734, " ln "},
		{ 1735, " ln "},
		{ 1736, " log "},
		{ 1737, " log "},
		{ 1738, " power "},
		{ 1739, " power "},
		{ 1764, "inc"},
		{ 1766, " < "},
		{ 1767, " > "},
		{ 1771, " -"}, // unary -
		{ 1915, " "}, // unary +
		{ 1973, "div"},
		{ 1980, "div"},
		{ 2167, "ceil"},
		{ 2169, "power"}

	};

	funcid_name *pos = bsearch(&funcid, funcmap, sizeof(funcmap)/sizeof(funcid_name),
		sizeof(funcid_name), func_id_cmp);
	
	int nw = 0, nw1 = 0;

	if (funcid == 1771 || funcid == 1915)
	{
		Assert(list_length(args) == 1);
		APPEND_CHAR('(');
		appendStringInfoChar(str, funcid == 1771 ? '-' : ' ');
		nw++;
		APPEND_EXPR(linitial(args));
		if (nw1 < 0) return 0;
		APPEND_CHAR(')');
		return nw;
	}
	else if ((funcid >= NUMERIC_FUNC_ID_START && funcid <= NUMERIC_FUNC_OPER_ID_MAX) ||
		(funcid == 1766 || funcid == 1767))
	{
		Assert(list_length(args) == 2);
		APPEND_CHAR('(');
		APPEND_EXPR(linitial(args));
		if (nw1 < 0) return 0;
		appendStringInfoString(str, pos->funcname);
		nw += strlen(pos->funcname);
		APPEND_EXPR(lsecond(args));
		if (nw1 < 0) return 0;
		APPEND_CHAR(')');
		return nw;
	}
	else if (funcid == 1703)
	{
		int typmod = -1;
		if (list_length(args) == 1) goto noprec;// no precision or scale specified

		Expr *tme = (Expr*)lsecond(args);
		if (!IsA(tme, Const)) return 0;
		typmod = ((Const *)tme)->constvalue;
	    if (typmod < (int32) (VARHDRSZ))
	    {
			return 0;// can't serialize
	    }
	
		int precision = ((typmod - VARHDRSZ) >> 16) & 0xffff;
		int scale = (typmod - VARHDRSZ) & 0xffff;
		if (scale < 0 || scale > 30 || precision <= 0 || precision > 65)
			return 0;
noprec:
		APPEND_STR(" CAST(");
		APPEND_EXPR(linitial(args));
		if (nw1 < 0) return 0;
		APPEND_STR(" as DECIMAL");
		if (typmod > 0)
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

	Assert(pos);
	appendStringInfoString(str, pos->funcname);
	nw = strlen(pos->funcname);
	return -nw; // caller need to continue to serialize the args of the function call
}

static ParamExternData * eval_extern_param_val(ParamListInfo paramInfo, int paramId)
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
