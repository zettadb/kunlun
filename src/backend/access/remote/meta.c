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
 *
 * INTERFACE ROUTINES
 *      make_remote_create_table_stmt1
 *      make_remote_create_table_stmt2
 *      make_remote_create_table_stmt_end
 *      show_remote_sql
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

extern const char *format_type_remote(Oid type_oid);
static void scan_all_schemas_drop(const char *db, StringInfo stmt);
static void SetRemoteDDLInfo(Relation rel, DDL_OP_Types optype, DDL_ObjTypes objtype);
static enum enum_sql_command
ddl_op_sqlcom(DDL_OP_Types optype, DDL_ObjTypes objtype);
static void TrackRemoteDropPartitionedTable(Relation rel, bool is_cascade);
static void RemoteDatabaseDDL(const char *db, const char *schema,
	char *cmdbuf, size_t retlen, bool iscreate);
static void NotifyDBApplierGone(int idx, Oid dbid);
static void TrackRemoteDropSequence(Relation rel);
static void build_change_column_stmt(void);
static void build_column_data_type(StringInfo str, Oid typid, int32 typmod, Oid collation);
static bool AlteredSomeTables(void);
static void print_str_list(StringInfo str, List *ln, char seperator);
static bool PrintParserTree(StringInfo str, Node*val);
static bool print_expr_list(StringInfo str, List *exprlist);
static void
update_index_attrname(Relation attrelation, Relation targetrelation,
	Oid indid, int16 attnum,
	const char *oldattname, const char *newattname,
	bool check_attname_uniquness);
static void get_indexed_cols(Oid indexId, Datum **keys, int *nKeys);
extern const char *current_authorization();
extern const char *current_role();

extern bool skip_tidsync;

bool use_mysql_native_seq = true;
typedef struct Object_ref
{
	Oid id;
	struct Object_ref *next;
	NameData name;
} Object_ref;

typedef struct AlterTableColumnAction {
	bool nullable;  // new nullability
	bool unique;

	/*
  	  In pg column rename is always seperate & independent from
	  redefining(altering type/nullability) ops, so below fields are never both
	  true and we either do 'modify column' or 'rename column' in mysql, don't
	  do 'change column' ever.
	
	*/
#define ATCA_ADD 'a'
#define ATCA_DROP 'd'
#define ATCA_RENAME 'r'
#define ATCA_MODIFY 'm'
	char action;

	Oid typid;	// the new type id
	struct AlterTableColumnAction *next;

	// default value str, used by 'add column' subcmd only. have to send this
	// down to storage shards in order to add default value for the new field
	// of existing rows.
	StringInfoData def_valstr;
	StringInfoData col_dtype;

	NameData name; // current colummn name
	NameData newname; // new colummn name, used in rename action.
} AlterTableColumnAction;

typedef struct Remote_shard_ddl_context
{
	/*
	  Used in 'create table' stmt.
	  The table and its associated objects such as sequences will all be
	  created on this shard. If the table is a non-leaf partitioned table,
	  all its associated objects will be created in this shard.
	*/
	Oid target_shard_id;
	uint16_t tables_handled;


	// put Relation here if needed.
	StringInfoData remote_ddl;
	StringInfoData remote_ddl_tail;

	/*
	  In 'alter table' stmt, there could be multiple leaf tables modified with
	  the same actions either written in remote_ddl or recorded in changed_cols,
	  but not both. and in pg, alter table commands
	  always cause the same actions made to a group of leaf tables of one
	  partitioned table, or one regular table.
	*/
	Object_ref altered_tables;
	
	/*
	  Modified columns of regular/leaf tables in altered_tables.
	*/
	AlterTableColumnAction changed_cols;

	struct Remote_shard_ddl_context *next;
} Remote_shard_ddl_context;

/*
  Statement tracking state.
*/
typedef struct CurrentRemoteStatement
{
	enum NodeTag top_stmt_tag;
	/*
	 * Any DDL stmt always operate in one way on one type of object, and on
	 * a single object. Note that we have to forbid 'DROP TABLE' to drop
	 * multiple tables in one statement.
	 * */
	DDL_ObjTypes objtype;
	DDL_OP_Types optype;
	/*
	 * True if operating on a partitioned table which has one or more leaf
	 * partitions;
	 * and true if dropping an index of a partitioned table, which requires
	 * dropping the indexes on all leaf partitions.
	 *
	 * This field is currently well maintained but not used, it maybe useful
	 * in the future.
	 * */
	bool is_partitioned;

	/*
	 * The 'CASCADE' option for drop index/table.
	 * */
	bool cascade;

	/*
	 * True if 'ONLY' set to 'CREATE INDEX' stmt, and leaf relations won't be
	 * indexed.
	 * */
	bool skip_indexing_leaves;
	struct CurrentRemoteStatement *next;
	NameData schema_name;
	NameData obj_name;
} CurrentRemoteStatement;

typedef struct Remote_ddl_context
{
	/*
	  One user DDL stmt may generate multiple DDL stmts to be executed in a
	  computing node.
	*/
	CurrentRemoteStatement curstmt;

	/*
	 * The SQL text received from client and to be executed by other
	 * computing nodes.
	 * */
	StringInfoData ddl_sql_src;

	/*
	 * The XA txn ID executed in metadata cluster to log the DDL op in log_ddl_op().
	 * It should be committed at end of txn in step #c, or aborted on error of
	 * step #b or step #c.
	 * */
	const char *metadata_xa_txnid;

	/*
	 * ddl op id returned from metadata cluster.
	 * */
	uint64_t ddl_log_op_id;

	/*
	 * Original SQL text sent from client.
	 * */
	const char *orig_sql;
	Remote_shard_ddl_context *tail;
	Remote_shard_ddl_context shards;

	/*
	 * If dropping a partitioned table PT, the 3 fields should be the schema
	 * name and object name of PT instead of its children's names.
	 * These fields should be allocated in txn cxt, they are not freed
	 * explicitly.
	 * */
	NameData db_name;
} Remote_ddl_context;

static Remote_ddl_context g_remote_ddl_ctx;
static CurrentRemoteStatement *g_curr_stmt = &g_remote_ddl_ctx.curstmt;
static CurrentRemoteStatement *g_root_stmt = &g_remote_ddl_ctx.curstmt;

// by default always use 1st one.
static StringInfo g_remote_ddl = &g_remote_ddl_ctx.shards.remote_ddl;
static StringInfo g_remote_ddl_tail = &g_remote_ddl_ctx.shards.remote_ddl_tail;


// GUC variables
bool replaying_ddl_log = false;
char *remote_stmt_ptr = NULL;
int str_key_part_len = 64;

static StringInfoData last_remote_ddl_sql;

static Remote_shard_ddl_context *GetShardDDLCtx(Oid shardid);
static AlterTableColumnAction*
FindChangedColumn(Remote_shard_ddl_context *rsdc, const char *colName);
static bool AddAlteredTable(Remote_shard_ddl_context *rsdc, Oid id, const char *name);
static bool fetch_column_constraints(ColumnDef *column, AlterTableColumnAction *csd);
static void fetch_column_data_type(AlterTableColumnAction *csd, Oid typid, int32 typmod, Oid collOid);

/*
 * true if operating on a partitioned table, or an index of a partitioned table.
 * false otherwise.
 * */
inline static void set_op_partitioned(bool b)
{
	g_root_stmt->is_partitioned = b;
}

inline static bool remote_skip_rel(Relation rel)
{
	Form_pg_class rd_rel = rel->rd_rel;
	if ((rd_rel->relshardid == InvalidOid &&
		 rd_rel->relkind != RELKIND_PARTITIONED_INDEX && 
		 rd_rel->relkind != RELKIND_PARTITIONED_TABLE) ||
		rd_rel->relpersistence == RELPERSISTENCE_TEMP)
		return true;
	return false;
}


// Not a GUC var anymore, and MUST ALWAYS keep it on.
bool enable_remote_relations = true;

/*
  whether current txn was started by current command. When executing DDLs we
  require the stmt be an autocommit txn.

  -1: UNKNOWN; 0: FALSE; 1: TRUE
*/
static int g_curtxn_started_curcmd = -1;
bool enable_remote_ddl()
{
	return enable_remote_relations && IsNormalProcessingMode() &&
		   IsUnderPostmaster && replaying_ddl_log == false;
}

static void check_ddl_txn_status()
{
	if (g_curtxn_started_curcmd != 1)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("Can not execute DDL statements in an explicit transaction in kunlun-db.")));
}


void set_curtxn_started_curcmd(int i)
{
	if (g_curtxn_started_curcmd == -1) g_curtxn_started_curcmd = i;
}

void init_curtxn_started_curcmd()
{
	g_curtxn_started_curcmd = -1;
}

void InitRemoteDDLContext()
{
	g_remote_ddl_ctx.tail = &g_remote_ddl_ctx.shards;
	initStringInfo2(&g_remote_ddl_ctx.shards.remote_ddl, 1024, TopMemoryContext);
	initStringInfo2(&g_remote_ddl_ctx.shards.remote_ddl_tail, 256, TopMemoryContext);
	initStringInfo2(&g_remote_ddl_ctx.ddl_sql_src, 512, TopMemoryContext);
	initStringInfo2(&last_remote_ddl_sql, 1024, TopMemoryContext);
}

/*
 * Stmts like 'drop table' and 'create index', if operand is a partition table,
 * then one such stmt can produce multiple 'drop table', 'create index' stmts
 * to be sent to multiple shards. We accumulate stmts for each shard seperately
 * into distinct Remote_shard_ddl_context objects.
 * */
static Remote_shard_ddl_context *GetShardDDLCtx(Oid shardid)
{
	Remote_shard_ddl_context *psdc = &g_remote_ddl_ctx.shards;
	while (psdc)
	{
		if (psdc->target_shard_id == shardid)
			return psdc;
		if (psdc->target_shard_id == InvalidOid)
		{
			psdc->target_shard_id = shardid;
			return psdc;
		}

		psdc = psdc->next;
	}

	psdc = MemoryContextAlloc(TopMemoryContext, sizeof(Remote_shard_ddl_context));
	psdc->target_shard_id = shardid;
	psdc->tables_handled = 0;
	psdc->altered_tables.next = NULL;
	psdc->changed_cols.next = NULL;
	memset(&psdc->altered_tables, 0, sizeof(psdc->altered_tables));
	memset(&psdc->changed_cols, 0, sizeof(psdc->changed_cols));
	initStringInfo2(&psdc->remote_ddl, 1024, TopMemoryContext);
	initStringInfo2(&psdc->remote_ddl_tail, 256, TopMemoryContext);
	psdc->next = NULL;
	g_remote_ddl_ctx.tail->next = psdc;
	g_remote_ddl_ctx.tail = psdc;
	return psdc;
}

void RemoteDDLCxtStartStmt(int top_stmt, const char *sql)
{
	/*
	  Caller may be recursively called and thus so is this function,
	  but we always want the top most orginal sql text. and we make more
	  command state if the execution of one user DDL generates more than
	  one user stmts.
	*/
	if (g_curr_stmt->top_stmt_tag == T_Invalid)
	{
		g_remote_ddl_ctx.orig_sql = sql;
	}
	else
	{
		g_curr_stmt->next = (CurrentRemoteStatement *)MemoryContextAllocZero(
			TopTransactionContext, sizeof(CurrentRemoteStatement));
		g_curr_stmt = g_curr_stmt->next;
	}

	g_curr_stmt->top_stmt_tag = top_stmt;
}

int CurrentCommand()
{
	return g_curr_stmt->top_stmt_tag;
}

static const char *find_create_table_body(const char *sql, bool *is_part_leaf)
{
	// regex is overkill, we simply want to find the start of 'partition of' clause.
	// sql here is a valid 'create table' stmt, it has only 2 forms:
	// create table NAME (...) and create table NAME partition of PARENT-TABLE...;
	const char *part_start = strcasestr(g_remote_ddl_ctx.orig_sql, "partition");
	for (const char *p = part_start + 9; part_start && *p;)
	{
		const char *q;
		if (isspace(*p))
			p++;
		else if ((q = strcasestr(p, "of")) && isspace(q[2]))
		{
			if (is_part_leaf) *is_part_leaf = true;

			return part_start;
		}
		else
			break;
	}

	if (is_part_leaf) *is_part_leaf = false;
	return strchr(g_remote_ddl_ctx.orig_sql, '(');
}

/*
 * Make a 'create table' stmt for a base table or leaf partition table,
 * not including the indexing part.
 * */
void make_remote_create_table_stmt1(Relation rel, const TupleDesc tupDesc)
{
	if (!enable_remote_ddl() || remote_skip_rel(rel)) return;
	check_ddl_txn_status();
	Assert(g_root_stmt->top_stmt_tag == T_CreateStmt);

	const char *relname = rel->rd_rel->relname.data;
	appendStringInfo(g_remote_ddl, "%c create table if not exists %s (",
		g_remote_ddl->len > 0 ? ';':' ',
		make_qualified_name(rel->rd_rel->relnamespace, relname, NULL));

	SetRemoteDDLInfo(rel, DDL_OP_Type_create, DDL_ObjType_table);
	/*
	 * users can't add with() params otherwise this generated stmts will have
	 * 2 with clauses and that's an error.
	 * */
	bool is_part_leaf = false;
	const char *body_start = find_create_table_body(g_remote_ddl_ctx.orig_sql, &is_part_leaf);
	if (!body_start)
	{
		/*
		  Creating a table not via CREATE TABLE stmts but something else, possible
		  alternatives:
		  SELECT * INTO TABLE dest_table FROM src_table;
		*/
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("Kunlun-db: The way of creating a table not supported.")));
	}

	const char *body_end = strrchr(body_start, ')');
	if (!body_end)
	{
		if (is_part_leaf && strcasestr(body_start, "default"))
			body_end = body_start + strlen(body_start);
		else
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("Kunlun-db: Unsupported 'CREATE TABLE' syntax")));
	}

	int body_len = body_end - body_start + 1;
	appendStringInfo(&g_remote_ddl_ctx.ddl_sql_src, "create table %s.%s.%s ",
		g_remote_ddl_ctx.db_name.data, g_root_stmt->schema_name.data,
		g_root_stmt->obj_name.data);
	appendBinaryStringInfo(&g_remote_ddl_ctx.ddl_sql_src, body_start, body_len);

	/*
	  TODO: user create table stmt can't have with options otherwise this code
	  would create another with option and will cause parse error on peers.
	*/
	appendStringInfo(&g_remote_ddl_ctx.ddl_sql_src, " with(shard=%u)",
		rel->rd_rel->relshardid);

	for (int i = 0; i < tupDesc->natts; i++)
	{
		Form_pg_attribute attrs = tupDesc->attrs + i;
		appendStringInfo(g_remote_ddl, "%s ", attrs->attname.data);
		build_column_data_type(g_remote_ddl, attrs->atttypid, attrs->atttypmod,
			attrs->attcollation);

		if (attrs->attnotnull)
		{
			appendStringInfoString(g_remote_ddl, " not null");
		}

		if (i < tupDesc->natts - 1)
		{
			appendStringInfoChar(g_remote_ddl, ',');
		}
	}

	// add table options for mysql 'create table' stmt.

	if (rel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED)
	{
		appendStringInfoString(g_remote_ddl_tail, " engine=MyISAM ");
	}

	// TODO: add allowed mysql table options as storage options if any is set.
	
}

void SetRemoteContextShardId(Oid shardid)
{
	Oid *poid = &g_remote_ddl_ctx.shards.target_shard_id;
	Assert((*poid == InvalidOid || *poid == shardid) && shardid != InvalidOid);
	if (*poid == InvalidOid) *poid = shardid;
}

Oid GetRemoteContextShardId()
{
	return g_remote_ddl_ctx.shards.target_shard_id;
}

inline static const char *get_am_name(Oid id)
{
  static const Oid am_ids[] = {
	BTREE_AM_OID, HASH_AM_OID, GIST_AM_OID, GIN_AM_OID, SPGIST_AM_OID, BRIN_AM_OID
  };

  static const char *am_names[] = {
		"btree", "hash", "gist", "gin", "spgist", "brin"
  };

  for (int i = 0; i < sizeof(am_ids)/sizeof(Oid); i++)
	  if (id == am_ids[i])
		  return am_names[i];
  return NULL;
}

void make_remote_create_table_stmt2(
	Relation indexrel, Relation heaprel, const TupleDesc tupDesc, bool is_primary,
	bool is_unique, int16*coloptions)
{
	if (!enable_remote_ddl() || remote_skip_rel(heaprel)) return;
	check_ddl_txn_status();
	/*
	 * Only produce 'create index' and send to backend storage nodes for
	 * partition leaf tables or regular tables.
	 * */
	if (heaprel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		return;

	bool is_create_tab = false;
	if (g_root_stmt->top_stmt_tag == T_Invalid)
	{
		g_root_stmt->top_stmt_tag = T_IndexStmt;
	}
	else
	{
		Assert(g_root_stmt->top_stmt_tag == T_CreateStmt ||
			   g_root_stmt->top_stmt_tag == T_IndexStmt ||
			   g_root_stmt->top_stmt_tag == T_AlterTableStmt);
		if (g_root_stmt->top_stmt_tag == T_CreateStmt)
		{
			is_create_tab = true;
			Assert(g_root_stmt->objtype == DDL_ObjType_table);
		}
	}

	static const char *idxtype_pk = "PRIMARY KEY";
	static const char *idxtype_uniq = "UNIQUE INDEX";
	static const char *idxtype_other = "INDEX";
	static const char *idxam_bt = "BTREE";
	static const char *idxam_hash = "HASH";

	Oid amid = indexrel->rd_rel->relam;

	if (amid != BTREE_AM_OID && amid != HASH_AM_OID)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("Kunlin-db: Remote relations only support btree and hash indexing method.")));

	int ret = 0;
	if (is_create_tab)
		ret = appendStringInfo(g_remote_ddl, ", %s %s using %s (",
					   is_primary ? idxtype_pk : (is_unique ? idxtype_uniq : idxtype_other),
					   is_primary ? "" : indexrel->rd_rel->relname.data, // mysql PKs don't have customized names
					   amid == BTREE_AM_OID ? idxam_bt : idxam_hash
					  );
	else
	{
		/*
		 * If 'create index' is done against a partitioned table, there can
		 * be multiple create index stmts generated, one for each leaf partition.
		 * So here we direct g_remote_ddl to the right shard first.
		 * */
		g_remote_ddl = &(GetShardDDLCtx(heaprel->rd_rel->relshardid)->remote_ddl);

		ret = appendStringInfo(g_remote_ddl, "%screate %s index %s using %s on %s (",
					   (!is_create_tab && g_remote_ddl->len > 0) ? "; " : "", // split away prev 'create index' stmt.
					   is_unique ? "unique" : "",
					   indexrel->rd_rel->relname.data,
					   amid == BTREE_AM_OID ? idxam_bt : idxam_hash,
					   make_qualified_name(heaprel->rd_rel->relnamespace, heaprel->rd_rel->relname.data, NULL)
					  );

		if (indexrel->rd_rel->relnamespace != heaprel->rd_rel->relnamespace)
		{
			ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					errmsg("The schema of the index relation (%s, %u) in kunlun-db must be the same as its main relation(%s, %u).",
						   indexrel->rd_rel->relname.data, indexrel->rd_rel->relnamespace,
						   heaprel->rd_rel->relname.data, heaprel->rd_rel->relnamespace)));
		}

	}

	char keypartlenstr[32] = {'\0'};
	// the first indnkeyattrs fields are key columns, the rest are included columns.
	for (int natt = 0; natt < IndexRelationGetNumberOfKeyAttributes(indexrel); natt++)
	{
		int         attno = indexrel->rd_index->indkey.values[natt];
		if (attno <= 0)
			ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					errmsg("Can not index system columns.")));

		keypartlenstr[0] = '\0';
		Form_pg_attribute attrs = heaprel->rd_att->attrs + attno - 1;
		if (needs_mysql_keypart_len(attrs->atttypid, attrs->atttypmod))
		{
			snprintf(keypartlenstr, sizeof(keypartlenstr), "(%d)", str_key_part_len);
		}

		ret = appendStringInfo(g_remote_ddl, "%s%s %s, ", attrs->attname.data, keypartlenstr,
			(amid == BTREE_AM_OID) ? ((coloptions[natt] & INDOPTION_DESC) ? "DESC" : "ASC") : "");
	}

	g_remote_ddl->len -= 2;
	appendStringInfoChar(g_remote_ddl, ')');
}


/*
 * called at the end of one statement. A single query sent from user can result
 * in multiple statements executed internally in pg, for example a 'create index'
 * statement executed against a partitioned table with multiple leaf partitions
 * will execute 'create index' stmts for each leaf partition. we want to send each
 * stmt individully to each target storage node, and finally when the entire user
 * stmt is executed we need to do some cleanup work, so we use 'is_top_level' to
 * distinguish the 2 scenarios.
 * */
void end_remote_ddl_stmt()
{
	if (!enable_remote_ddl()) return;

	/*
	  Some shards may not have DDL work to do but others have, for example a
	  new sequence is created on a shard but the existing table  is on another
	  shard. We don't try to create the sequence on the same shard as its table
	  since pg's perfect modularity makes this hard to do and we don't want to
	  break that modularity.
	*/
	if ((((g_root_stmt->top_stmt_tag == T_AlterTableStmt ||
		 g_root_stmt->top_stmt_tag == T_RenameStmt) &&
		g_root_stmt->optype == DDL_OP_Type_alter &&
		g_root_stmt->objtype == DDL_ObjType_table) ||
		g_root_stmt->top_stmt_tag == T_AlterEnumStmt) &&
		AlteredSomeTables())
		build_change_column_stmt();

	/*
	  During Kunlun cluster bootstrap, before a computing node is added to a
	  cluster, it doesn't know the metashard, and can't send DDL logs, just as
	  it can't do tidsync either. So we use skip_tidsync for this check. And
	  as such DDL ops are executed for every CN, there is no need for
	  replicating them either.
	  Creating a partitioned table involves no storage shard actions but it's
	  identified as a non-simple ddl in is_supported_simple_ddl_stmt(),
	  so we have to exclude this special case.
	*/
	if (g_remote_ddl->len == 0 &&
		((!is_supported_simple_ddl_stmt(g_root_stmt->top_stmt_tag) &&
		  g_root_stmt->objtype == DDL_ObjType_Invalid &&
		  g_root_stmt->optype == DDL_OP_Type_Invalid &&
		  !g_root_stmt->is_partitioned) ||
		  g_remote_ddl_ctx.ddl_sql_src.len == 0 || skip_tidsync))
		return;

	/*
	 * At start of the DDL execution, let's roughly check whether we have
	 * conflicting log entries to be executed, if so, fail.
	 * */
	if (check_ddl_op_conflicts_rough(get_metadata_cluster_conn(false),
			g_remote_ddl_ctx.db_name.data, g_root_stmt->objtype))
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("Kunlun-db: Pending conflicting DDL operation to execute before this DDL statement can be executed.")));

	const bool is_create_table =
		(g_root_stmt->objtype == DDL_ObjType_table &&
		 g_root_stmt->optype == DDL_OP_Type_create);
	/*
	 * a 'create table' stmt never can embed another 'create table' stmt
	 * since we don't use pg's sequence.
	 * */
	if (is_create_table && g_remote_ddl->len > 0)
	{
		/*
		  end of create table stmt. this isn't needed for 'create index' stmt.
		  if creating a partitioned table, no sql is sent to storage shard
		  unless creating aux objects such as sequences, etc, and we should not
		  append the ) in such cases.
		*/
		if (!g_root_stmt->is_partitioned)
			appendStringInfoString(g_remote_ddl, ")");

		if (g_remote_ddl_tail->len != 0)
		{
			appendBinaryStringInfo(g_remote_ddl, g_remote_ddl_tail->data,
								   g_remote_ddl_tail->len);
		}
	}

	StringInfoData sql_sn;
	initStringInfo2(&sql_sn, 1024, TopMemoryContext);
	appendStringInfoChar(&sql_sn, '[');
	int nshards = 0;
	Oid target_shardid = 0;
	struct timeval tv;

	gettimeofday(&tv, NULL);
	const char *xa_txnid = MakeTopTxnName(tv.tv_usec, tv.tv_sec);
	int ignore_err = 0;

	/*
	  for create index stmt, one shard can recv multiple stmts, which may half
	  execute, so must always add IF NOT EXISTS when re-executing at recovery.
	  there is no 'IF NOT EXISTS' for 'create index    ', or 'IF EXISTS' for 'drop index',
	  instead we can ignore the ER_CANT_DROP_FIELD_OR_KEY returned by
	  'drop index', and the ER_DUP_KEYNAME error returned by 'create index'.
	*/
	if (g_root_stmt->objtype == DDL_ObjType_index)
	{
		if (g_root_stmt->optype == DDL_OP_Type_create)
			ignore_err = ER_DUP_KEYNAME;

		if (g_root_stmt->optype == DDL_OP_Type_drop)
			ignore_err = ER_CANT_DROP_FIELD_OR_KEY;
	}

	resetStringInfo(&last_remote_ddl_sql);

	/*
	 * Step 1: log statement in metadata cluster.
	 *
	 * If sending to one storage shard, then in metadata op log entry,
	 * we store the {target shard id, sql text} as part of the row;
	 * if > 1 shards, we store {0, json-str}, the json-str is a json string
	 * of one array, in the array, each element is of this format:
	 * {"target_shard_id" : number, "sql_text": storage-sql-string}
	 * at recovery, storage node need to parse the string and find its
	 * own sql text to execute.
	 * */
	Remote_shard_ddl_context *rsdc = NULL;
	for (rsdc = &g_remote_ddl_ctx.shards;
		 rsdc && rsdc->target_shard_id != InvalidOid; rsdc = rsdc->next)
	{
		appendStringInfo(&sql_sn, "%s { \\\"target_shard_id\\\": %u, \\\"sql_text\\\": \\\"%*s\\\"}",
			nshards == 0 ? "" : ",", rsdc->target_shard_id,
			rsdc->remote_ddl.len, rsdc->remote_ddl.data);
		elog(LOG, "Executed DDL statement %*s on shard %u for txn '%s'.",
			 rsdc->remote_ddl.len, rsdc->remote_ddl.data,
			 rsdc->target_shard_id, xa_txnid);
		target_shardid = rsdc->target_shard_id;

		nshards++;
		if (nshards > 1)
		{
			target_shardid = 0;
			appendStringInfoString(&last_remote_ddl_sql, ";\n");
		}
		appendBinaryStringInfo(&last_remote_ddl_sql,
			rsdc->remote_ddl.data, rsdc->remote_ddl.len);
	}

	if (nshards == 1)
	{
		rsdc = &g_remote_ddl_ctx.shards;
		resetStringInfo(&sql_sn);
		appendBinaryStringInfo(&sql_sn, rsdc->remote_ddl.data, rsdc->remote_ddl.len);
	}
	else if (nshards > 1)
	{
		appendStringInfoChar(&sql_sn, ']');
	}
	else
	{
		// nshards == 0
		resetStringInfo(&sql_sn);
	}

	g_remote_ddl_ctx.metadata_xa_txnid = xa_txnid;
	uint64_t logid = log_ddl_op(get_metadata_cluster_conn(false), xa_txnid,
		g_remote_ddl_ctx.db_name.data, g_root_stmt->schema_name.data,
		current_role(), current_authorization(),
		g_root_stmt->obj_name.data, g_root_stmt->objtype,
		g_root_stmt->optype, g_remote_ddl_ctx.ddl_sql_src.data,
		sql_sn.data, target_shardid);
	g_remote_ddl_ctx.ddl_log_op_id = logid;

	/*
	 * Step 2: Send DDL statements if any to storage nodes.
	 * */
	for (rsdc = &g_remote_ddl_ctx.shards;
		 rsdc && rsdc->target_shard_id != InvalidOid; rsdc = rsdc->next)
	{
		Assert((rsdc->remote_ddl.data && rsdc->remote_ddl.len > 0) ||
			   (rsdc->remote_ddl.data == NULL && rsdc->remote_ddl.len == 0) ||
			   (rsdc->remote_ddl.data[0] == '\0' && rsdc->remote_ddl.len == 0));
		if (rsdc->remote_ddl.len > 0)
		{
			AsyncStmtInfo *asi = GetAsyncStmtInfo(rsdc->target_shard_id);
			asi->ignore_error = ignore_err;
			append_async_stmt(asi, rsdc->remote_ddl.data, rsdc->remote_ddl.len,
				CMD_DDL, false, ddl_op_sqlcom(g_root_stmt->optype,
											  g_root_stmt->objtype));
		}
	}
	send_multi_stmts_to_multi();

	/*
	 * Step 3: update local ddl operation log id.
	 * */
	update_my_max_ddl_op_id(logid, g_root_stmt->objtype == DDL_ObjType_db);
}

/*
 * GUC show hook for 'remote_sql' var.
 * We must define this function in this file in order to use the 'g_stmt_buf'
 * array. With gcc7 if we do 'extern char*g_stmt_buf;' in guc.c to use it there,
 * the 'g_stmt_buf' var in guc.c would be always 0 at runtime.
 * This is a compiler bug.
 * */
const char *
show_remote_sql(void)
{
	if (last_remote_ddl_sql.data[0])
		return last_remote_ddl_sql.data;
	else
		return "";
}

void ResetRemoteDDLStmt()
{

	Remote_shard_ddl_context *psdc = &g_remote_ddl_ctx.shards;
	while (psdc)
	{
		psdc->target_shard_id = InvalidOid;
		resetStringInfo(&psdc->remote_ddl);
		resetStringInfo(&psdc->remote_ddl_tail);
		psdc->tables_handled = 0;
		psdc->changed_cols.next = NULL;
		psdc->altered_tables.next = NULL;
		memset(&psdc->altered_tables, 0, sizeof(psdc->altered_tables));
		memset(&psdc->changed_cols, 0, sizeof(psdc->changed_cols));

		psdc = psdc->next;
	}

	memset(g_root_stmt, 0, sizeof(*g_root_stmt ));
	g_root_stmt->top_stmt_tag = T_Invalid;
	g_remote_ddl = &g_remote_ddl_ctx.shards.remote_ddl;
	g_remote_ddl_tail = &g_remote_ddl_ctx.shards.remote_ddl_tail;
	g_root_stmt->obj_name.data[0] = '\0';
	g_root_stmt->objtype = DDL_ObjType_Invalid;
	g_root_stmt->optype = DDL_OP_Type_Invalid;
	g_root_stmt->is_partitioned = false;
	g_root_stmt->schema_name.data[0] = '\0';
	g_remote_ddl_ctx.db_name.data[0] = '\0';
	resetStringInfo(&g_remote_ddl_ctx.ddl_sql_src);
	g_remote_ddl_ctx.metadata_xa_txnid = NULL;
	g_remote_ddl_ctx.ddl_log_op_id = 0;
	g_remote_ddl_ctx.orig_sql = NULL;
	g_root_stmt->skip_indexing_leaves = false;
	g_curr_stmt = g_root_stmt;
}


void TrackRemoteDropTableStorage(Relation rel)
{
	if (!enable_remote_ddl() || remote_skip_rel(rel)) return;
	check_ddl_txn_status();
	Oid relshardid = rel->rd_rel->relshardid;

	if (rel->rd_rel->relkind == RELKIND_SEQUENCE)
	{
		if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP)
			TrackRemoteDropSequence(rel);
		return;
	}

	Remote_shard_ddl_context *ddl_cxt = GetShardDDLCtx(relshardid);
	StringInfo str = &ddl_cxt->remote_ddl;

	if (ddl_cxt->tables_handled == 0)
	{
		appendStringInfo(str, "%c drop table if exists %s",
			(lengthStringInfo(str) == 0 ? ' ' : ';'),
			make_qualified_name(rel->rd_rel->relnamespace,
								rel->rd_rel->relname.data, NULL));
	}
	else
	{
		Assert(lengthStringInfo(str) > 0);
		appendStringInfo(str, ", %s",
			make_qualified_name(rel->rd_rel->relnamespace,
								rel->rd_rel->relname.data, NULL));
	}

	ddl_cxt->tables_handled++;
}


static void SetRemoteDDLInfo(Relation rel, DDL_OP_Types optype, DDL_ObjTypes objtype)
{
	get_database_name3(MyDatabaseId, &g_remote_ddl_ctx.db_name);
	get_namespace_name3(rel->rd_rel->relnamespace, &g_root_stmt->schema_name);
	strncpy(g_root_stmt->obj_name.data, rel->rd_rel->relname.data, sizeof(NameData));
	g_root_stmt->objtype = objtype;
	g_root_stmt->optype = optype;
}

static void TrackRemoteDropPartitionedTable(Relation rel, bool is_cascade)
{
	if (!enable_remote_ddl() || remote_skip_rel(rel)) return;
	check_ddl_txn_status();
	if (g_remote_ddl_ctx.db_name.data[0])
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("Can not drop more than one table in a statement in kunlun-db, drop tables one by one.")));
	g_root_stmt->cascade = is_cascade;
	SetRemoteDDLInfo(rel, DDL_OP_Type_drop, DDL_ObjType_table);
	set_op_partitioned(true);
	appendStringInfo(&g_remote_ddl_ctx.ddl_sql_src,
		"drop table if exists %s.%s.%s %s",
		g_remote_ddl_ctx.db_name.data, g_root_stmt->schema_name.data,
		g_root_stmt->obj_name.data, is_cascade ? "cascade" : "restrict");
}

void TrackRemoteCreatePartitionedTable(Relation rel)
{
	if (!enable_remote_ddl() || remote_skip_rel(rel)) return;
	check_ddl_txn_status();
	if (g_remote_ddl_ctx.db_name.data[0])
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("Can not drop more than one table in a statement in kunlun-db, drop tables one by one.")));
	SetRemoteDDLInfo(rel, DDL_OP_Type_create, DDL_ObjType_table);
	set_op_partitioned(true);

	appendStringInfo(&g_remote_ddl_ctx.ddl_sql_src,
		"create table if not exists %s.%s.%s %s",
		g_remote_ddl_ctx.db_name.data, g_root_stmt->schema_name.data,
		g_root_stmt->obj_name.data,
		strchr(g_remote_ddl_ctx.orig_sql, '('));
	Oid shardid = rel->rd_rel->relshardid;
	if (shardid == InvalidOid) shardid = g_remote_ddl_ctx.shards.target_shard_id;

	/*
	  The partitioned table may have some sequence columns and in this case we
	  must pass the shard=N param to peer computing nodes so that they can
	  associate in those nodes the table with its sequences stored in storage shards.
	  TODO: user create table stmt can't have with options otherwise this code
	  would create another with option and will cause parse error on peers.
	*/
	if (shardid != InvalidOid) 
	{
		char *end = strrchr(g_remote_ddl_ctx.ddl_sql_src.data, ')');
		end++;
		*end = '\0';
		g_remote_ddl_ctx.ddl_sql_src.len = end - g_remote_ddl_ctx.ddl_sql_src.data;
		appendStringInfo(&g_remote_ddl_ctx.ddl_sql_src, " with(shard=%u)", shardid);
	}
}

void RemoteDDLSetSkipStorageIndexing(bool b)
{
	g_root_stmt->skip_indexing_leaves = b;
}

bool is_metadata_txn(uint64_t *opid)
{
	*opid = g_remote_ddl_ctx.ddl_log_op_id;
	return g_remote_ddl_ctx.metadata_xa_txnid != NULL;
}

void end_metadata_txn(bool commit_it)
{
	static char txn_cmd[128];
	if (g_remote_ddl_ctx.metadata_xa_txnid == NULL)
		return;

	int len = snprintf(txn_cmd, sizeof(txn_cmd),
					   "XA %s '%s'", commit_it ? "COMMIT" : "ROLLBACK",
					   g_remote_ddl_ctx.metadata_xa_txnid);
	Assert(len < sizeof(txn_cmd));
	PG_TRY();
	send_stmt_to_cluster_meta(get_metadata_cluster_conn(false), txn_cmd,
							  len, CMD_UTILITY, false);
	PG_CATCH();
	// probably mysql_read/write timed out, or others. let cluster_mgr
	// take over to abort it. If XA COMMIT succeeded but timeout still happened
	// and an exception is thrown and caught here,
	// or other error happened after XA COMMIT succeeded,
	// that would require removing the computing node where
	// the DDL initially executed manually.
	disconnect_metadata_shard();
	ereport(WARNING,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("Kunlun-db: Error %s transaction branch '%s' on metadata shard "
			 		"when executing DDL statement '%s'",
			 		commit_it ? "committing" : "aborting",
					g_remote_ddl_ctx.metadata_xa_txnid,
					g_remote_ddl_ctx.orig_sql),
			 errhint("There could be unrevokable effects(i.e. leftover "
			 		 "tables/databases, etc) "
			 		 "in target storage shards. DBAs should manually check all"
					 "target shards and clear leftover effects.")));
	PG_RE_THROW();
	PG_END_TRY();
}

/*
 * 'relid' is the id of a relation to be dropped.
 * See if 'relid' is a partitioned table, if so, do bookkeeping properly.
 * */
void TrackRemoteDropTable(Oid relid, bool is_cascade)
{
	if (!enable_remote_ddl()) return;
	Relation rel = relation_open(relid, AccessExclusiveLock);
	if (remote_skip_rel(rel)) goto end;
	check_ddl_txn_status();
	TrackRemoteDropPartitionedTable(rel, is_cascade);
	if (rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
	{
		set_op_partitioned(false);
	}
end:
	relation_close(rel, NoLock);
}


void TrackRemoteDropIndex(Oid relid, bool is_cascade, bool drop_as_constr)
{
	if (!enable_remote_ddl()) return;
	Relation indexrel = relation_open(relid, AccessExclusiveLock);
	if (remote_skip_rel(indexrel)) goto end;
	check_ddl_txn_status();
	if (g_remote_ddl_ctx.db_name.data[0])
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("Kunlun-db: Can not drop more than one index in a statement, do so one by one.")));

	SetRemoteDDLInfo(indexrel, DDL_OP_Type_drop, DDL_ObjType_index);
	g_root_stmt->cascade = is_cascade;
	set_op_partitioned(indexrel->rd_rel->relkind == RELKIND_PARTITIONED_INDEX);
	if (drop_as_constr)
		appendStringInfoString(&g_remote_ddl_ctx.ddl_sql_src, g_remote_ddl_ctx.orig_sql);
	else
		appendStringInfo(&g_remote_ddl_ctx.ddl_sql_src,
		"drop index if exists %s.%s %s",
		g_root_stmt->schema_name.data, indexrel->rd_rel->relname.data,
		is_cascade ? "cascade" : "restrict");
end:
	relation_close(indexrel, NoLock);
}

void TrackRemoteCreateIndex(Relation heaprel, const char *idxname, Oid amid,
	bool is_unique, bool is_partitioned)
{
	if (!enable_remote_ddl() || remote_skip_rel(heaprel)) return;
	check_ddl_txn_status();
	if (g_remote_ddl_ctx.curstmt.top_stmt_tag != T_IndexStmt)
	{
		/*
		 * This is part of another operation such as 'CREATE TABLE'.
		 * */
		return;
	}

	SetRemoteDDLInfo(heaprel, DDL_OP_Type_create, DDL_ObjType_index);
	set_op_partitioned(is_partitioned);
	appendStringInfo(&g_remote_ddl_ctx.ddl_sql_src,
				"create %s index if not exists %s on %s.%s.%s using %s %s",
				is_unique ? "unique" : "",
				idxname,
				g_remote_ddl_ctx.db_name.data, g_root_stmt->schema_name.data,
				g_root_stmt->obj_name.data, get_am_name(amid),
				strchr(g_remote_ddl_ctx.orig_sql, '('));
}


void TrackRemoteDropIndexStorage(Relation rel)
{
	if (!enable_remote_ddl() || remote_skip_rel(rel)) return;
	check_ddl_txn_status();
	const DDL_ObjTypes objtype = g_root_stmt->objtype;
	/*
	  Could be dropping a temp table and/or its toast table and/or their index,
	  i.e. not a DDL performed by user to a normal table/index.
	*/
	if (!(objtype == DDL_ObjType_table || objtype == DDL_ObjType_index))
		return;

	/*
	 * Dropping the owner table, no need to generate 'drop index'
	 * stmt seperately for the storage node.
	 * */
	if (objtype == DDL_ObjType_table)
		return;

	Oid relshardid = rel->rd_rel->relshardid;

	Remote_shard_ddl_context *ddl_cxt = GetShardDDLCtx(relshardid);
	StringInfo str = &ddl_cxt->remote_ddl;
	Form_pg_index indexForm;
	Oid indexRelationId = RelationGetRelid(rel);
	HeapTuple indexTuple = SearchSysCache1(INDEXRELID,
								 ObjectIdGetDatum(indexRelationId));
	if (!HeapTupleIsValid(indexTuple))
		elog(ERROR, "cache lookup failed for index %u", indexRelationId);
	indexForm = (Form_pg_index) GETSTRUCT(indexTuple);
	Relation heaprel = relation_open(indexForm->indrelid, AccessExclusiveLock);
	ReleaseSysCache(indexTuple);

	if (lengthStringInfo(str) == 0)
	{
		appendStringInfo(str, "drop index %s on %s", rel->rd_rel->relname.data,
			make_qualified_name(heaprel->rd_rel->relnamespace,
								heaprel->rd_rel->relname.data, NULL));
	}
	else
	{
		appendStringInfo(str, "; drop index %s on %s", rel->rd_rel->relname.data,
			make_qualified_name(heaprel->rd_rel->relnamespace,
								heaprel->rd_rel->relname.data, NULL));
	}

	relation_close(heaprel, AccessExclusiveLock);
}

typedef struct OpObjSqlcom
{
	DDL_OP_Types optype;
	DDL_ObjTypes objtype;
	enum enum_sql_command sqlcom;
} OpObjSqlcom;

static enum enum_sql_command
ddl_op_sqlcom(DDL_OP_Types optype, DDL_ObjTypes objtype)
{
	const static OpObjSqlcom combinations[] = {
		{DDL_OP_Type_create, DDL_ObjType_db, SQLCOM_CREATE_DB},
		{DDL_OP_Type_create, DDL_ObjType_index, SQLCOM_CREATE_INDEX},
		{DDL_OP_Type_create, DDL_ObjType_matview, SQLCOM_END},
		{DDL_OP_Type_create, DDL_ObjType_partition, SQLCOM_CREATE_TABLE},
		{DDL_OP_Type_create, DDL_ObjType_schema, SQLCOM_CREATE_DB},
		{DDL_OP_Type_create, DDL_ObjType_seq, SQLCOM_CREATE_SEQUENCE},
		{DDL_OP_Type_create, DDL_ObjType_table, SQLCOM_CREATE_TABLE},

		{DDL_OP_Type_drop, DDL_ObjType_db, SQLCOM_DROP_DB},
		{DDL_OP_Type_drop, DDL_ObjType_index, SQLCOM_DROP_INDEX},
		{DDL_OP_Type_drop, DDL_ObjType_matview, SQLCOM_END},
		{DDL_OP_Type_drop, DDL_ObjType_partition, SQLCOM_DROP_TABLE},
		{DDL_OP_Type_drop, DDL_ObjType_schema, SQLCOM_DROP_DB},
		{DDL_OP_Type_drop, DDL_ObjType_seq, SQLCOM_DROP_SEQUENCE},
		{DDL_OP_Type_drop, DDL_ObjType_table, SQLCOM_DROP_TABLE},

		{DDL_OP_Type_rename, DDL_ObjType_db, SQLCOM_END},
		{DDL_OP_Type_rename, DDL_ObjType_index, SQLCOM_ALTER_TABLE},
		{DDL_OP_Type_rename, DDL_ObjType_matview, SQLCOM_END},
		{DDL_OP_Type_rename, DDL_ObjType_partition, SQLCOM_RENAME_TABLE},
		{DDL_OP_Type_rename, DDL_ObjType_schema, SQLCOM_END},
		/*
		  simply use SQLCOM_RENAME_TABLE for sequence rename, we don't rely
		  on the result of the mapping to do anything meaningful.
		*/
		{DDL_OP_Type_rename, DDL_ObjType_seq, SQLCOM_RENAME_TABLE},
		{DDL_OP_Type_rename, DDL_ObjType_table, SQLCOM_RENAME_TABLE},

		{DDL_OP_Type_alter, DDL_ObjType_db, SQLCOM_END},
		{DDL_OP_Type_alter, DDL_ObjType_index, SQLCOM_END},
		{DDL_OP_Type_alter, DDL_ObjType_matview, SQLCOM_END},
		{DDL_OP_Type_alter, DDL_ObjType_partition, SQLCOM_END},
		{DDL_OP_Type_alter, DDL_ObjType_schema, SQLCOM_END},
		{DDL_OP_Type_alter, DDL_ObjType_seq, SQLCOM_ALTER_TABLE},
		{DDL_OP_Type_alter, DDL_ObjType_table, SQLCOM_ALTER_TABLE},

		{DDL_OP_Type_replace, DDL_ObjType_db, SQLCOM_END},
		{DDL_OP_Type_replace, DDL_ObjType_index, SQLCOM_END},
		{DDL_OP_Type_replace, DDL_ObjType_matview, SQLCOM_END},
		{DDL_OP_Type_replace, DDL_ObjType_partition, SQLCOM_END},
		{DDL_OP_Type_replace, DDL_ObjType_schema, SQLCOM_END},
		{DDL_OP_Type_replace, DDL_ObjType_seq, SQLCOM_END},
		{DDL_OP_Type_replace, DDL_ObjType_table, SQLCOM_END}
	};

	for (int i = 0; i < sizeof(combinations)/sizeof(OpObjSqlcom); i++)
	{
		OpObjSqlcom v = combinations[i];
		if (optype == v.optype && objtype == v.objtype)
		{
			 return v.sqlcom; 
		}
	}

	return SQLCOM_END;
}

extern bool use_mysql_native_seq;
/*
 * Create a schema under current db.
 * When a schema is created in pg, need to create a db in mysql via this function.
 * When a db is created in pg, we also create a dbname_$$_public db in mysql.
 * TODO: port current pg db's encoding, collation and locale info to mysql's db.
 * */
void RemoteCreateSchema(const char *schemaName)
{
	static NameData dbname;
	static char cmdbuf[192];

	if (!enable_remote_ddl()) return;
	check_ddl_txn_status();
	get_database_name3(MyDatabaseId, &dbname);

	int len = snprintf(cmdbuf, sizeof(cmdbuf), "create database %s_$$_%s", dbname.data, schemaName);
	Assert(len < sizeof(cmdbuf));

	RemoteDatabaseDDL(dbname.data, schemaName, cmdbuf, len, true);
}

/*
 * Core for create/drop database/schema.
 * If creating/dropping a db, 'schema' is NULL.
 * cmdbuf: SQL stmt to send to every storage shards.
 * */
static void RemoteDatabaseDDL(const char *db, const char *schema, char *cmdbuf,
	size_t retlen, bool iscreate)
{
	struct timeval tv;

	if (!enable_remote_ddl()) return;
	check_ddl_txn_status();
	/*
	 * At start of the DDL execution, let's roughly check whether we have
	 * conflicting log entries to be executed, if so, fail.
	 * */
	if (check_ddl_op_conflicts_rough(get_metadata_cluster_conn(false), db,
		  schema == NULL ? DDL_ObjType_db : DDL_ObjType_schema))
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("Kunlun-db: Pending conflicting DDL operation to execute before this DDL statement can be executed.")));
	gettimeofday(&tv, NULL);
	const char *xa_txnid = MakeTopTxnName(tv.tv_usec, tv.tv_sec);
	g_remote_ddl_ctx.metadata_xa_txnid = xa_txnid;

	resetStringInfo(&last_remote_ddl_sql);
	appendBinaryStringInfo(&last_remote_ddl_sql, cmdbuf, retlen);

	char *pdbname = strcasestr(g_remote_ddl_ctx.orig_sql, db);
	const size_t dbnamelen = strlen(db);

	/*
	 * Remove the "" around db name if any, otherwise the SQL sent to metadata
	 * cluster is invalid.
	 * */
	if (pdbname != NULL && pdbname > g_remote_ddl_ctx.orig_sql &&
		(pdbname[-1] == '"' || pdbname[dbnamelen] == '"'))
	{
		Assert(pdbname[-1] == '"' && pdbname[dbnamelen] == '"');
		pdbname[-1] = ' ';
		pdbname[dbnamelen] = ' ';
	}

	/*
	 * Step 1: log statement in metadata cluster.
	 * create database and create schema can't specify qualified name, and
	 * there are many options specifiable, thus we always use the original
	 * SQL from client, never construct from pieces.
	 *
	 * The replay thread must connect to the right database in order to replay
	 * the 'create schema' stmt correctly. Use schema name 'public' means
	 * creating a db, otherwise creating a schema.
	 */
	DDL_ObjTypes objtype = schema == NULL ? DDL_ObjType_db : DDL_ObjType_schema;
	uint64_t logid = log_ddl_op(get_metadata_cluster_conn(false), xa_txnid,
		db, schema, current_role(), current_authorization(), db, objtype,
		iscreate ? DDL_OP_Type_create : DDL_OP_Type_drop,
		g_remote_ddl_ctx.orig_sql,
		cmdbuf, 0);
	g_remote_ddl_ctx.ddl_log_op_id = logid;
	/*
	 * Step 2: Send DDL statements to all storage nodes.
	 * If there are sequences in the dropped db/schema, sql stmts to drop the
	 * sequences are already accumulated in g_remote_ddl_ctx already, we need to
	 * send them after sending the 'drop db' sql in cmdbuf.
	 * Since storage shards don't support executing ddl in an explicit txn,
	 * the DDL will be executed as independent txns in each storage shard, and
	 * the DML stmts (i.e. delete sequence rows) sent to target shards will
	 * be executed in a global txn. and the changes to metadata is already
	 * executed in yet another independent txn. This isn't worth any fix although
	 * perfect solution would be to execute all 3 parts in one global txn ---
	 * mysql ddl are only atomic but can't be in an explicit txn so we can't
	 * do this and puting 2 of the 3 parts in a global txn doesn't make the
	 * work flow more robust than what's done here now.
	 * This situation is true also for creating/dropping sequences.
	 * */
	send_stmt_to_all_shards(cmdbuf, retlen, CMD_DDL, false,
		iscreate ? SQLCOM_CREATE_DB : SQLCOM_DROP_DB);

	/*
	  Clean up the states(did_ddl) from all asis otherwise txn commit will
	  fail because ddl executed in a global txn.
	*/
	int num_asis = GetAsyncStmtInfoUsed();
	for (int i = 0; i < num_asis; i++)
	{
		AsyncStmtInfo *asi = GetAsyncStmtInfoByIndex(i);
		ResetASIInternal(asi);
	}
#if 0
	stmts like drop table can also be tracked into g_remote_ddl_ctx but we
	can simply ignore them here because mysql always drops a database cascade
	so all tables in it are dropped.
	we only need to drop sequence rows of the target schema from
	mysql.sequences.

	int num_appended = 0;
	for (Remote_shard_ddl_context *psdc = &g_remote_ddl_ctx.shards; psdc; psdc = psdc->next)
	{
		if (psdc->remote_ddl.len == 0) continue;
		Assert(psdc->target_shard_id != InvalidOid);
		AsyncStmtInfo *asi = GetAsyncStmtInfo(psdc->target_shard_id);
		StringInfo str = &psdc->remote_ddl;
		/*
		  So far these are always sequences. If there will be other types in
		  future, we need to distinguish this in Remote_shard_ddl_context.
		*/
		append_async_stmt(asi, str->data, str->len, CMD_DELETE, false, SQLCOM_DELETE);
		num_appended++;
	}

	if (num_appended)
	{
		Assert(schema && !iscreate);
		send_multi_stmts_to_multi();
	}
#endif
	if (!iscreate)
	{
		/*
		  drop sequences in such databases from all storage shards.
		*/
		static char sqlbuf[256];
		int slen = snprintf(sqlbuf, sizeof(sqlbuf),
			"delete from mysql.sequences where db %s '%s_%s_%s'",
			(!schema) ? "like" : "=",
			db, use_mysql_native_seq ? "@0024@0024" : "$$",
			schema ? schema : "%");

		Assert(slen < sizeof(sqlbuf));
		send_stmt_to_all_shards(sqlbuf, slen, CMD_DELETE, false, SQLCOM_DELETE);
	}

	/*
	 * Step 3: update local ddl operation log id.
	 * */
	update_my_max_ddl_op_id(logid, objtype == DDL_ObjType_db);
	// prevent end_remote_ddl_stmt() from handling this again.
	g_remote_ddl_ctx.ddl_sql_src.len = 0;
	g_remote_ddl_ctx.shards.remote_ddl.len = 0;
}

static void scan_all_schemas_drop(const char *db, StringInfo stmt)
{
	Relation	pg_schemas;
	SysScanDesc scan;
	HeapTuple	nstuple;

	/*
	 * There's no syscache for pg_database indexed by name, so we must look
	 * the hard way.
	 */
	pg_schemas = heap_open(NamespaceRelationId, AccessShareLock);
	scan = systable_beginscan(pg_schemas, InvalidOid, false,
							  NULL, 0, NULL);

	// skip system nammespaces(schemas): pg_catalog, information_schema pg_toast%, pg_temp%
	while (HeapTupleIsValid(nstuple = systable_getnext(scan)))
	{
		Form_pg_namespace pgns = (Form_pg_namespace)GETSTRUCT(nstuple);
		if (strcmp(pgns->nspname.data, "pg_catalog") == 0 ||
			strcmp(pgns->nspname.data, "information_schema") == 0 ||
			strncmp(pgns->nspname.data, "pg_toast", 8) == 0 ||
			strncmp(pgns->nspname.data, "pg_temp", 7) == 0)
			continue;
		appendStringInfo(stmt, "drop database if exists %s_$$_%s;", db, pgns->nspname.data);
	}

	systable_endscan(scan);
	heap_close(pg_schemas, AccessShareLock);

	return;
}

void RemoteDropDatabase(const char *db)
{
	if (!enable_remote_ddl()) return;
	check_ddl_txn_status();
	StringInfoData cmd;
	initStringInfo2(&cmd, 512, CurTransactionContext);
	scan_all_schemas_drop(db, &cmd);
	int cmdlen = cmd.len;
	RemoteDatabaseDDL(db, NULL, donateStringInfo(&cmd), cmdlen, false);
}

void RemoteDropSchema(const char *schema)
{
	static NameData dbname;
	static char cmdbuf[192];

	if (!enable_remote_ddl()) return;
	check_ddl_txn_status();
	get_database_name3(MyDatabaseId, &dbname);

	int ret = snprintf(cmdbuf, sizeof(cmdbuf), "drop database if exists %s_$$_%s",
					   dbname.data, schema);
	RemoteDatabaseDDL(dbname.data, schema, cmdbuf, ret, false);
}

void RemoteCreateDatabase(const char *dbname)
{
	static char cmdbuf[128];
	if (!enable_remote_ddl()) return;
	check_ddl_txn_status();
	// TODO add encoding, collation and locale options to the remote stmt. Do these work in a seperate function.
	int retlen = snprintf(cmdbuf, sizeof(cmdbuf), "CREATE DATABASE %s_$$_public", dbname);
	if (retlen >= sizeof(cmdbuf))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: Command buffer insufficient when making 'create database' statement for db %s_$$_public.", dbname)));
	RemoteDatabaseDDL(dbname, NULL, cmdbuf, retlen, true);
}

/*
  Create a standalone sequence.
  If it's created associated with a table column, do not send the
  'create seq' stmt to peers but only accumulate 'create seq' stmts targeted to
  storage shards. and only send to peers the 'create table' instead later.
  there can be many sequence objects bound to a table's columns, but simply store
  all of them on the same shard --- where the table is stored, so that the
  'create table' stmt's 'with shard=N' tells also where its sequences are stored.

  if creating a partitioned table, the sequences should be created on storage
  shards, although no tables are created on storage shards. and need to let its
  leaf tables know about the sequences(this probably already working).
  and to tell peers about which shard stores all the table's sequences, also
  add a 'with shard=N' clause to such a 'create table' stmt.
*/
const static int64_t InvalidSeqVal = -9223372036854775808L;
void RemoteCreateSeqStmt(Relation rel, Form_pg_sequence seqform,
		CreateSeqStmt *seq, List*owned_by, bool toplevel)
{
	if (!enable_remote_ddl() || remote_skip_rel(rel)) return;

	check_ddl_txn_status();
	StringInfoData stmt;
	initStringInfo2(&stmt, 256, CurTransactionContext);
	StringInfoData stmt1;
	initStringInfo2(&stmt1, 256, CurTransactionContext);

	/*
	  Fetch schema-name and type name to form qualified sql string for other
	  computing nodes to execute.
	*/
	NameData dbname, schemaName, typName;
	get_database_name3(MyDatabaseId, &dbname);
	get_namespace_name3(rel->rd_rel->relnamespace, &schemaName);

	char seqcache[32];
	if (seqform->seqcache <= 1)
	{
		seqform->seqcache = 1;
		snprintf(seqcache, sizeof(seqcache), "NOCACHE");
	}
	else
		snprintf(seqcache, sizeof(seqcache), "CACHE %ld", seqform->seqcache);

	/*
	  seqstart = 1, seqincrement = 1, seqmax = 9223372036854775807, seqmin = 1,
	  seqcache = 1, seqcycle = false

	  Make sql for storage shard to execute.
	*/
	if (use_mysql_native_seq)
	{
		appendStringInfo(&stmt, "create sequence %s_$$_%s.%s increment by %ld start with %ld maxvalue %ld minvalue %ld %s %s ",
			dbname.data, schemaName.data, rel->rd_rel->relname.data,
			seqform->seqincrement, seqform->seqstart,
			seqform->seqmax, seqform->seqmin, seqform->seqcycle ? "cycle":"nocycle",
			seqcache);
	}
	else
	{
		appendStringInfo(&stmt, "insert into mysql.sequences(db, name, curval, start, step, max_value, min_value, do_cycle, n_cache) values('%s_$$_%s', '%s', %ld, %ld, %ld, %ld, %ld, %d, %ld) ",
			dbname.data, schemaName.data, rel->rd_rel->relname.data,
			InvalidSeqVal, seqform->seqstart, seqform->seqincrement,
			seqform->seqmax, seqform->seqmin, seqform->seqcycle ? 1 : 0,
			seqform->seqcache);
	}

	if (!toplevel)
	{
		/*
		  If creating a seq as part of creating a table, we only need to
		  accumulate the stmt here in order to send along with other
		  'create table' stmts to storage shards, and no peer stmt needed,
		  simply let other
		  computing nodes create its own seqs and tables locally, except to
		  assocate to the same seq/table on storage shard.
		*/
		Remote_shard_ddl_context *ddl_cxt = GetShardDDLCtx(rel->rd_rel->relshardid);
		StringInfo str = &ddl_cxt->remote_ddl;
		if (str->len > 0)
			appendStringInfoChar(str, ';');
		appendBinaryStringInfo(str, stmt.data, stmt.len);
		return;
	}

	SetRemoteDDLInfo(rel, DDL_OP_Type_create, DDL_ObjType_seq);
	HeapTuple ctup = SearchSysCache1(TYPEOID, seqform->seqtypid);
	if (!HeapTupleIsValid(ctup))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Cache lookup in kunlun-db failed for pg_type by typeid %u", seqform->seqtypid)));
	}

	Form_pg_type typ = (Form_pg_type)GETSTRUCT(ctup);
	typName = typ->typname;

	ReleaseSysCache(ctup);

	/*
	NameData rolename;
	ctup = SearchSysCache1(AUTHOID, seq->ownerId);
	if (!HeapTupleIsValid(ctup))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Cache lookup failed for pg_type by typeid %u", seq->ownerId)));
	}

	Form_pg_authid role = (Form_pg_authid)GETSTRUCT(ctup);
	rolename = role->rolName;

	ReleaseSysCache(ctup);
	*/

	struct timeval tv;
	gettimeofday(&tv, NULL);
	const char *xa_txnid = MakeTopTxnName(tv.tv_usec, tv.tv_sec);
	g_remote_ddl_ctx.metadata_xa_txnid = xa_txnid;

	/*
	  process_owned_by() has already validated the input. OWNED BY clause of
	  CREATE SEQUENCE stmt specifies the table column to associate to.
	*/
	char owned_by_clause[160] = {'\0'};
	if (owned_by && linitial(owned_by) && strVal(linitial(owned_by)) &&
		strcasecmp(strVal(linitial(owned_by)), "none") != 0)
	{
		const char *relname = strVal(linitial(owned_by));
		const char *attrname = strVal(lfirst(list_tail(owned_by)));
		snprintf(owned_by_clause, sizeof(owned_by_clause),
				 "owned by %s.%s", relname, attrname);
	}

	appendStringInfo(&stmt1, "create sequence %s.%s.%s as %s increment by %ld minvalue %ld maxvalue %ld start with %ld %s cycle cache %ld %s shard %u",
		dbname.data, schemaName.data, rel->rd_rel->relname.data, typName.data, seqform->seqincrement,
		seqform->seqmin, seqform->seqmax, seqform->seqstart, seqform->seqcycle ? "":"no",
		seqform->seqcache, owned_by_clause, rel->rd_rel->relshardid);


	uint64_t logid = log_ddl_op(get_metadata_cluster_conn(false), xa_txnid,
		dbname.data, schemaName.data, current_role(), current_authorization(),
		rel->rd_rel->relname.data, DDL_ObjType_seq,
		DDL_OP_Type_create, stmt1.data, stmt.data, rel->rd_rel->relshardid);

	g_remote_ddl_ctx.ddl_log_op_id = logid;

	/*
	 * Step 2: Send DDL statement to target storage shard.
	 * */
	AsyncStmtInfo *asi = GetAsyncStmtInfo(rel->rd_rel->relshardid);
	append_async_stmt(asi, stmt.data, stmt.len, CMD_DDL, false, SQLCOM_CREATE_SEQUENCE);
	send_multi_stmts_to_multi();

	/*
	 * Step 3: update local ddl operation log id.
	 * */
	update_my_max_ddl_op_id(logid, false);
}

static void TrackRemoteDropSequence(Relation rel)
{
	Remote_shard_ddl_context *ddl_cxt = GetShardDDLCtx(rel->rd_rel->relshardid);
	StringInfo str = &ddl_cxt->remote_ddl;
	char delim = ';';
	if (g_remote_ddl_ctx.db_name.data[0] == '\0')
	{
		SetRemoteDDLInfo(rel, DDL_OP_Type_drop, DDL_ObjType_seq);
		appendStringInfoString(&g_remote_ddl_ctx.ddl_sql_src, g_remote_ddl_ctx.orig_sql);
	}

	if (lengthStringInfo(str) == 0) delim = ' ';
	if (use_mysql_native_seq)
	{
		appendStringInfo(str, "%c drop sequence if exists %s", delim,
			    make_qualified_name(rel->rd_rel->relnamespace,
				                    rel->rd_rel->relname.data, NULL));
	}
	else
	{
		static NameData dbname, nspname;
		get_database_name3(MyDatabaseId, &dbname);
		get_namespace_name3(rel->rd_rel->relnamespace, &nspname);
		appendStringInfo(str, "%c delete from mysql.sequences where db='%s_$$_%s' and name='%s'", delim,
			    dbname.data, nspname.data, rel->rd_rel->relname.data);
	}
}

void TrackAlterSeq(Relation rel, List *owned_by, RemoteAlterSeq*raseq, bool toplevel, bool setval)
{
	NameData dbname, schemaName;
	char astyp[80] = {'\0'};
	bool changing_identity = false;
	struct timeval tv;
	const char *xa_txnid = NULL;

	if (raseq->update_stmt.len == 0 || raseq->update_stmt_peer.len == 0)
	{
		Assert(raseq->update_stmt.len == 0 && raseq->update_stmt_peer.len == 0);
		// owned_by could be modified internally
		if (raseq->newtypid == InvalidOid && !raseq->do_restart && !owned_by)
			changing_identity = true;
	}

	get_database_name3(MyDatabaseId, &dbname);
	get_namespace_name3(rel->rd_rel->relnamespace, &schemaName);
	const char *delim = use_mysql_native_seq ? "@0024@0024" : "$$";

	StringInfoData stmt, stmt1;
	initStringInfo2(&stmt, 256, CurTransactionContext);
	initStringInfo2(&stmt1, 256, CurTransactionContext);

	if (raseq->update_stmt.len > 0)
		appendStringInfo(&stmt,
			"update mysql.sequences set %s where db='%s_%s_%s' and name='%s'",
				raseq->update_stmt.data,
				dbname.data, delim, schemaName.data,
				rel->rd_rel->relname.data);
	toplevel = (toplevel && g_root_stmt->top_stmt_tag == T_AlterSeqStmt);
	if (!toplevel)
	{
		if (stmt.len == 0)
		{
			if (changing_identity)
			{
				appendStringInfo(&stmt1, "alter sequence %s.%s.%s set generated %s",
					dbname.data, schemaName.data, rel->rd_rel->relname.data,
					raseq->for_identity ? "always" : "by default");
			}
			goto end;
		}

		if (setval)
		{
			/*
			  Executing select setval() function, no ddl will be executed.
			*/
			AsyncStmtInfo *asi = GetAsyncStmtInfo(rel->rd_rel->relshardid);
			append_async_stmt(asi, stmt.data, stmt.len, CMD_DDL, false,
				SQLCOM_ALTER_TABLE);
			send_multi_stmts_to_multi();
			InvalidateCachedSeq(rel->rd_id);
		}
		else
		{
			/*
			  If altering a seq owned by a table, we only need to
			  accumulate the stmt here in order to send along with other
			  'alter table' stmts to storage shards, and no peer stmt needed,
			*/
	    	Remote_shard_ddl_context *ddl_cxt = GetShardDDLCtx(rel->rd_rel->relshardid);
	    	StringInfo str = &ddl_cxt->remote_ddl;
			appendStringInfo(str, "%c %*s", (str->len > 0 ? ';':' '), stmt.len, stmt.data);
		}
		goto end;
	}
	Assert(!setval);
	SetRemoteDDLInfo(rel, DDL_OP_Type_alter, DDL_ObjType_seq);

	if (raseq->newtypid != InvalidOid)
	{
	    HeapTuple ctup = SearchSysCache1(TYPEOID, raseq->newtypid);
	    if (!HeapTupleIsValid(ctup))
	    {
	        ereport(ERROR,
	                (errcode(ERRCODE_INTERNAL_ERROR),
	                 errmsg("Kunlun-db: Cache lookup failed for pg_type by typeid %u", raseq->newtypid)));
	    }
	
	    Form_pg_type typ = (Form_pg_type)GETSTRUCT(ctup);
		snprintf(astyp, sizeof(astyp), "as %s", typ->typname.data);
	    ReleaseSysCache(ctup);
	}

	/*
	  process_owned_by() has already validated the input. OWNED BY clause of
	  CREATE SEQUENCE stmt specifies the table column to associate to.
	*/
	{
	char owned_by_clause[160] = {'\0'};
	if (owned_by && linitial(owned_by) && strVal(linitial(owned_by)) &&
		strcasecmp(strVal(linitial(owned_by)), "none") != 0)
	{
		const char *relname = strVal(linitial(owned_by));
		const char *attrname = strVal(lfirst(list_tail(owned_by)));
		snprintf(owned_by_clause, sizeof(owned_by_clause),
				 "owned by %s.%s", relname, attrname);
	}

	appendStringInfo(&stmt1, "alter sequence %s.%s.%s %s %s %s",
		dbname.data, schemaName.data, rel->rd_rel->relname.data, astyp,
		raseq->update_stmt_peer.data, owned_by_clause);
	}

	gettimeofday(&tv, NULL);
	xa_txnid = MakeTopTxnName(tv.tv_usec, tv.tv_sec);
	g_remote_ddl_ctx.metadata_xa_txnid = xa_txnid;

	uint64_t logid = log_ddl_op(get_metadata_cluster_conn(false), xa_txnid,
		dbname.data, schemaName.data, current_role(), current_authorization(), 
		rel->rd_rel->relname.data, DDL_ObjType_seq,
		DDL_OP_Type_alter, stmt1.data, stmt.data, rel->rd_rel->relshardid);

	g_remote_ddl_ctx.ddl_log_op_id = logid;

	/*
	 * Step 2: Send DDL statement to target storage shard.
	 * */
	if (stmt.len > 0)
	{
		AsyncStmtInfo *asi = GetAsyncStmtInfo(rel->rd_rel->relshardid);
		append_async_stmt(asi, stmt.data, stmt.len, CMD_DDL, false,
			SQLCOM_ALTER_TABLE);
		send_multi_stmts_to_multi();
	}

	/*
	 * Step 3: update local ddl operation log id.
	 * */
	update_my_max_ddl_op_id(logid, false);

	/*if (raseq->do_restart)
		UpdateSeqLastFetched(rel->rd_id, raseq->restart_val);
	*/
end:
	InvalidateCachedSeq(rel->rd_id);
}

bool findSequenceByName(const char *seqname)
{
	Oid relid = RelnameGetRelid(seqname);
	if (relid == InvalidOid)
		return false;

	HeapTuple htup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(htup))
		elog(ERROR, "cache lookup failed for relation %u", relid);
	Form_pg_class pg_class_tuple = (Form_pg_class) GETSTRUCT(htup);
	return pg_class_tuple->relkind == RELKIND_SEQUENCE;
}


/*
  Store retrieved&cached value into FormData_pg_sequence_data::last_value
  Use SEQRELID(see ResetSequence() for seq cache access) to get the cached seq entry.
*/
//==============================================================================

RemoteMetaIPCSync *g_remote_meta_sync = NULL;

Size MetaSyncShmemSize()
{
	return sizeof(RemoteMetaIPCSync);
}

void CreateMetaSyncShmem()
{
	bool found = false;
	Size size = MetaSyncShmemSize();
	g_remote_meta_sync = (RemoteMetaIPCSync *)ShmemInitStruct("Remote Meta data sync", size, &found);
	if (!found)
	{
		/*
		 * We're the first - initialize.
		 */
		MemSet(g_remote_meta_sync, 0, size);
	}
}

bool IsCurrentProcMainApplier()
{
	return getpid() == g_remote_meta_sync->main_applier_pid;
}

void WaitForDBApplierExit(Oid dbid)
{
	SyncSlot *dbslots = g_remote_meta_sync->dbslots;
	while (true)
	{
		CHECK_FOR_INTERRUPTS();
		LWLockAcquire(RemoteMetaSyncLock, LW_EXCLUSIVE);
		/*
		only wait if the main applier is running and current process isn't the main applier.
		the main applier will drop db if it receives such a log, but it should not wait here
		otherwise it will self-lock. This implies that postgres db should never be dropped.
		*/
		bool do_wait = g_remote_meta_sync->main_applier_pid > 0 &&
			getpid() != g_remote_meta_sync->main_applier_pid;
		if (!do_wait) {
			LWLockRelease(RemoteMetaSyncLock);
			return;
		}

		/*
		  find a free slot to store the dbid,MyProc pair.
		*/
		for (int i = 0; i < MAX_DBS_ALLOWED; i++)
		{
			SyncSlot *slot = dbslots + i;
			if (slot->proc || slot->dbid != InvalidOid) {
				Assert(slot->proc && slot->dbid != InvalidOid);
				continue;
			}
			slot->proc = MyProc;
			slot->dbid = dbid;

			LWLockRelease(RemoteMetaSyncLock);
			if (MyProc->last_sem_wait_timedout)
			{
				PGSemaphoreReset(MyProc->sem);
				MyProc->last_sem_wait_timedout = false;
			}
			/*
			  Without a timeout, current process could be waiting forever
			  if the db's applier isn't running now.
			  This is done at start of 'drop db' stmt, so simply abort the txn
			  and data/metadata is totally consistent.
			*/
			int lret = PGSemaphoreTimedLock(MyProc->sem, StatementTimeout);
			Assert(lret == 0 || lret == 1);
			if (lret == 1)
			{
				MyProc->last_sem_wait_timedout = true;
				ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
						errmsg("Kunlun-db: Timeout waiting for database (%u, %s)'s applier process to exit.",
						dbid, get_database_name(dbid))));
			}
			 
			return;
		}

		LWLockRelease(RemoteMetaSyncLock);
		elog(INFO, "WaitForDBApplierExit: Waiting for free slot for DDL stmts against database (%u, %s).",
			 dbid, get_database_name(dbid));
		/*
		  if no free slots, wait 10ms and retry.
		*/
		usleep(10000);
	}
}


/*
  If multiple 'drop db' stmts dropping the same db, there will be multiple
  slots in g_remote_meta_sync->dbslots for the db.
*/
static void NotifyDBApplierGone(int idx, Oid dbid)
{
	// caller has the lock.
	//LWLockAcquire(RemoteMetaSyncLock, LW_EXCLUSIVE);
	SyncSlot *dbslots = g_remote_meta_sync->dbslots;
	for (int i = idx; i < MAX_DBS_ALLOWED; i++)
	{
		SyncSlot *slot = dbslots + i;
		if (slot->dbid == dbid)
		{
			PGSemaphoreUnlock(slot->proc->sem);
			slot->dbid = InvalidOid;
			slot->proc = NULL;
			// keep searching, there can be more waiters for the db.
		}
	}
	//LWLockRelease(RemoteMetaSyncLock);
}

/*
  @retval >=0: the NO. of unique dbs handled, note that real NO. of 'drop db' stmts waiting
  can be more since multiple 'drop db' stmts can be issued at the same time.
  -1: invalid dbid
  -2: postmaster died, so caller should exit immediately.
*/
int handle_remote_meta_sync_reqs(on_remote_meta_sync_func_t func)
{
	LWLockAcquire(RemoteMetaSyncLock, LW_EXCLUSIVE);
	SyncSlot *dbslots = g_remote_meta_sync->dbslots;
	int cnt = 0;

	for (int i = 0; i < MAX_DBS_ALLOWED; i++) {
		SyncSlot *slot = dbslots + i;
		if (slot->dbid != InvalidOid)
		{
			int ret = func(slot->dbid);
			if (ret < 0)
				return ret;
			Assert(ret == 0);
			cnt++;
			NotifyDBApplierGone(i, slot->dbid);
		}
	}
	LWLockRelease(RemoteMetaSyncLock);
	return cnt;
}

int wakeup_remote_meta_sync_waiters(Oid dbid)
{
	LWLockAcquire(RemoteMetaSyncLock, LW_EXCLUSIVE);
	SyncSlot *dbslots = g_remote_meta_sync->dbslots;
	int cnt = 0;

	for (int i = 0; i < MAX_DBS_ALLOWED; i++) {
		SyncSlot *slot = dbslots + i;
		if (slot->dbid == dbid)
		{
			cnt++;
			NotifyDBApplierGone(i, dbid);
		}
	}
	LWLockRelease(RemoteMetaSyncLock);
	return cnt;
}

/*
  These ddl stmts don't need storage shards actions, simply append them to
  ddl log for other computing nodes to replicate and execute.
*/
bool is_supported_simple_ddl_stmt(NodeTag stmt)
{
	static NodeTag allowed_cmds[] = {
		T_DefineStmt,
		T_GrantRoleStmt,
		T_CreateRoleStmt,
		T_AlterRoleStmt,
		T_AlterRoleSetStmt,
		T_DropRoleStmt,

		T_ReassignOwnedStmt,
		T_CreateExtensionStmt,
		T_AlterExtensionStmt,
		T_AlterExtensionContentsStmt,
		T_CreateFdwStmt,
		T_AlterFdwStmt,
		T_CreateForeignServerStmt,
		T_AlterForeignServerStmt,
		T_CreateUserMappingStmt,
		T_AlterUserMappingStmt,
		T_DropUserMappingStmt,
		T_ImportForeignSchemaStmt,

		T_CompositeTypeStmt,
		T_CreateEnumStmt, /* CREATE TYPE AS ENUM */
		T_CreateRangeStmt, /* CREATE TYPE AS RANGE */
		T_ViewStmt,    /* CREATE VIEW */
		T_CreateFunctionStmt,  /* CREATE FUNCTION */
		T_AlterFunctionStmt,   /* ALTER FUNCTION */
		T_RefreshMatViewStmt,
		T_CreatePLangStmt,
		T_CreateConversionStmt,
		T_CreateCastStmt,
		T_CreateOpClassStmt,
		T_CreateOpFamilyStmt,
		T_CreateTransformStmt,
		T_AlterOpFamilyStmt,
		T_AlterTSDictionaryStmt,
		T_AlterTSConfigurationStmt,
		T_RenameStmt,
		T_AlterObjectDependsStmt,
		T_AlterOwnerStmt,
		T_AlterOperatorStmt,
		T_CommentStmt,
		T_GrantStmt,
		T_AlterObjectSchemaStmt,
		T_AlterDefaultPrivilegesStmt,
		/*
		  Policies can't be supported for update stmts, alghouth they can be
		  supported for insert stmts, we have to disable them.
		*/
		//T_CreatePolicyStmt,
		//T_AlterPolicyStmt,
		T_SecLabelStmt,
		T_CreateAmStmt,
		T_AlterCollationStmt,
		T_AlterSystemStmt,
		T_LoadStmt,
		/*
		  forbidden parts are banned in impl.
		*/
		T_AlterDatabaseStmt,
		T_AlterDatabaseSetStmt,
		/*
		  This is handled in RemoveObjects() because for all kinds of relations,
		  we need to do actions for storage shards, it's not enough to simply
		  replicate the sql string for them; for others, it's sufficient to do so.
		T_DropStmt
		*/
	}
	;

	for (int i = 0; i < sizeof(allowed_cmds)/sizeof(NodeTag); i++)
		if (stmt == allowed_cmds[i])
			return true;
	return false;
}

/*
  These DDL stmts are not supported, they should be banned.
  Apart from the banned stmts and the is_simple_supported_ddl_stmt() stmts,
  all other stmts belong to 2 categories:
  1. support for them are implemented: create/drop/alter table/index/sequence/schema/db
  2. they involve a single session and no extra work need to be done for them.
  LISTEN/UNLISTEN/NOTIFY/DO/CALL/FETCH/DECLARE CURSOR/CLOSE/COPY/PREPARE/EXECUTE/DEALLOCATE/
  EXPLAIN/SET/SHOW/LOCK/SET CONSTRAINTS/CHECKPOINT etc
*/
bool is_banned_ddl_stmt(NodeTag stmt)
{
	static NodeTag banned_cmds[] = {
		T_CreateTableSpaceStmt,
		T_DropTableSpaceStmt,
		T_AlterTableSpaceOptionsStmt,
		T_ClusterStmt,
		T_CreateTrigStmt,
		T_AlterTableMoveAllStmt,
		T_CreatePublicationStmt,
		T_AlterPublicationStmt,
		T_CreateSubscriptionStmt,
		T_AlterSubscriptionStmt,
		T_DropSubscriptionStmt,
		T_CreateEventTrigStmt,
		T_AlterEventTrigStmt,
		T_VacuumStmt,
		T_ReindexStmt,
		/*
			TODO: support below commands in future
			ANALYZE (it's done as a T_VacuumStmt)
			and below stmts:
		*/

		/*
		  rules can be created/dropped and they already work well but rules
		  are often pitfalls for users, according to
		  pg user comments rules are error prone. only enable it when it and
		  its implications are fully understood .
		*/
		T_RuleStmt,
		
		/*
		  Although 'create domain' currently can be executed and domains can
		  be used to create tables, if we don't allow 'alter domain',
		  we might piss someone off, so ban both for now.
		  In future, we really need to think twice whether domains are
		  really needed: despite its advantages claimed by pg's doc(see the
		  page for create domain), domains cause coupling of tables that use
		  them and over time users may need to alter a domain's definition
		  for some tables and inadvently impact other tables which don't expect
		  such changes, and such coupling could be a common source of errors.
		  So only support them on strong user needs.
		*/
		T_CreateDomainStmt,
		T_AlterDomainStmt,

		T_TruncateStmt, // may support in future but not now
		T_DropOwnedStmt, //DropOwnedObjects
		// T_CreateTableAsStmt,// also used for matview, which is allowed.
		// 'create table as' and 'select into' stmts are forbidden.
		T_CreateStatsStmt,

		/*
		  Policies can't be supported for update stmts, alghouth they can be
		  supported for insert stmts, we have to disable them.
		*/
		T_CreatePolicyStmt,
		T_AlterPolicyStmt
	};

	for (int i = 0; i < sizeof(banned_cmds)/sizeof(banned_cmds[0]); i++)
		if (stmt == banned_cmds[i])
			return true;
	return false;
}

bool is_object_stored_in_shards(ObjectType objtype)
{
	static ObjectType stored_in_shard_obj_types[] = {
		OBJECT_INDEX,
		OBJECT_TABLE,
		OBJECT_SEQUENCE,
		OBJECT_DATABASE,
		OBJECT_SCHEMA
	};

	for (int i = 0; i < sizeof(stored_in_shard_obj_types)/sizeof(stored_in_shard_obj_types[0]); i++)
		if (objtype == stored_in_shard_obj_types[i])
			return true;
	return false;
}

void accumulate_simple_ddl_sql(NodeTag tag, const char *sql, int start, int len)
{
	Assert(len >= 0);
	char delim = g_remote_ddl_ctx.ddl_sql_src.len > 0 ? ';':' ';

	if (len > 0)
		appendStringInfo(&g_remote_ddl_ctx.ddl_sql_src, "%c %*s", delim,
			len, sql + start);
	else
		appendStringInfo(&g_remote_ddl_ctx.ddl_sql_src, "%c %s", delim,
			sql + start);

	elog(DEBUG2, "g_remote_ddl_ctx.ddl_sql_src:%s",
		 g_remote_ddl_ctx.ddl_sql_src.data);

	if (g_remote_ddl_ctx.db_name.data[0] == '\0')
	{
		get_database_name3(MyDatabaseId, &g_remote_ddl_ctx.db_name);
		g_root_stmt->optype = DDL_OP_Type_generic;
		g_root_stmt->objtype = DDL_ObjType_generic;
		/* Mark privilege related statement */
		switch (tag) {
			case T_GrantStmt:
			case T_GrantRoleStmt:
			case T_AlterDefaultPrivilegesStmt:
			case T_CreateRoleStmt:
			case T_AlterRoleStmt:
			case T_DropRoleStmt:
			case T_AlterRoleSetStmt:
			case T_ReassignOwnedStmt:
				{
					g_root_stmt->objtype = DDL_ObjType_user;
					break;
				}
			default: break;
		}

	}
	/* other fields of g_remote_ddl_ctx are irrelevant. */
}


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

bool is_supported_alter_table_subcmd(AlterTableCmd *subcmd)
{
	const static AlterTableType banned_alcmds[] = {
		AT_AttachPartition,

		AT_SetStatistics,
		AT_SetStorage,

		/*
		  If we allow adding/changing constraints to existing table, we would
		  need to verify them against existing rows by either pushing
		  constraints down(issue: mysql may not have needed
		  functions/functionality required by such constraints) to storage
		  shards or pulling rows up to computing nodes(issue: hurts system
		  performance). So we will not support constraint changes for now.
		  Probably we can do below stmt to validate new constraints:
		  'select exists(select * from target_table where NOT (CHECK-CONSTRAINT-EXPRESSION))'
		  if it's true the new contraints doesn't pass.
		  this requires mysql support for full constraint expression, or
		  push down supported portions and check remaining in computing node.

		  In performance perspective, such constraint validation could be
		  expensive so probably we should leave them to client software.
		*/
		AT_AddConstraint,
		AT_AlterConstraint,
		AT_ValidateConstraint,
		AT_AddIndexConstraint,

		/*
		  This could be supported in future because mysql allows switching
		  a table's storage engine.
		*/
		AT_SetLogged,               /* SET LOGGED */
		AT_SetUnLogged,             /* SET UNLOGGED */

		AT_ClusterOn,               /* CLUSTER ON */
		AT_DropCluster,             /* SET WITHOUT CLUSTER */
		AT_AddOids,
		AT_SetTableSpace,           /* SET TABLESPACE */

		/*
		  mysql doesn't allow changing table storage parameters. and pg's
		  original storage params are not used now.
		*/
		AT_SetRelOptions,           /* SET (...) -- AM specific parameters */
		AT_ResetRelOptions,         /* RESET (...) -- AM specific parameters */
		AT_ReplaceRelOptions,       /* replace reloption list in its entirety */

		/*
		  triggers and foreign keys will never be supported.
		*/
		AT_EnableTrig,              /* ENABLE TRIGGER name */
		AT_EnableAlwaysTrig,        /* ENABLE ALWAYS TRIGGER name */
		AT_EnableReplicaTrig,       /* ENABLE REPLICA TRIGGER name */
		AT_DisableTrig,             /* DISABLE TRIGGER name */
		AT_EnableTrigAll,           /* ENABLE TRIGGER ALL */
		AT_DisableTrigAll,          /* DISABLE TRIGGER ALL */
		AT_EnableTrigUser,          /* ENABLE TRIGGER USER */
		AT_DisableTrigUser,         /* DISABLE TRIGGER USER */

		AT_EnableReplicaRule,

		/*
		  table types, composite types and table inheritance will never be
		  supported.
		*/
		AT_AddInherit,              /* INHERIT parent */
		AT_DropInherit,             /* NO INHERIT parent */
		AT_AddOf,                   /* OF <type_name> */
		AT_DropOf,                  /* NOT OF */
		AT_ReplicaIdentity,         /* REPLICA IDENTITY */
		AT_GenericOptions,

		/*
		  Need to disable this feature because we don't support policy and we
		  can't enforce such rules for updates.
		*/
		AT_EnableRowSecurity,
		AT_DisableRowSecurity,
		AT_ForceRowSecurity,
		AT_NoForceRowSecurity
	};

	for (int i = 0; i < sizeof(banned_alcmds)/sizeof(banned_alcmds[0]); i++)
		if (subcmd->subtype == banned_alcmds[i])
			return false;
	return true;
}

/*
{

done:
AT_AddColumn,AT_DropColumn, AT_SetNotNull, AT_DropNotNull, AT_AlterColumnType,
AT_AddIndex

TODO:
AT_AddIdentity,
AT_SetIdentity,             // SET identity column options
AT_DropIdentity             // DROP IDENTITY
}
*/

/*
rename supported/banned (done)
{
involves storage shard actions: OBJECT_COLUMN, OBJECT_SEQUENCE,OBJECT_TABLE,
forbidden: INDEX, OBJECT_DATABASE, OBJECT_SCHEMA(already banned)
all others are supported already.
}

AlterTableNamespace can be executed, because in mysql, can move a table to another db(done)
*/

/*
  Add (id, name) into rsdc->altered_tables if not already in it.
  return whether it's been added in this call.
*/
static bool
AddAlteredTable(Remote_shard_ddl_context *rsdc, Oid id, const char *name)
{
	Object_ref *p;

	for (Object_ref *tr = &rsdc->altered_tables; tr; tr=tr->next)
	{
		p = tr;
		if (tr->id == id)
		{
			Assert(strcmp(name, tr->name.data) == 0);
			return false;
		}
	}

	if (p->id != InvalidOid) // *p is already used
	{
		p->next = (Object_ref *)MemoryContextAllocZero(
			TopTransactionContext, sizeof(Object_ref));
		p = p->next;
	}
	p->id = id;
	strncpy(p->name.data, name, sizeof(NameData) - 1);

	return true;
}

/*
  Whether current stmt altered some tables.
*/
static bool AlteredSomeTables()
{
	for (Remote_shard_ddl_context *rsdc = &g_remote_ddl_ctx.shards; rsdc;
		 rsdc = rsdc->next)
	{
		if (rsdc->altered_tables.id != InvalidOid)
			return true;
	}
	return false;
}

static AlterTableColumnAction*
FindChangedColumn(Remote_shard_ddl_context *rsdc, const char *colName)
{
	AlterTableColumnAction *csd, *p;
	for (csd = &rsdc->changed_cols; csd; csd = csd->next)
	{
		p = csd;
		if (strcmp(csd->name.data, colName) == 0)
			return csd;
	}

	if (p->name.data[0])// *p is already used
	{
		p->next = (AlterTableColumnAction *)MemoryContextAllocZero(
			TopTransactionContext, sizeof(AlterTableColumnAction));
		p = p->next;
	}
	strncpy(p->name.data, colName, sizeof(NameData)-1);
	return p;
}

static void build_change_column_stmt()
{
	Assert((g_root_stmt->top_stmt_tag == T_AlterTableStmt ||
			g_root_stmt->top_stmt_tag == T_AlterEnumStmt ||
		    g_root_stmt->top_stmt_tag == T_RenameStmt) &&
			g_root_stmt->optype == DDL_OP_Type_alter &&
			g_root_stmt->objtype == DDL_ObjType_table);

	StringInfoData stmts;
	initStringInfo2(&stmts, 256, TopTransactionContext);

	for (Remote_shard_ddl_context *rsdc = &g_remote_ddl_ctx.shards; rsdc;
		 rsdc = rsdc->next)
	{
		/*
		  There could be other stmts already formed but they rely on the initial
		  'alter table' stmts, so must generate and execute the alter table
		  stmts first. for example when
		  adding a unique column, the 'create unique index' stmt is already
		  generated when this func is called.
		*/
		resetStringInfo(&stmts);
		if (rsdc->remote_ddl.len > 0)
		{
			appendBinaryStringInfo(&stmts, rsdc->remote_ddl.data, rsdc->remote_ddl.len);
			resetStringInfo(&rsdc->remote_ddl);
		}

		for (Object_ref *tr = &rsdc->altered_tables; tr; tr=tr->next)
		{
			if (tr->id == InvalidOid)
			{
				Assert(tr == &rsdc->altered_tables);
				break;
			}

			Assert(tr->id != InvalidOid && tr->name.data[0] != '\0');

			/*
			  if multiple leaf tables of a partitioned table could be from
			  multiple schemas, then we'd need to store schema name in
			  Object_ref too. this seems unecessary now.
			  Probably 'attach partition' will produce such situations.
			*/
			appendStringInfo(&rsdc->remote_ddl, "%c alter table %s_$$_%s.%s ",
				rsdc->remote_ddl.len > 0 ? ';' : ' ',
				g_remote_ddl_ctx.db_name.data, g_root_stmt->schema_name.data,
				tr->name.data);

			int idx = 0;
			for (AlterTableColumnAction *csd = &rsdc->changed_cols;
				 csd; csd = csd->next)
			{
				if (csd->action == ATCA_MODIFY)
				{
					/*
					  Never send 'default' constraint to storage shard in
					  'alter column' subcmd, only need to do so for 'add column'
					  because only in this case do we need to change existing
					  rows' new field.
					*/
					Assert(csd->name.data[0] != '\0' && csd->col_dtype.data[0] != '\0');
					appendStringInfo(&rsdc->remote_ddl, "%c modify column %s %s %s ",
						idx == 0 ? ' ':',', csd->name.data,
						csd->col_dtype.data,
						csd->nullable ? "" : "not null");
				}
				else if (csd->action == ATCA_RENAME)
				{
					appendStringInfo(&rsdc->remote_ddl, "%c rename column %s to %s",
						idx == 0 ? ' ':',', csd->name.data, csd->newname.data);
				}
				else if (csd->action == ATCA_DROP)
				{
					appendStringInfo(&rsdc->remote_ddl, "%c drop column %s",
						idx == 0 ? ' ':',', csd->name.data);
				}
				else if (csd->action == ATCA_ADD)
				{
					Assert(csd->name.data[0] != '\0' && csd->col_dtype.data[0] != '\0');
					appendStringInfo(&rsdc->remote_ddl, "%c add column %s %s %s %s %s",
						idx == 0 ? ' ':',', csd->name.data,
						csd->col_dtype.data, csd->nullable ? "":"not null",
						csd->def_valstr.len > 0 ? csd->def_valstr.data:"",
						csd->unique ? "unique":"");
				}
				else
					Assert(false);
				idx++;
			}
		}

		if (stmts.len > 0)
		{
			if (rsdc->remote_ddl.len > 0)
				appendStringInfoChar(&rsdc->remote_ddl, ';');
			appendBinaryStringInfo(&rsdc->remote_ddl, stmts.data, stmts.len);
		}
	}
}

void TrackColumnNullability(Relation rel, const char *colName, Oid typid,
	bool nullable, int32 typmod, Oid collOid)
{
	if (!enable_remote_ddl() || rel->rd_rel->relshardid == InvalidOid) return;
	
	check_ddl_txn_status();
	Assert(rel->rd_rel->relkind == RELKIND_RELATION);
	Remote_shard_ddl_context *rsdc = GetShardDDLCtx(rel->rd_rel->relshardid);
	AddAlteredTable(rsdc, rel->rd_id, rel->rd_rel->relname.data);
	AlterTableColumnAction *csd = FindChangedColumn(rsdc, colName);
	fetch_column_data_type(csd, typid, typmod, collOid);
	csd->typid = typid;
	csd->nullable = nullable;
	csd->action = ATCA_MODIFY;
}

void TrackAlterColType(Relation rel, const char *colname, bool notnull,
	Oid targettype, int32 typmod, Oid collOid)
{
	if (!enable_remote_ddl() || rel->rd_rel->relshardid == InvalidOid) return;
	
	check_ddl_txn_status();
	Assert(rel->rd_rel->relkind == RELKIND_RELATION);
	Remote_shard_ddl_context *rsdc = GetShardDDLCtx(rel->rd_rel->relshardid);
	AddAlteredTable(rsdc, rel->rd_id, rel->rd_rel->relname.data);
	AlterTableColumnAction *csd = FindChangedColumn(rsdc, colname);
	fetch_column_data_type(csd, targettype, typmod, collOid);
	csd->typid = targettype;
	csd->nullable = !notnull;
	csd->action = ATCA_MODIFY;
}

void TrackDropColumn(Relation rel, const char *colName)
{
	if (!enable_remote_ddl() || rel->rd_rel->relshardid == InvalidOid) return;
	check_ddl_txn_status();
	Assert(rel->rd_rel->relkind == RELKIND_RELATION);
	Remote_shard_ddl_context *rsdc = GetShardDDLCtx(rel->rd_rel->relshardid);
	AddAlteredTable(rsdc, rel->rd_id, rel->rd_rel->relname.data);
	AlterTableColumnAction *csd = FindChangedColumn(rsdc, colName);
	csd->action = ATCA_DROP;
	//strncpy(csd->name.data, colName, sizeof(NameData)-1);
}

void TrackAddColumn(Relation rel, ColumnDef *coldef, char typtype,
	Oid typid, int32 typmod, Oid collOid)
{
	//TODO: make sure coldef->collClause always use utf8 collations.

	if (!enable_remote_ddl() || rel->rd_rel->relshardid == InvalidOid) return;
	check_ddl_txn_status();

	Assert(rel->rd_rel->relkind == RELKIND_RELATION);
	if (typtype != TYPTYPE_BASE  && typtype != TYPTYPE_DOMAIN &&
		typtype != TYPTYPE_ENUM)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("Kunlun-db: Can not add column of unsupported meta types, only basic types, domain types and enum types allowed.")));

	Remote_shard_ddl_context *rsdc = GetShardDDLCtx(rel->rd_rel->relshardid);
	AddAlteredTable(rsdc, rel->rd_id, rel->rd_rel->relname.data);
	AlterTableColumnAction *csd = FindChangedColumn(rsdc, coldef->colname);
	csd->action = ATCA_ADD;
	csd->nullable = !coldef->is_not_null;
	fetch_column_constraints(coldef, csd);
	fetch_column_data_type(csd, typid, typmod, collOid);
	csd->typid = typid;
	//strncpy(csd->name.data, coldef->colname, sizeof(NameData)-1);
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
  relation rename and schema move is always indepent 'alter table' stmt

ALTER TABLE/SEQUENCE [ IF EXISTS ] name
RENAME TO new_name
ALTER TABLE/SEQUENCE  [ IF EXISTS ] name
SET SCHEMA new_schema

ALTER INDEX old_ind RENAME TO new_ind

isrel: true if renaming relation, false if moving to another schema. they both
		can be handled by rename table stmt in mysql.
*/
void TrackRelationRename(Relation rel, const char*objname, bool isrel)
{
	if (!enable_remote_ddl() || rel->rd_rel->relshardid == InvalidOid) return;
	
	check_ddl_txn_status();
	Remote_shard_ddl_context *rsdc = GetShardDDLCtx(rel->rd_rel->relshardid);
	const char*dbname = g_remote_ddl_ctx.db_name.data;
	const char*schemaname = g_root_stmt->schema_name.data;
	const char *my_name = NULL;
	if (isrel)
		my_name = rel->rd_rel->relname.data;
	else
		my_name = schemaname;

	if (rel->rd_rel->relkind == RELKIND_RELATION)
	{
		SetRemoteDDLInfo(rel, DDL_OP_Type_rename, DDL_ObjType_table);
		if (strncmp(objname, my_name, NAMEDATALEN - 1) == 0) goto skip;
		if (isrel)
			appendStringInfo(&rsdc->remote_ddl, "%c alter table %s_$$_%s.%s rename to %s_$$_%s.%s",
				rsdc->remote_ddl.len > 0 ? ';':' ', dbname, schemaname,
				rel->rd_rel->relname.data, dbname, schemaname, objname);
		else
			appendStringInfo(&rsdc->remote_ddl, "%c rename table %s_$$_%s.%s to %s_$$_%s.%s",
				rsdc->remote_ddl.len > 0 ? ';':' ', dbname, schemaname,
				rel->rd_rel->relname.data, dbname, objname,
				rel->rd_rel->relname.data);
	}
	else if (rel->rd_rel->relkind == RELKIND_INDEX)
	{
		if (!isrel)
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("Kunlun-db: Can not move index to another db alone.")));
		if (!rel->rd_index || rel->rd_index->indisprimary)
			goto skip; // in mysql, a primary index is always named PRIMARY.

		SetRemoteDDLInfo(rel, DDL_OP_Type_rename, DDL_ObjType_index);
		if (strncmp(objname, my_name, NAMEDATALEN - 1) == 0) goto skip;
		/*
		  The same index in computing nodes and storage shards always share
		  the same name except PK so we can identify the target index in
		  storage shards as done here.
		*/
		Oid heapid = IndexGetRelation(rel->rd_id, false);
		Relation heaprel = heap_open(heapid, AccessShareLock);
		appendStringInfo(&rsdc->remote_ddl, "%c alter table %s_$$_%s.%s rename index %s to %s",
			rsdc->remote_ddl.len > 0 ? ';':' ', dbname, schemaname,
			heaprel->rd_rel->relname.data,
			rel->rd_rel->relname.data, objname);
		heap_close(heaprel, NoLock);
	}
	else if (rel->rd_rel->relkind == RELKIND_SEQUENCE)
	{
		SetRemoteDDLInfo(rel, DDL_OP_Type_rename, DDL_ObjType_seq);
		if (strncmp(objname, my_name, NAMEDATALEN - 1) == 0) goto skip;
		if (isrel)
			appendStringInfo(&rsdc->remote_ddl, "%c update mysql.sequences set name= '%s' where db='%s_%s_%s' and name='%s'",
				rsdc->remote_ddl.len > 0 ? ';':' ', objname,dbname,
				use_mysql_native_seq ? "@0024@0024" : "$$", schemaname,
				rel->rd_rel->relname.data);
		else
			appendStringInfo(&rsdc->remote_ddl, "%c update mysql.sequences set db = '%s_%s_%s' where db='%s_%s_%s' and name='%s'",
				rsdc->remote_ddl.len > 0 ? ';':' ', dbname,
				use_mysql_native_seq ? "@0024@0024" : "$$", objname, dbname,
				use_mysql_native_seq ? "@0024@0024" : "$$",
				schemaname, rel->rd_rel->relname.data);
	}
	else
	{
		// no need to involve storage shards, nothing to do.
	}
	return;
skip:
	ResetRemoteDDLStmt();
}

void TrackColumnRename(Relation rel, const char*oldname, const char*newname)
{
	if (!enable_remote_ddl() || rel->rd_rel->relshardid == InvalidOid) return;
	
	check_ddl_txn_status();
	Assert(rel->rd_rel->relkind == RELKIND_RELATION);
	/*
	  Renaming is internally not called by AlterTable() 
	*/
	SetRemoteDDLInfo(rel, DDL_OP_Type_alter, DDL_ObjType_table);
	Remote_shard_ddl_context *rsdc = GetShardDDLCtx(rel->rd_rel->relshardid);
	AddAlteredTable(rsdc, rel->rd_id, rel->rd_rel->relname.data);
	AlterTableColumnAction *csd = FindChangedColumn(rsdc, oldname);
	csd->action = ATCA_RENAME;
	strncpy(csd->newname.data, newname, sizeof(NameData)-1);
	strncpy(csd->name.data, oldname, sizeof(NameData)-1);
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

/*
  Find from column's constraint whether it's unique.
*/
static bool
fetch_column_constraints(ColumnDef *column, AlterTableColumnAction *csd)
{
	ListCell   *clist;
	foreach(clist, column->constraints)
	{    
		Constraint *constraint = lfirst_node(Constraint, clist);

		switch (constraint->contype)
		{
			case CONSTR_UNIQUE:
				csd->unique = true;
				break;
			case CONSTR_DEFAULT:
				break;
			case CONSTR_PRIMARY:
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Kunlun-db: Can not add new column(s) as primary key.")));
				break;
			default:
				break;
		}
	}

	if (column->raw_default || column->cooked_default)
	{
		if (!(csd->def_valstr.len == 0 && csd->def_valstr.data == NULL))
		{
			Assert(csd->def_valstr.len > 0 && csd->def_valstr.data != NULL);
			return false; // 2nd and more leaf table of a partitioned table.
		}

		initStringInfo2(&csd->def_valstr, 64, TopTransactionContext);

		/*
		  prefer transformed exprs. we may or may not be able to handle such
		  default exprs.
		*/
		if (column->cooked_default)
		{
			appendStringInfoString(&csd->def_valstr, "default ");
			int slen = snprint_expr(&csd->def_valstr, (Expr*)column->cooked_default, NULL);
			if (slen <= 0)
			{
				pfree(csd->def_valstr.data);
				csd->def_valstr.data = NULL;
				csd->def_valstr.len = 0;
			}
		}
		else
		{
			appendStringInfoString(&csd->def_valstr, "default ");
			bool done = PrintParserTree(&csd->def_valstr, column->raw_default);
			if (!done)
			{
				pfree(csd->def_valstr.data);
				csd->def_valstr.data = NULL;
				csd->def_valstr.len = 0;
			}
		}
	}

	return false;
}

/*
  always use original sql, other computing nodes will abandon unsupported
  pieces just like done here.
  concreate actions will be tracked by other functions.
*/
void TrackAlterTableGeneral(Oid relid)
{
	/*
	  AlterTable() could be called as part of another DDL stmt execution, and
	  in such a case it's not topmost and should not store the stmt again,
	  other parts will need to do that.
	*/
	if (g_root_stmt->top_stmt_tag != T_Invalid &&
		g_remote_ddl_ctx.ddl_sql_src.len > 0)
		return;

	appendStringInfoString(&g_remote_ddl_ctx.ddl_sql_src,
		g_remote_ddl_ctx.orig_sql);
	Relation rel = relation_open(relid, AccessExclusiveLock);
	/*
	  This could be called after some of 'alter table' substmts are executed.
	  So far an 'alter table' stmt is always top most.
	*/
	SetRemoteDDLInfo(rel, DDL_OP_Type_alter, DDL_ObjType_table);
	relation_close(rel, NoLock);
}


void TrackRenameGeneral(ObjectType objtype)
{
	if (!(objtype == OBJECT_TABLE || objtype == OBJECT_SEQUENCE))
		return;

	/*
	  'alter table/sequence rename to' is always a single and top level stmt,
	  and thus no need for below actions, they will be done in
	  TrackRelationRename().
	*/
	Assert(g_root_stmt->top_stmt_tag == T_RenameStmt &&
		   g_remote_ddl_ctx.ddl_sql_src.len == 0);

	/*
	g_root_stmt->top_stmt_tag = T_RenameStmt;
	g_root_stmt->optype = DDL_OP_Type_rename;
	if (objtype == OBJECT_SEQUENCE)
		g_root_stmt->objtype = DDL_ObjType_seq;
	else
		g_root_stmt->objtype = DDL_ObjType_table;

	this is already done in accumulate_simple_ddl_sql() for T_RenameStmt.
	appendStringInfoString(&g_remote_ddl_ctx.ddl_sql_src,
		g_remote_ddl_ctx.orig_sql);
	*/
}

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

static void fetch_column_data_type(AlterTableColumnAction *csd, Oid typid,
	int32 typmod, Oid collOid)
{
	if (!(csd->col_dtype.len == 0 && csd->col_dtype.data == NULL))
	{
		Assert(csd->col_dtype.len > 0 && csd->col_dtype.data != NULL);
		return; // this could happen for leaf tables of a partitioned table.
	}

	initStringInfo2(&csd->col_dtype, 64, TopTransactionContext);
	build_column_data_type(&csd->col_dtype, typid, typmod, collOid);
}

static void build_column_data_type(StringInfo str, Oid typid,
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


void init_remote_alter_seq(RemoteAlterSeq *raseq)
{
	initStringInfo2(&raseq->update_stmt, 256, TopTransactionContext);
	initStringInfo2(&raseq->update_stmt_peer, 256, TopTransactionContext);
	raseq->newtypid = InvalidOid;
	raseq->do_restart = false;
	raseq->restart_val = 0;
}

void AlterDependentTables(Oid enum_type_oid)
{
	ListCell *lc;
	List *reflist = getTypeTableColumns(enum_type_oid);
	int cnt = 0;

	/*
	  So we can see the newly added enum labels.
	*/
	CommandCounterIncrement();

	foreach (lc, reflist)
	{
		ObjectAddress *oa = (ObjectAddress *)lfirst(lc);
		Relation rel = relation_open(oa->objectId, RowExclusiveLock);
		if (cnt++ == 0)
			SetRemoteDDLInfo(rel, DDL_OP_Type_alter, DDL_ObjType_table);

		if (rel->rd_rel->relkind != RELKIND_RELATION ||
			rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP ||
			rel->rd_rel->relshardid == InvalidOid)
			goto next;
		Form_pg_attribute attrs = rel->rd_att->attrs + oa->objectSubId - 1;
		TrackAlterColType(rel, attrs->attname.data, attrs->attnotnull,
			attrs->atttypid, attrs->atttypmod, attrs->attcollation);
next:
		relation_close(rel, NoLock);
	}
}
