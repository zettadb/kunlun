/*-------------------------------------------------------------------------
 *
 * remote_ddl.c
 *
 *  Generate sqls for creating/deleting/altering objects in storage nodes.
 *
 * Copyright (c) 2021-2022 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 *-------------------------------------------------------------------------
 */

#include "remote_ddl.h"

#include "miscadmin.h"
#include "access/remote_meta.h"
#include "access/heapam.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "access/htup_details.h"
#include "access/remotetup.h"
#include "catalog/dependency.h"
#include "catalog/objectaccess.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_am_d.h"
#include "commands/seclabel.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "nodes/print.h"
#include "optimizer/planner.h"
#include "postmaster/postmaster.h"
#include "rewrite/rewriteHandler.h"
#include "sharding/sharding_conn.h"
#include "sharding/cluster_meta.h"
#include "tcop/utility.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/queryenvironment.h"
#include "utils/syscache.h"
#include "utils/builtins.h"
#include "libpq-fe.h"

#include "common.h"
#include "sequence_service.h"

extern bool use_mysql_native_seq; // guc variable
extern int str_key_part_len;
extern const int64_t InvalidSeqVal;
static void generate_remote_seq_create(Relation seq_rel, Form_pg_sequence seqform);

void remote_create_database(const char *dbname)
{
	StringInfoData remote_sql;
	initStringInfo(&remote_sql);
	/* create database for the default schema */
	appendStringInfo(&remote_sql, "CREATE DATABASE IF NOT EXISTS %s_$$_public", dbname);
	List *shardlist = GetAllShardIds();
	ListCell *lc;
	foreach(lc, shardlist)
	{
		enque_remote_ddl(SQLCOM_CREATE_DB, lfirst_oid(lc), &remote_sql, false);
	}
}

void remote_drop_database(const char *dbname)
{
	/* Generate ddl to drop database in stroage nodes */
	List *shardlist = GetAllShardIds();
	ListCell *lc;
	StringInfoData conninfo, remote_sql;
	initStringInfo(&conninfo);
	initStringInfo(&remote_sql);

	appendStringInfo(&conninfo, "host=%s port=%d dbname=%s user=postgres", Unix_socket_directories, PostPortNumber, dbname);

	/* We cannot get the schema of other database directly, so connect and query it */
	PGconn *conn = PQconnectdb(conninfo.data);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		/* TODO: query the storage nodes with 'SHOW DATABASES LIKE 'dbname_$$_%'*/
		// elog(ERROR, "failed to connect to database %s to get collect schema to deleted", dbname);
		appendStringInfo(&remote_sql, "drop database if exists %s_$$_public", dbname);
		foreach (lc, shardlist)
		{
			enque_remote_ddl(SQLCOM_DROP_DB, lfirst_oid(lc), &remote_sql, false);
		}
	}
	else
	{
		PG_TRY();
		{
			const char *sql =
				"SELECT nspname  FROM pg_namespace "
				"WHERE nspname='public' OR nspowner != (SELECT usesysid FROM pg_user WHERE usename='postgres')";

			PGresult *results = PQexec(conn, sql);

			if (PQresultStatus(results) != PGRES_TUPLES_OK)
			{
				elog(ERROR, "failed to get schema information %s", PQresultErrorMessage(results));
			}

			for (int i = 0; i < PQntuples(results); ++i)
			{
				resetStringInfo(&remote_sql);
				char *nspname = PQgetvalue(results, i, 0);
				appendStringInfo(&remote_sql, "drop database if exists %s_$$_%s", dbname, nspname);

				foreach (lc, shardlist)
				{
					enque_remote_ddl(SQLCOM_DROP_DB, lfirst_oid(lc), &remote_sql, false);
				}
			}
			PQclear(results);
		}
		PG_CATCH();
		{
			PQfinish(conn);
			PG_RE_THROW();
		}
		PG_END_TRY();

		PQfinish(conn);
	}

	/* Drop non native sequence defined in that database */
	remote_drop_nonnative_sequence_in_schema(dbname, "%");
}

void remote_create_schema(const char *schema)
{
	/* Generate ddl to create schema in storage */
	StringInfoData remote_sql;
	initStringInfo(&remote_sql);
	appendStringInfo(&remote_sql, "CREATE DATABASE IF NOT EXISTS %s_$$_%s", get_database_name(MyDatabaseId), schema);
	List *shardlist = GetAllShardIds();
	ListCell *lc;
	foreach (lc, shardlist)
	{
		enque_remote_ddl(SQLCOM_CREATE_DB, lfirst_oid(lc), &remote_sql, false);
	}
}

void remote_drop_schema(const char *schema)
{
	StringInfoData remote_sql;
	initStringInfo(&remote_sql);
	appendStringInfo(&remote_sql, "DROP DATABASE IF EXISTS %s_$$_%s", get_database_name(MyDatabaseId), schema);
	List *shardlist = GetAllShardIds();
	ListCell *lc;
	foreach (lc, shardlist)
	{
		enque_remote_ddl(SQLCOM_DROP_DB, lfirst_oid(lc), &remote_sql, false);
	}

	/* drop sequence belong to the deleted schema */
	remote_drop_nonnative_sequence_in_schema(
		get_database_name(MyDatabaseId),
		schema);
}

void remote_create_table(Relation rel)
{
	Oid relid = rel->rd_id;
	Oid shardid = rel->rd_rel->relshardid;

	if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
		return;

	{
		/**
		 * Pg may create multiple objects with different shardid,
		 * we always adjust the shardid to the first created object.
		 */
		shardid = remoteddl_set_shardid(shardid);

		change_relation_shardid(relid, shardid);

		rel->rd_rel->relshardid = shardid;
	}

	/* Generate sql executed by kunlun storage node */
	if (shardid != InvalidOid)
	{
		StringInfoData remote_sql;
		initStringInfo(&remote_sql);
		appendStringInfo(&remote_sql, "CREATE TABLE IF NOT EXISTS %s (",
						 make_qualified_name(RelationGetNamespace(rel),
											 RelationGetRelationName(rel), NULL));

		for (int i = 0; i < rel->rd_att->natts; ++i)
		{
			Form_pg_attribute attr = rel->rd_att->attrs + i;
			appendStringInfo(&remote_sql, "%c %s ", i ? ',' : ' ', attr->attname.data);
			build_column_data_type(&remote_sql,
								   attr->atttypid,
								   attr->atttypmod,
								   attr->attcollation);
			if (attr->attnotnull)
				appendStringInfo(&remote_sql, " not null");
		}
		appendStringInfoChar(&remote_sql, ')');
		if (rel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED)
		{
			appendStringInfo(&remote_sql, " ENGINE=MyISAM;");
		}

		/* add the */
		enque_remote_ddl(SQLCOM_CREATE_TABLE, shardid, &remote_sql, false);
	}
}

void remote_drop_table(Relation relation)
{
	Assert(relation->rd_rel->relkind == RELKIND_RELATION);
	StringInfoData remote_sql;
	initStringInfo(&remote_sql);
	appendStringInfo(&remote_sql, "DROP TABLE IF EXISTS %s",
					 make_qualified_name(RelationGetNamespace(relation),
										 RelationGetRelationName(relation), NULL));

	enque_remote_ddl(SQLCOM_DROP_TABLE, relation->rd_rel->relshardid, &remote_sql, false);
}

void generate_remote_seq_create(Relation seq_rel, Form_pg_sequence seqform)
{
	StringInfoData remote_sql;
	initStringInfo(&remote_sql);

	char seqcache[32];
	if (seqform->seqcache <= 1)
	{
		seqform->seqcache = 1;
		snprintf(seqcache, sizeof(seqcache), "NOCACHE");
	}
	else
		snprintf(seqcache, sizeof(seqcache), "CACHE %ld", seqform->seqcache);

	if (use_mysql_native_seq)
	{
		appendStringInfo(&remote_sql, "create sequence %s_$$_%s.%s increment by %ld start with %ld maxvalue %ld minvalue %ld %s %s ",
						 get_database_name(MyDatabaseId),
						 get_namespace_name(RelationGetNamespace(seq_rel)),
						 RelationGetRelationName(seq_rel),
						 seqform->seqincrement,
						 seqform->seqstart,
						 seqform->seqmax,
						 seqform->seqmin,
						 seqform->seqcycle ? "cycle" : "nocycle",
						 seqcache);
	}
	else
	{
		appendStringInfo(&remote_sql, "insert into kunlun_sysdb.sequences(db, name, curval, start, step, max_value, min_value, do_cycle, n_cache)"
									  "values('%s_$$_%s', '%s', %ld, %ld, %ld, %ld, %ld, %d, %ld) ",
						 get_database_name(MyDatabaseId),
						 get_namespace_name(RelationGetNamespace(seq_rel)),
						 RelationGetRelationName(seq_rel),
						 InvalidSeqVal,
						 seqform->seqstart,
						 seqform->seqincrement,
						 seqform->seqmax,
						 seqform->seqmin,
						 seqform->seqcycle ? 1 : 0,
						 seqform->seqcache);
	}

	enque_remote_ddl(SQLCOM_CREATE_SEQUENCE, seq_rel->rd_rel->relshardid, &remote_sql, false);
}

void remote_create_sequence(Relation relation)
{
	Oid shardid = relation->rd_rel->relshardid;
	Oid seq_relid = relation->rd_id;
	HeapTuple pgstuple;
	Form_pg_sequence pgsform;

	{
		/**
		 * Pg may create multiple objects with different shardid,
		 * we always adjust the shardid to the first created object.
		 */
		shardid = remoteddl_set_shardid(shardid);

		change_relation_shardid(seq_relid, shardid);

		relation->rd_rel->relshardid = shardid;
	}
	
	pgstuple = SearchSysCache1(SEQRELID, ObjectIdGetDatum(seq_relid));
	if (!HeapTupleIsValid(pgstuple))
		elog(ERROR, "cache lookup failed for sequence %u", seq_relid);
	pgsform = (Form_pg_sequence)GETSTRUCT(pgstuple);

	generate_remote_seq_create(relation, pgsform);

	ReleaseSysCache(pgstuple);
}

void remote_drop_sequence(Relation relation)
{
  Assert(relation->rd_rel->relkind == RELKIND_SEQUENCE);
  StringInfoData remote_sql;
  initStringInfo(&remote_sql);
  enum enum_sql_command sql_command;
  if (use_mysql_native_seq)
  {
    appendStringInfo(&remote_sql, "drop sequence %s",
                     make_qualified_name(RelationGetNamespace(relation),
                                         RelationGetRelationName(relation), NULL));
    sql_command = SQLCOM_DROP_SEQUENCE;
  }
  else
  {
    appendStringInfo(&remote_sql, "delete from kunlun_sysdb.sequences where db='%s_$$_%s' and name='%s'",
                     get_database_name(MyDatabaseId),
                     get_namespace_name(RelationGetNamespace(relation)),
                     RelationGetRelationName(relation));
    sql_command = SQLCOM_DELETE;
  }

  enque_remote_ddl(sql_command, relation->rd_rel->relshardid, &remote_sql, false);
}

void remote_drop_nonnative_sequence_in_schema(const char *db, const char *schema)
{
  if (use_mysql_native_seq)
    return;
  StringInfoData remote_sql;
  initStringInfo(&remote_sql);
  appendStringInfo(&remote_sql, "delete from kunlun_sysdb.sequences where db like '%s_$$_%s'", db, schema);

  List *shardlist = GetAllShardIds();
  ListCell *lc;
  foreach (lc, shardlist)
  {
    enque_remote_ddl(SQLCOM_UPDATE, lfirst_oid(lc), &remote_sql, false);
  }
}

static bool
is_alter_table_column_only(Node *node)
{
	if (IsA(node, AlterTableStmt))
	{
		ListCell *lc;
		foreach (lc, ((AlterTableStmt *)node)->cmds)
		{
			AlterTableType type = lfirst_node(AlterTableCmd, lc)->subtype;
			if (type != AT_AlterColumnType && type != AT_AlterColumnGenericOptions)
				return false;
		}
		return true;
	}

	return false;
}

void remote_add_index(Relation indexrel)
{
	Node *top_stmt = remoteddl_top_stmt();
	Assert(top_stmt);
	Assert(indexrel->rd_rel->relkind == RELKIND_INDEX);
	if (is_alter_table_column_only(top_stmt)) return;
	if (indexrel->rd_rel->relpersistence == RELPERSISTENCE_TEMP) return;

	/* Get the coresponding heap relation */
	Form_pg_index indexForm;
	Oid indexRelationId = RelationGetRelid(indexrel);
	HeapTuple indexTuple = SearchSysCache1(INDEXRELID,
										   ObjectIdGetDatum(indexRelationId));
	if (!HeapTupleIsValid(indexTuple))
		elog(ERROR, "cache lookup failed for index %u", indexRelationId);
	indexForm = (Form_pg_index)GETSTRUCT(indexTuple);
	Relation heaprel = relation_open(indexForm->indrelid, NoLock);
	const char *indextype =
		indexForm->indisprimary ? "primary" : (indexForm->indisunique ? "unique" : "");
	const char *indexname =
	 	indexForm->indisprimary ? "" : RelationGetRelationName(indexrel);
	ReleaseSysCache(indexTuple);

	{
		StringInfoData remote_ddl;
		initStringInfo(&remote_ddl);

		if (RelationGetNamespace(indexrel) != RelationGetNamespace(heaprel))
		{
			ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
							errmsg("The schema of the index relation (%s, %u) in kunlun-db must be the same as its main relation(%s, %u).",
								   RelationGetRelationName(indexrel), RelationGetNamespace(indexrel),
								   RelationGetRelationName(heaprel), RelationGetNamespace(heaprel))));
		}

		appendStringInfo(&remote_ddl, "alter table %s add %s key %s using %s (",
						 make_qualified_name(RelationGetNamespace(heaprel),
											 RelationGetRelationName(heaprel),
											 NULL),
						 indextype,
						 indexname,
						 indexrel->rd_rel->relam == BTREE_AM_OID ? "BTREE" : "HASH");

		char keypartlenstr[32] = {'\0'};
		// the first indnkeyattrs fields are key columns, the rest are included columns.
		for (int natt = 0; natt < IndexRelationGetNumberOfKeyAttributes(indexrel); natt++)
		{
			int attno = indexrel->rd_index->indkey.values[natt];
			if (attno <= 0)
				ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
								errmsg("Can not index system columns.")));

			keypartlenstr[0] = '\0';
			Form_pg_attribute attrs = heaprel->rd_att->attrs + attno - 1;
			if (needs_mysql_keypart_len(attrs->atttypid, attrs->atttypmod))
			{
				snprintf(keypartlenstr, sizeof(keypartlenstr), "(%d)", str_key_part_len);
			}

			const char *order_option = "";
			if (indexrel->rd_amhandler == RM_BTREE_ID)
				order_option = (indexrel->rd_indoption[natt] & INDOPTION_DESC) ? "DESC" : "ASC";

			appendStringInfo(&remote_ddl, "%s%s %s, ",
							 attrs->attname.data, keypartlenstr, order_option);
		}

		remote_ddl.len -= 2;
		appendStringInfoChar(&remote_ddl, ')');

		enque_remote_ddl(SQLCOM_CREATE_INDEX, heaprel->rd_rel->relshardid, &remote_ddl, false);
	}

	relation_close(heaprel, NoLock);
}

void remote_drop_index(Relation relation)
{
	Node *top_stmt = remoteddl_top_stmt();
	Assert(top_stmt);
	Assert(relation->rd_rel->relkind == RELKIND_INDEX);
	if (is_alter_table_column_only(top_stmt)) return;

	/* Only generate remote ddl if the orignal client sql is 'drop index' or 'alter */
	bool generate_sql = false;
	switch (nodeTag(top_stmt))
	{
	case T_DropStmt:
		generate_sql = (((DropStmt *)top_stmt)->removeType == OBJECT_INDEX);
		break;
	case T_AlterTableStmt: /* maybe drop constraint */
		generate_sql = true;
		break;
	default:
		break;
	}

	if (!generate_sql)
		return;

	/* Get the coresponding heap relation */
	Form_pg_index indexForm;
	Oid indexRelationId = RelationGetRelid(relation);
	HeapTuple indexTuple = SearchSysCache1(INDEXRELID,
										   ObjectIdGetDatum(indexRelationId));
	if (!HeapTupleIsValid(indexTuple))
		elog(ERROR, "cache lookup failed for index %u", indexRelationId);
	indexForm = (Form_pg_index)GETSTRUCT(indexTuple);
	/* The primary index is always be 'PRIMARY' in MySQL */
	const char *indexname =
		indexForm->indisprimary ? "`PRIMARY`" : RelationGetRelationName(relation);
	Relation heaprel = relation_open(indexForm->indrelid, NoLock);
	ReleaseSysCache(indexTuple);

	StringInfoData remote_sql;
	initStringInfo(&remote_sql);
	appendStringInfo(&remote_sql, "drop index %s on %s",
					 indexname,
					 make_qualified_name(RelationGetNamespace(heaprel),
										 RelationGetRelationName(heaprel), NULL));

	enque_remote_ddl(SQLCOM_DROP_INDEX, heaprel->rd_rel->relshardid, &remote_sql, false);
	relation_close(heaprel, NoLock);
}

void remote_alter_table(Relation rel)
{
	if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
		return;
	StringInfoData remote_sql;
	initStringInfo(&remote_sql);
	Node *top_stmt = remoteddl_top_stmt();
	if (nodeTag(top_stmt) == T_RenameStmt)
	{
		RenameStmt *stmt = (RenameStmt *)top_stmt;
		Assert(stmt->renameType == OBJECT_TABLE);
		appendStringInfo(&remote_sql, "rename table %s to %s",
						 make_qualified_name(RelationGetNamespace(rel),
											 RelationGetRelationName(rel),
											 NULL),
						 make_qualified_name(RelationGetNamespace(rel),
											 stmt->newname,
											 NULL));
	}
	else if (nodeTag(top_stmt) == T_AlterObjectSchemaStmt)
	{
		AlterObjectSchemaStmt *stmt = (AlterObjectSchemaStmt *)top_stmt;
		Assert(stmt->objectType == OBJECT_TABLE);
		Oid new_nspid = get_namespace_oid(stmt->newschema, false);
		
		if (new_nspid != RelationGetNamespace(rel))
		{
			appendStringInfo(&remote_sql, "rename table %s to %s",
							 make_qualified_name(RelationGetNamespace(rel),
												 RelationGetRelationName(rel),
												 NULL),
							 make_qualified_name(new_nspid,
												 RelationGetRelationName(rel),
												 NULL));
		}
	}

	if (remote_sql.len > 0)
		enque_remote_ddl(SQLCOM_RENAME_TABLE, rel->rd_rel->relshardid, &remote_sql, false);
}

void remote_alter_index(Relation rel)
{
	if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
		return;

	StringInfoData remote_sql;
	initStringInfo(&remote_sql);
	Node *top_stmt = remoteddl_top_stmt();
	if (nodeTag(top_stmt) == T_RenameStmt)
	{
		RenameStmt *stmt = (RenameStmt *)top_stmt;
		Relation heaprel = NULL;

		if (rel->rd_index && rel->rd_index->indisprimary)
		{
			/**
			 * The name of the primary index in MySQL is always 'PRIMARY',
			 * we do nothing on the storage nodes.
			 */
			return;
		}
		{
			/* Get the coresponding heap relation */
			Form_pg_index indexForm;
			Oid indexRelationId = RelationGetRelid(rel);
			HeapTuple indexTuple = SearchSysCache1(INDEXRELID,
												   ObjectIdGetDatum(indexRelationId));
			if (!HeapTupleIsValid(indexTuple))
				elog(ERROR, "cache lookup failed for index %u", indexRelationId);
			indexForm = (Form_pg_index)GETSTRUCT(indexTuple);
			heaprel = relation_open(indexForm->indrelid, NoLock);
			ReleaseSysCache(indexTuple);
		}

		Assert(stmt->renameType == OBJECT_INDEX || stmt->renameType == OBJECT_TABCONSTRAINT);
		appendStringInfo(&remote_sql, "alter table %s rename index %s to %s",
						 make_qualified_name(RelationGetNamespace(heaprel),
											 RelationGetRelationName(heaprel), NULL),
						 RelationGetRelationName(rel),
						 stmt->newname);
		relation_close(heaprel, NoLock);
	}

	if (remote_sql.len > 0)
		enque_remote_ddl(SQLCOM_RENAME_TABLE, rel->rd_rel->relshardid, &remote_sql, false);
}

void remote_alter_sequence(Relation rel)
{
	if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
		return;
	bool reset_cache = false;
	StringInfoData remote_sql;
	initStringInfo(&remote_sql);
	Node *top_stmt = remoteddl_top_stmt();
	
	Oid seq_relid = RelationGetRelid(rel);
	HeapTuple tuple = SearchSysCache1(SEQRELID, ObjectIdGetDatum(seq_relid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for sequence %u", seq_relid);
	Form_pg_sequence seqform = (Form_pg_sequence)GETSTRUCT(tuple);

	if (nodeTag(top_stmt) == T_RenameStmt)
	{
		RenameStmt *stmt = (RenameStmt *)top_stmt;
		Assert(stmt->renameType == OBJECT_SEQUENCE || stmt->renameType == OBJECT_TABLE);
		appendStringInfo(&remote_sql, "update kunlun_sysdb.sequences set name= '%s' where db='%s_%s_%s' and name='%s'",
						 stmt->newname,
						 get_database_name(MyDatabaseId),
						 use_mysql_native_seq ? "@0024@0024" : "$$",
						 get_namespace_name(RelationGetNamespace(rel)),
						 RelationGetRelationName(rel));
	}
	else if (nodeTag(top_stmt) == T_AlterObjectSchemaStmt)
	{
		AlterObjectSchemaStmt *stmt = (AlterObjectSchemaStmt*)top_stmt;
		appendStringInfo(&remote_sql, "update kunlun_sysdb.sequences set db= '%s_%s_%s' where db='%s_%s_%s' and name='%s'",
						 get_database_name(MyDatabaseId),
						 use_mysql_native_seq ? "@0024@0024" : "$$",
						 stmt->newschema,
						 get_database_name(MyDatabaseId),
						 use_mysql_native_seq ? "@0024@0024" : "$$",
						 get_namespace_name(RelationGetNamespace(rel)),
						 RelationGetRelationName(rel));
	}
	else if (nodeTag(top_stmt) == T_AlterSeqStmt)
	{
		StringInfoData option_update;
		initStringInfo(&option_update);
		AlterSeqStmt *stmt = (AlterSeqStmt *)top_stmt;
		ListCell *option;
		foreach (option, stmt->options)
		{
			DefElem *defel = (DefElem *)lfirst(option);
			if (option_update.len > 0)
				appendStringInfoChar(&option_update, ',');

			if (strcmp(defel->defname, "increment") == 0)
			{
				appendStringInfo(&option_update, " step = %ld", defGetInt64(defel));
			}
			else if (strcmp(defel->defname, "start") == 0)
			{
				appendStringInfo(&option_update, " start = %ld", defGetInt64(defel));
			}
			else if (strcmp(defel->defname, "restart") == 0)
			{
				if (defel->arg == NULL)
					appendStringInfo(&option_update, " curval = %ld", InvalidSeqVal);
				else
					appendStringInfo(&option_update, " curval = %ld", defGetInt64(defel));
			}
			else if (strcmp(defel->defname, "maxvalue") == 0)
			{
				appendStringInfo(&option_update, " max_value = %ld", defGetInt64(defel));
			}
			else if (strcmp(defel->defname, "minvalue") == 0)
			{
				appendStringInfo(&option_update, " min_value = %ld", defGetInt64(defel));
			}
			else if (strcmp(defel->defname, "cache") == 0)
			{
				appendStringInfo(&option_update, " n_cache = %ld", defGetInt64(defel));
			}
			else if (strcmp(defel->defname, "cycle") == 0)
			{
				bool cycle = intVal(defel);
				appendStringInfo(&option_update, " do_cycle = %s", cycle ? "true" : "false");
			}
		}

		if (option_update.len > 0)
		{
			appendStringInfo(&remote_sql, "update kunlun_sysdb.sequences set %.*s where db='%s_%s_%s' and name='%s'",
							 option_update.len, option_update.data,
							 get_database_name(MyDatabaseId),
							 use_mysql_native_seq ? "@0024@0024" : "$$",
							 get_namespace_name(RelationGetNamespace(rel)),
							 RelationGetRelationName(rel));
			reset_cache = true;
		}
	}

	ReleaseSysCache(tuple);

	if (remote_sql.len > 0)
		enque_remote_ddl(SQLCOM_UPDATE, rel->rd_rel->relshardid, &remote_sql, false);

	/* Mark the sequence cache entry should be reloaded */
	if (reset_cache)
		invalidate_seq_shared_cache(MyDatabaseId, rel->rd_id, false);
}

/* Use to evaluate the default value when add column */
static void print_default_value(Relation rel, int attrnum, StringInfo str)
{
	Oid typoutput;
	FmgrInfo finfo;
	bool isnull, typisvarlena;
	int orig_datestyle, orig_dateorder, orig_intvstyle;
	pg_tz *origtz;
	
	Expr *defexpr = (Expr*)build_column_default(rel, attrnum);
	EState *estate = CreateExecutorState();
	ExprContext *econtext = CreateExprContext(estate);
	/* Initialize executable expression */
	ExprState *state = ExecInitExpr(expression_planner(defexpr), NULL);
	Datum value = ExecEvalExpr(state, econtext, &isnull);
	if (isnull)
	{
		FreeExprContext(econtext, false);
		FreeExecutorState(estate);
		appendStringInfo(str, "null");
		return;
	}

	/* Get the print function */
	{
		Const c;
		Form_pg_attribute attr;
		RemotePrintExprContext rpec;

		attr = TupleDescAttr(rel->rd_att, attrnum - 1);
		memset(&c, 0, sizeof(c));
		c.xpr.type = T_Const;
		c.consttype = attr->atttypid;
		c.constisnull = isnull;
		c.constvalue = value;
		InitRemotePrintExprContext(&rpec, NIL);
		if (snprint_expr(str, (Expr*)&c, &rpec) <= 0)
		{
			elog(ERROR, "Serialize field (attrnum=%d) default value failed", attrnum);
		}
	}
	
	FreeExprContext(econtext, false);
	FreeExecutorState(estate);
}

void remote_alter_column(Relation rel, int attrnum, ObjectAccessType type)
{
	/* Generate alter for remote storage node if the client ddl is alter too*/
	if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
		return;

	StringInfoData remote_sql;
	initStringInfo(&remote_sql);
	if (nodeTag(remoteddl_top_stmt()) == T_RenameStmt)
	{
		if (type == OAT_POST_ALTER)
		{
			Form_pg_attribute attr = rel->rd_att->attrs + attrnum - 1;
			appendStringInfo(&remote_sql, "alter table %s rename column %s to ",
							 make_qualified_name(RelationGetNamespace(rel), RelationGetRelationName(rel), NULL),
							 attr->attname.data);
			print_pg_attribute(rel->rd_id, attrnum, true, &remote_sql);
		}
	}
	else if (nodeTag(remoteddl_top_stmt()) == T_AlterTableStmt)
	{
		if (type == OAT_DROP)
		{
			Form_pg_attribute attr = rel->rd_att->attrs + attrnum - 1;
			appendStringInfo(&remote_sql, "alter table %s drop column %s ",
							 make_qualified_name(RelationGetNamespace(rel), RelationGetRelationName(rel), NULL),
							 attr->attname.data);
		}
		else
		{
			const char *action = NULL;
			if (type == OAT_POST_ALTER)
				action = "modify";
			else if (type == OAT_POST_CREATE)
				action = "add column";

			Assert(action);

			appendStringInfo(&remote_sql, "alter table %s %s ",
							 make_qualified_name(RelationGetNamespace(rel), RelationGetRelationName(rel), NULL),
							 action);

			print_pg_attribute(rel->rd_id, attrnum, false, &remote_sql);

			Expr *defexpr = (Expr*)build_column_default(rel, attrnum);
			if (defexpr && type == OAT_POST_CREATE)
			{
				RemotePrintExprContext rpec;
				StringInfoData defstr;
				initStringInfo(&defstr);
				InitRemotePrintExprContext(&rpec, NULL);
				/**
				 * Pushdown the default value if possible, otherwise evaluates the expression
				 */
				if (snprint_expr(&defstr, defexpr, &rpec) > 0)
				{
					appendStringInfo(&remote_sql, " default (%.*s)", defstr.len, defstr.data);
				}
				else
				{
					resetStringInfo(&defstr);
					print_default_value(rel, attrnum, &defstr);
					appendStringInfo(&remote_sql, " default (%.*s)", defstr.len, defstr.data);
				}

				// else
				// {
				// 	/* hope the storage nodes report error*/
				// 	Datum expr = DirectFunctionCall3(pg_get_expr_ext,
				// 									 CStringGetTextDatum(attdef->adbin),
				// 									 ObjectIdGetDatum(rel->rd_id),
				// 									 BoolGetDatum(false));
				// 	appendStringInfo(&remote_sql, " default %s", TextDatumGetCString(expr));
				// }
			}
		}
	}

	if (remote_sql.len > 0)
		enque_remote_ddl(SQLCOM_ALTER_TABLE,
						 rel->rd_rel->relshardid,
						 &remote_sql,
						 false);
}

void remote_alter_type(Oid typid)
{
	List *refColumns = getTypeTableColumns(typid);	
	ListCell *lc;
	StringInfoData remote_sql;
	initStringInfo(&remote_sql);

	CommandCounterIncrement();

	foreach (lc, refColumns)
	{
		ObjectAddress *object = (ObjectAddress *)lfirst(lc);
		if (object->classId != RelationRelationId || object->objectSubId == 0)
			continue;
		Relation rel = relation_open(object->objectId, NoLock);
		if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP)
		{
			resetStringInfo(&remote_sql);
			appendStringInfo(&remote_sql, "alter table %s modify ",
								   make_qualified_name(RelationGetNamespace(rel),
													   RelationGetRelationName(rel),
													   NULL));

			print_pg_attribute(RelationGetRelid(rel),
							   object->objectSubId,
							   false,
							   &remote_sql);
			
			enque_remote_ddl(SQLCOM_ALTER_TABLE,
							 rel->rd_rel->relshardid,
							 &remote_sql,
							 false);
		}
		relation_close(rel, NoLock);
	}
}
static void
do_remote_truncate(Relation rel, bool restart_seqs)
{
       StringInfoData remote_sql;
       initStringInfo(&remote_sql);

       if (restart_seqs)
       {
               List *seqlist = getOwnedSequences(RelationGetRelid(rel), 0);
               ListCell *lc;

               foreach (lc, seqlist)
               {
                       Oid seqoid;
                       HeapTuple tuple;
                       Form_pg_sequence form_seq;
                       Relation seqrel;

                       seqoid = lfirst_oid(lc);
                       tuple = SearchSysCache1(SEQRELID, ObjectIdGetDatum(seqoid));
                       if (!HeapTupleIsValid(tuple))
                               elog(ERROR, "cache lookup failed for sequence %u", seqoid);
                       form_seq = (Form_pg_sequence)GETSTRUCT(tuple);
                       seqrel = heap_open(seqoid, NoLock);
                       {
                               resetStringInfo(&remote_sql);
                               appendStringInfo(&remote_sql, "update kunlun_sysdb.sequences set curval = %ld where db='%s_%s_%s' and name='%s'",
                                                InvalidSeqVal,
                                                get_database_name(MyDatabaseId),
                                                use_mysql_native_seq ? "@0024@0024" : "$$",
                                                get_namespace_name(RelationGetNamespace(seqrel)),
                                                RelationGetRelationName(seqrel));
                               enque_remote_ddl(SQLCOM_UPDATE,
                                                seqrel->rd_rel->relshardid,
                                                &remote_sql,
                                                false);
                       }
                       heap_close(seqrel, NoLock);
                       ReleaseSysCache(tuple);

                       /* Truncate table not invoke object alter hook, so have to handle it here */
                       invalidate_seq_shared_cache(MyDatabaseId, seqoid, false);
               }
       }

       resetStringInfo(&remote_sql);
       appendStringInfo(&remote_sql, "truncate table %s",
                        make_qualified_name(RelationGetNamespace(rel), RelationGetRelationName(rel), NULL));
       enque_remote_ddl(SQLCOM_TRUNCATE,
                        rel->rd_rel->relshardid,
                        &remote_sql,
                        false);
}

bool remote_truncate_table(TruncateStmt *stmt)
{
       ListCell *lc;
       List *reloids = NIL;
       int numTemp = 0;
       int numNonTemp = 0;

       foreach (lc, stmt->relations)
       {
               RangeVar *rv = lfirst(lc);
               Relation rel = heap_openrv(rv, NoLock);

               if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
               {
                       numTemp ++;
                       heap_close(rel, NoLock);
                       continue;;
               }
               numNonTemp ++;

               if (IS_REMOTE_RTE(rel->rd_rel) &&
                   !list_member_oid(reloids, rel->rd_id))
               {
                       reloids = lappend_oid(reloids, rel->rd_id);
                       do_remote_truncate(rel, stmt->restart_seqs);
               }

               if (rv->inh)
               {
                       List *children;
                       ListCell *child;

                       children = find_all_inheritors(rel->rd_id, NoLock, NULL);
                       foreach (child, children)
                       {
                               Oid childoid = lfirst_oid(child);
                               if (!list_member_oid(reloids, rel->rd_id))
                               {
                                       Relation childrel = heap_open(childoid, NoLock);
                                       if (IS_REMOTE_RTE(childrel->rd_rel))
                                       {
                                               reloids = lappend_oid(reloids, childrel->rd_id);
                                               do_remote_truncate(childrel, stmt->restart_seqs);
                                       }
                                       heap_close(childoid, NoLock);
                               }
                       }
               }

               heap_close(rel, NoLock);
       }

       if (numTemp && numNonTemp)
       {
               ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                               errmsg("Kunlun-db: Statement 'truncate' not support temporary object mixied normal object")));
       }

       return numNonTemp > 0;
}

typedef struct Remote_ddl_sql
{
	enum enum_sql_command sql_command;
	Oid shard;
	char *sql;
	size_t sql_len;
}Remote_ddl_sql;

/**
 * @brief  Add the ddl that will be executed on Kunlun storage to the queue.
 */
void enque_remote_ddl(enum enum_sql_command sql_command, Oid shard, StringInfo query, bool replace)
{
	MemoryContext oldctx =
		MemoryContextSwitchTo(g_remote_ddl_trans->mem_ctx);

	Remote_ddl_sql *ddl = (Remote_ddl_sql *)palloc0(sizeof(Remote_ddl_sql));
	ddl->sql_command = sql_command;
	ddl->shard = shard;
	ddl->sql = pnstrdup(query->data, query->len);
	ddl->sql_len = query->len;

	g_remote_ddl_context->ddlremote_list =
		lappend(g_remote_ddl_context->ddlremote_list, ddl);

	MemoryContextSwitchTo(oldctx);
}

/**
 * @brief Send all the ddl in the queue to the kunlun storage for execution
 */
void execute_all_remote_ddl(void)
{
	ListCell *lc1, *lc2;
	foreach (lc1, g_remote_ddl_trans->ddl_context_list)
	{
		Remote_ddl_context *rcontext = (Remote_ddl_context*)lfirst(lc1);
		foreach (lc2, rcontext->ddlremote_list)
		{
			Remote_ddl_sql *ddl = (Remote_ddl_sql *)lfirst(lc2);
			AsyncStmtInfo *asi = GetAsyncStmtInfo(ddl->shard);
			send_stmt_async_nowarn(asi,
							  ddl->sql,
							  ddl->sql_len,
							  CMD_DDL,
							  false,
							  ddl->sql_command);
		}
	}
	
	flush_all_stmts();
}

char *dump_all_remote_ddl()
{
	ListCell *lc;
	bool first = true;
	StringInfoData str;
	initStringInfo(&str);
	
	if (list_length(g_remote_ddl_context->ddlremote_list) > 0)
	{
		appendStringInfoChar(&str, '[');
		foreach (lc, g_remote_ddl_context->ddlremote_list)
		{
			Remote_ddl_sql *ddl = (Remote_ddl_sql *)lfirst(lc);
			appendStringInfo(&str, "%s {\"target_shard_id\": %u, \"sql_text\": \"%.*s\"}",
							 first ? "" : ",",
							 ddl->shard,
							 (int)ddl->sql_len, ddl->sql);
			first = false;
		}
		appendStringInfoChar(&str, ']');
	}
	return str.data;
}
