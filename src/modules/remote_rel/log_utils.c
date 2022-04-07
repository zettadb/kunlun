/*-------------------------------------------------------------------------
 *
 * log_utils.c
 *
 *	Helper function to use when logging ddl events.
 *
 * Copyright (c) 2021-2022 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 *-------------------------------------------------------------------------
 */
#include <sys/time.h>
#include <unistd.h>

#include "log_utils.h"

#include "access/xact.h"
#include "access/remote_xact.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "catalog/pg_cluster_meta.h"
#include "catalog/pg_ddl_log_progress.h"
#include "catalog/indexing.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "tcop/debug_injection.h"
#include "sharding/cluster_meta.h"
#include "utils/guc.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"

#include "common.h"
#include "remote_ddl.h"

/**
 * @brief DDL information to be logged to the metaserver
 *
 */
typedef struct DDLLogContext
{
	DDL_OP_Types optype;
	DDL_ObjTypes objtype;
	char *user;
	char *db;
	char *schema;
	char *object;
	char *query;
	Oid shardid;
	char *info;
	char *search_path;
	char *sql_storage;
} DDLLogContext;

void ddl_log_get_lock()
{
	pgstat_report_activity(STATE_RUNNING, "waiting for global ddl lock");

	MYSQL_CONN *conn = get_metadata_cluster_conn(false);
	do
	{
		send_stmt_to_cluster_meta(conn, STRING_WITH_LEN("SELECT GET_LOCK('DDL', 1)"), CMD_SELECT, false);
		MYSQL_ROW row = mysql_fetch_row(conn->result);
		if (row && row[0] && row[0][0] == '1')
		{
			free_metadata_cluster_result(conn);
			break;
		}
		free_metadata_cluster_result(conn);
		CHECK_FOR_INTERRUPTS();
	} while (true);
}

void ddl_log_release_lock()
{
	MYSQL_CONN *conn = get_metadata_cluster_conn(false);
	PG_TRY();
	{
		send_stmt_to_cluster_meta(conn, STRING_WITH_LEN("SELECT RELEASE_LOCK('DDL')"), CMD_SELECT, false);
		free_metadata_cluster_result(conn);
	}
	PG_CATCH();
	{
		disconnect_metadata_shard();
	}
	PG_END_TRY();
}

void update_ddl_applier_progress(uint64_t newid)
{
	Datum values[3] = {0, 0, 0};
	bool nulls[3] = {false, false, false};
	bool replaces[3] = {false, false, false};
	HeapTuple tup = NULL;
	SysScanDesc scan;
	ScanKeyData key;

	bool free_txn = false;
	if (!IsTransactionState())
	{
		free_txn = true;
		StartTransactionCommand();
	}

	Oid dbid = get_database_oid("postgres", false);

	Relation ddloplog_rel = heap_open(DDLLogProgressRelationId, RowExclusiveLock);

	ScanKeyInit(&key,
				Anum_pg_ddl_log_progress_dbid,
				BTGreaterEqualStrategyNumber,
				F_OIDEQ, dbid);

	scan = systable_beginscan(ddloplog_rel, DDLLogDbidIndexId, true, NULL, 1, &key);

	if ((tup = systable_getnext(scan)))
	{
		Form_pg_ddl_log_progress prog = ((Form_pg_ddl_log_progress)GETSTRUCT(tup));
		if (newid > prog->ddl_op_id)
		{
			replaces[Anum_pg_ddl_log_progress_ddl_op_id - 1] = true;
			values[Anum_pg_ddl_log_progress_ddl_op_id - 1] = UInt64GetDatum(newid);
			HeapTuple newtuple =
				heap_modify_tuple(tup, RelationGetDescr(ddloplog_rel),
								  values, nulls, replaces);
			CatalogTupleUpdate(ddloplog_rel, &newtuple->t_self, newtuple);
		}
		systable_endscan(scan);
	}
	else
	{
		systable_endscan(scan);

		values[Anum_pg_ddl_log_progress_dbid - 1] = dbid;
		values[Anum_pg_ddl_log_progress_ddl_op_id - 1] = newid;
		values[Anum_pg_ddl_log_progress_max_op_id_done_local - 1] = 0;
		HeapTuple tup = heap_form_tuple(ddloplog_rel->rd_att, values, nulls);
		CatalogTupleInsert(ddloplog_rel, tup);
	}
	CommandCounterIncrement();

	relation_close(ddloplog_rel, RowExclusiveLock);

	if (free_txn)
		CommitTransactionCommand();
}

uint64_t get_ddl_applier_progress(bool sharelock)
{
	bool free_txn = false;
	if (!IsTransactionState())
	{
		free_txn = true;
		StartTransactionCommand();
	}

	Relation ddloplog_rel = heap_open(DDLLogProgressRelationId, sharelock ? AccessShareLock : NoLock);

	uint64_t opid = 0, ntups = 0;
	HeapTuple tup = NULL;
	SysScanDesc scan;

	ScanKeyData key;

	ScanKeyInit(&key,
				Anum_pg_ddl_log_progress_dbid,
				BTGreaterEqualStrategyNumber,
				F_OIDGE, (Oid)0);
	scan = systable_beginscan(ddloplog_rel, DDLLogDbidIndexId, true, NULL, 1, &key);

	while ((tup = systable_getnext(scan)) != NULL)
	{
		Form_pg_ddl_log_progress prog = ((Form_pg_ddl_log_progress)GETSTRUCT(tup));
		if (prog->dbid == InvalidOid)
			continue;
		if (opid < prog->ddl_op_id)
		{
			opid = prog->ddl_op_id;
		}
		ntups++;
	}
	systable_endscan(scan);
	heap_close(ddloplog_rel, sharelock ? AccessShareLock : NoLock);

	if (free_txn)
		CommitTransactionCommand();

	return opid;
}

void catch_up_latest_meta()
{
	MYSQL_CONN *conn;
	MYSQL_ROW row;
	char sql[256];
	int len;
	uint64_t max_opid = 0;

	pgstat_report_activity(STATE_RUNNING, "catching up latest meta");

	conn = get_metadata_cluster_conn(false);
	len = snprintf(sql, sizeof(sql), "SELECT MAX(id) from " KUNLUN_METADATA_DBNAME ".ddl_ops_log_%s", GetClusterName2());
	Assert(len < sizeof(sql));

	send_stmt_to_cluster_meta(conn, sql, len, CMD_SELECT, false);
	if ((row = mysql_fetch_row(conn->result)) && row[0])
		max_opid = strtoul(row[0], NULL, 10);
	free_metadata_cluster_result(conn);

	do
	{
		uint64_t local_opid = get_ddl_applier_progress(false);
		if (local_opid >= max_opid)
			break;
		sleep(1);
		CHECK_FOR_INTERRUPTS();
	} while (true);
}

/**
 * @brief Add information about ddl query to be logged to the meta server.
 */
void log_ddl_add(DDL_OP_Types op,
				 DDL_ObjTypes objtype,
				 const char *db,
				 const char *schema,
				 const char *object,
				 const char *query,
				 Oid shardid,
				 const char *info)
{
	Assert(!g_remote_ddl_context->ddllog_context);
	MemoryContext oldctx = MemoryContextSwitchTo(g_remote_ddl_trans->mem_ctx);

	DDLLogContext *context = (DDLLogContext *)palloc0(sizeof(DDLLogContext));
	context->optype = op;
	context->objtype = objtype;
	context->user = get_current_username();
	context->db = pstrdup(db);
	context->schema = schema ? pstrdup(schema) : "";
	context->object = object ? pstrdup(object) : "";
	context->query = query ? escape_mysql_string(query) : "";
	context->shardid = shardid;
	context->info = info ? pstrdup(info) : "";
	context->search_path = escape_mysql_string(current_search_path());
	context->sql_storage = NULL;

	g_remote_ddl_context->ddllog_context = context;

	MemoryContextSwitchTo(oldctx);

	/* Make sure we have got the name of current user */
	if (!context->user)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to get the name of current user while assembling ddl log event.")));
	}
}

extern char last_remote_sql[1024];
void log_ddl_add_extra()
{
	if (!g_remote_ddl_context)
		return;
	MemoryContext oldctx = MemoryContextSwitchTo(g_remote_ddl_trans->mem_ctx);
	DDLLogContext *context = g_remote_ddl_context->ddllog_context;
	if (context)
	{
		char *sql_storage = dump_all_remote_ddl();
		/* For debug */
		strncpy(last_remote_sql, sql_storage, sizeof(last_remote_sql));

		context->sql_storage = escape_mysql_string(sql_storage);
	}
	MemoryContextSwitchTo(oldctx);
}

bool log_ddl_skip()
{
	if (g_remote_ddl_trans)
	{
		ListCell *lc;
		foreach (lc, g_remote_ddl_trans->ddl_context_list)
		{
			Remote_ddl_context *rc = (Remote_ddl_context *)lfirst(lc);
			if (rc->ddllog_context)
				return false;
		}
	}
	return true;
}

static inline void
make_append_ddl_log_entry_procedure(StringInfo sql, const char *cluster, const char *database, const char *schema, const char *role,
									const char *user, const char *search_path, const char *object, const char *object_type, const char *op_type, uint64_t cur_opid,
									const char *client_sql, const char *remote_sqls, uint32_t shardid, uint32_t initiator_id, uint64_t txn_id, const char *logid_var)
{
	/**
	 *  CREATE PROCEDURE `append_ddl_log_entry`(
	 *		tblname varchar(256),
	 *		dbname varchar(64),
	 *		schema_name varchar(64),
	 *		role_name varchar(64),
	 *		user_name varchar(64),
	 *		search_path text,
	 *		objname varchar(64),
	 *		obj_type varchar(16),
	 *		op_type varchar(16),
	 *		cur_opid bigint unsigned,
	 *		sql_src text,
	 *		sql_src_sn text,
	 *		target_shardid int unsigned,
	 *		initiator_id int unsigned,
	 * 		txn_id  bigint unsigned,
	 *		OUT my_opid bigint unsigned)
	 */
	appendStringInfo(sql, "CALL " KUNLUN_METADATA_DBNAME ".append_ddl_log_entry('ddl_ops_log_%s', "
						  "'%s', '%s', '%s', '%s','%s', '%s', '%s', '%s', %lu, '%s', '%s', %u, %u, %lu,   %s);",
					 cluster,
					 database,
					 schema,
					 role,
					 user,
					 search_path,
					 object,
					 object_type,
					 op_type,
					 cur_opid,
					 client_sql,
					 remote_sqls,
					 shardid,
					 initiator_id,
					 txn_id,
					 logid_var);
}

/**
 * @brief Prepare a transaction which log the ddl information to the meta server.
 *
 * @return uint64_t The log id of the ddl query
 */
uint64_t log_ddl_prepare(void)
{
	uint64_t cur_dll_opid = get_ddl_applier_progress(true);
	MYSQL_CONN *conn = get_metadata_cluster_conn(false);

	StringInfoData str;
	initStringInfo(&str);

	struct timeval tv;
	gettimeofday(&tv, NULL);
	g_remote_ddl_trans->xa_txnid = MakeTopTxnName(tv.tv_usec, tv.tv_sec);

	/**
	 *  Create xa transaction to log the ddl log event into metaserver.
	 */
	appendStringInfo(&str,
					 "set transaction_isolation='repeatable-read'; XA START '%s'; set @my_opid = 0; ",
					 g_remote_ddl_trans->xa_txnid);

	ListCell *lc;
	uint64_t txnid = GetCurrentGlobalTransactionId();
	foreach (lc, g_remote_ddl_trans->ddl_context_list)
	{
		Remote_ddl_context *rcontext = (Remote_ddl_context *)lfirst(lc);
		DDLLogContext *context = rcontext->ddllog_context;
		if (!context)
			continue;
		make_append_ddl_log_entry_procedure(&str, GetClusterName2(),
											context->db,
											context->schema,
											"none",
											context->user,
											context->search_path,
											context->object,
											DDL_ObjTypeNames[context->objtype],
											DDL_OP_TypeNames[context->optype],
											cur_dll_opid,
											context->query,
											context->sql_storage,
											context->shardid,
											comp_node_id, 
											txnid,
											"@my_opid");
	}
	appendStringInfo(&str,
					 "XA END '%s'; XA PREPARE '%s'; select @my_opid",
					 g_remote_ddl_trans->xa_txnid,
					 g_remote_ddl_trans->xa_txnid);

	unsigned long res = 0;
	PG_TRY();
	{

		bool done = !send_stmt_to_cluster_meta(conn, str.data, str.len, CMD_SELECT, false);

		if (done)
		{
			Assert(conn->result);
			MYSQL_ROW row = mysql_fetch_row(conn->result);
			char *endptr = NULL;
			if (row)
			{
				Assert(row[0] != 0);
				res = strtoul(row[0], &endptr, 10);
			}
			else
			{
				free_metadata_cluster_result(conn);
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Kunlun-db: log_ddl_op() failed to call " KUNLUN_METADATA_DBNAME ".append_ddl_log_entry(), no op-id got.")));
			}

			free_metadata_cluster_result(conn);
			pfree(str.data);
			DEBUG_INJECT_IF("ddl_txn_crash_after_send_ddl_log_to_meta_and_prepare", DBUG_SUICIDE(););
		}
	}
	PG_CATCH();
	{
		char rbstmt[256];
		int ret = snprintf(rbstmt, sizeof(rbstmt), "XA END '%s'", g_remote_ddl_trans->xa_txnid);
		/*
		  We don't know whether XA END has been executed in above stmts, try
		  executing it here and catch all exceptions but don't throw any. If
		  other errors happened and XA END is really needed, XA ROLLBACK will
		  be able to abort the txn and network/communication/mysql errors will
		  be caught below and handled.
		*/
		PG_TRY();
		{
			send_stmt_to_cluster_meta(conn, rbstmt, ret, CMD_UTILITY, false);
		}
		PG_CATCH();
		{
		}
		PG_END_TRY();
		/*
		 * Rollback the XA txn. this can't be generally done in send_stmt_to_cluster_meta()
		 * because in other situations thant this one, client needs to send rollback stmt
		 * when he receives the error.
		 * */
		ret = snprintf(rbstmt, sizeof(rbstmt), "XA ROLLBACK '%s'", g_remote_ddl_trans->xa_txnid);
		PG_TRY();
		{
			send_stmt_to_cluster_meta(conn, rbstmt, ret, CMD_UTILITY, false);
		}
		PG_CATCH();
		{
			// make sure cluster_mgr can takeover and abort it.
			disconnect_metadata_shard();
			PG_RE_THROW();
		}
		PG_END_TRY();

		// if metadata shard server&connection is perfectly OK, we got some
		// other types of error and it will be rethrown here. --- this will
		// never be reached but let's leave it here just in case above code
		// changes in future causing error stack filled.
		PG_RE_THROW();
	}
	PG_END_TRY();

	return res;
}

/**
 * @brief Commit the transcation which log the ddl information to the meta server.
 *
 */
void log_ddl_commit(void)
{
	MYSQL_CONN *conn = get_metadata_cluster_conn(false);
	char rbstmt[256];

	int ret = snprintf(rbstmt, sizeof(rbstmt), "XA COMMIT '%s'", g_remote_ddl_trans->xa_txnid);
	PG_TRY();
	{
		send_stmt_to_cluster_meta(conn, rbstmt, ret, CMD_UTILITY, false);
	}
	PG_CATCH();
	{
		// make sure cluster_mgr can takeover and abort it.
		disconnect_metadata_shard();
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/**
 * @brief Rollback the prepared transaction which log the ddl information to the meta server
 *
 */
void log_ddl_rollback(void)
{
	MYSQL_CONN *conn = get_metadata_cluster_conn(false);
	char rbstmt[256];

	int ret = snprintf(rbstmt, sizeof(rbstmt), "XA ROLLBACK '%s'; ", g_remote_ddl_trans->xa_txnid);
	PG_TRY();
	{
		send_stmt_to_cluster_meta(conn, rbstmt, ret, CMD_UTILITY, false);
	}
	PG_CATCH();
	{
		// make sure cluster_mgr can takeover and abort it.
		disconnect_metadata_shard();
		PG_RE_THROW();
	}
	PG_END_TRY();
}

bool is_ddl_query(Node *node)
{
	switch (nodeTag(node))
	{
	case T_AlterCollationStmt:
	case T_AlterDatabaseSetStmt:
	case T_AlterDatabaseStmt:
	case T_AlterDefaultPrivilegesStmt:
	case T_AlterDomainStmt:
	case T_AlterEnumStmt:
	case T_AlterEventTrigStmt:
	case T_AlterExtensionContentsStmt:
	case T_AlterExtensionStmt:
	case T_AlterFdwStmt:
	case T_AlterForeignServerStmt:
	case T_AlterFunctionStmt:
	case T_AlterObjectDependsStmt:
	case T_AlterObjectSchemaStmt:
	case T_AlterOpFamilyStmt:
	case T_AlterOperatorStmt:
	case T_AlterOwnerStmt:
	case T_AlterPolicyStmt:
	case T_AlterPublicationStmt:
	// case T_AlterRoleSetStmt:
	case T_AlterRoleStmt:
	case T_AlterSeqStmt:
	case T_AlterSubscriptionStmt:
	case T_AlterSystemStmt:
	case T_AlterTSConfigurationStmt:
	case T_AlterTSDictionaryStmt:
	case T_AlterTableMoveAllStmt:
	case T_AlterTableSpaceOptionsStmt:
	case T_AlterTableStmt:
	case T_AlterUserMappingStmt:
	case T_ClusterStmt:
	case T_CommentStmt:
	case T_CompositeTypeStmt:
	case T_CreateAmStmt:
	case T_CreateCastStmt:
	case T_CreateConversionStmt:
	case T_CreateDomainStmt:
	case T_CreateEnumStmt:
	case T_CreateEventTrigStmt:
	case T_CreateExtensionStmt:
	case T_CreateFdwStmt:
	case T_CreateForeignServerStmt:
	case T_CreateForeignTableStmt:
	case T_CreateFunctionStmt:
	case T_CreateOpClassStmt:
	case T_CreateOpFamilyStmt:
	case T_CreatePLangStmt:
	case T_CreatePolicyStmt:
	case T_CreatePublicationStmt:
	case T_CreateRangeStmt:
	case T_CreateRoleStmt:
	case T_CreateSchemaStmt:
	case T_CreateSeqStmt:
	case T_CreateStatsStmt:
	case T_CreateStmt:
	case T_CreateSubscriptionStmt:
	case T_CreateTableAsStmt:
	case T_CreateTableSpaceStmt:
	case T_CreateTransformStmt:
	case T_CreateTrigStmt:
	case T_CreateUserMappingStmt:
	case T_CreatedbStmt:
	case T_DefineStmt:
	case T_DropOwnedStmt:
	case T_DropRoleStmt:
	case T_DropStmt:
	case T_DropSubscriptionStmt:
	case T_DropTableSpaceStmt:
	case T_DropUserMappingStmt:
	case T_DropdbStmt:
	case T_GrantRoleStmt:
	case T_GrantStmt:
	case T_ImportForeignSchemaStmt:
	case T_IndexStmt:
	case T_ReassignOwnedStmt:
	case T_RefreshMatViewStmt:
	case T_RenameStmt:
	case T_ReindexStmt:
	case T_ReplicaIdentityStmt:
	case T_RuleStmt:
	case T_SecLabelStmt:
	case T_TruncateStmt:
	case T_ViewStmt:
	case T_VacuumStmt:
		return true;
	default:
		return false;
	}
}
