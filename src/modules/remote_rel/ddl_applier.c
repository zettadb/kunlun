/*-------------------------------------------------------------------------
 *
 * ddl_applier.c
 *
 *	The implementation of ddl log event applier
 *
 *
 * Copyright (c) 2021-2022 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 *-------------------------------------------------------------------------
 */
#include <sys/types.h>
#include <signal.h>

#include "postgres.h"
#include "c.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_cluster_meta.h"
#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/xidsender.h"
#include "postmaster/postmaster.h"
#include "sharding/mysql/mysql.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "storage/shm_toc.h"
#include "storage/shm_mq.h"
#include "sharding/sharding.h"

#include "common.h"
#include "log_utils.h"
#include "ddl_applier.h"

PG_FUNCTION_INFO_V1(apply_log_wrapper);

/**
 * @brief Applier message queue that informs which database will be dropped
 */
typedef struct Applier_mq_context
{
	pid_t applier_pid;
	Oid *queue;
	Size queue_size;
	Size head;
} Applier_mq_context;

static Applier_mq_context *applier_mq_context;
static LWLock *applier_mq_lock = NULL;
static char applier_user[NAMEDATALEN+1];

typedef struct Applier_conn
{
	Oid dbid;
	PGconn *conn;
}Applier_conn;
static Applier_conn applier_conns[MAX_DBS_ALLOWED];

/* The content of ddl log */
typedef struct DDL_log_event
{
	uint64_t id;
	uint64_t conflictId;
	DDL_OP_Types optype;
	DDL_ObjTypes objtype;
	char *dbname;
	char *objname;
	char *sqlsrc;
	char *user;
	char *role;
	char *searchPath;
} DDL_log_event;

typedef struct DDL_log_read_context
{
	uint64_t startpos;
	List *eventQue;
	MemoryContext memContext;
} DDL_log_read_context;

static DDL_OP_Types DDL_OP_TypeNames_to_enum(const char *typname)
{
  for (int i = 0; i < sizeof(DDL_OP_TypeNames) / sizeof(char *); i++)
    if (strcmp(typname, DDL_OP_TypeNames[i]) == 0)
      return (DDL_OP_Types)i;

  return DDL_OP_Type_Invalid;
}

static DDL_ObjTypes DDL_ObjTypeNames_to_enum(const char *objname)
{
  for (int i = 0; i < sizeof(DDL_ObjTypeNames) / sizeof(char *); i++)
    if (strcmp(objname, DDL_ObjTypeNames[i]) == 0)
      return (DDL_ObjTypes)i;
  return DDL_ObjType_Invalid;
}

static void free_ddl_log_event(DDL_log_event *e)
{
	pfree(e->objname);
	pfree(e->sqlsrc);
	pfree(e->user);
	pfree(e->role);
	pfree(e->searchPath);
	pfree(e);
}

static void Debug_ddl_log_event(DDL_log_event *e)
{
	elog(DEBUG1, "applier: log event, id=%lu, conflict=%lu, objname=%s, user=%s, role=%s, search_path=%s, sqlsrc=%s",
			e->id, e->conflictId, e->objname, e->user, e->role, e->searchPath, e->sqlsrc);
}

static void Init_ddl_log_read_context(DDL_log_read_context *context, uint64_t startpos)
{
	context->startpos = startpos;
	context->eventQue = NIL;
	context->memContext =
		AllocSetContextCreate(TopMemoryContext,
							  "DDL log event queue memory context",
							  ALLOCSET_DEFAULT_SIZES);
}

static DDL_log_event *Peek_ddl_log_event(DDL_log_read_context *context)
{
	bool in_txn = IsTransactionState();

	/* Peek event from queue */
	if (list_head(context->eventQue))
	{
		return linitial(context->eventQue);
	}
	/* Once all previously fetched log events are consumed, reset the memory context. */
	MemoryContextReset(context->memContext);

	MemoryContext savedContext =
		MemoryContextSwitchTo(context->memContext);

	DDL_log_event *event = NULL;
	if (context->startpos == ULONG_MAX)
		goto end;

	PG_TRY();
	do {
		/* Pull log event from meta server */
		MYSQL_CONN *conn = get_metadata_cluster_conn(true);
		char stmt[512];
		context->startpos = get_ddl_applier_progress(false);
		int ret = snprintf(stmt, sizeof(stmt),
				"select id, db_name, objname, optype, objtype, sql_src, user_name, role_name, search_path from " KUNLUN_METADATA_DBNAME ".ddl_ops_log_%s "
				"where id > %lu order by id limit 500;",
				GetClusterName2(), context->startpos);
		Assert(ret < sizeof(stmt));

		if (send_stmt_to_cluster_meta(conn, stmt, ret, CMD_SELECT, true /* is bg */))
		{
			break;
		}

		MYSQL_ROW row;
		uint64_t progress = get_ddl_applier_progress(true);
		while ((row = mysql_fetch_row(conn->result)))
		{
			uint64_t id = strtoul(row[0], NULL, 10);
			if (id > progress)
			{
				DDL_log_event *e = palloc(sizeof(DDL_log_event));
				e->id = id;
				e->dbname = pstrdup(row[1]);
				e->objname = pstrdup(row[2]);
				e->optype = DDL_OP_TypeNames_to_enum(row[3]);
				e->objtype = DDL_ObjTypeNames_to_enum(row[4]);
				e->sqlsrc = pstrdup(row[5]);
				e->user = pstrdup(row[6]);
				e->role = pstrdup(row[7]);
				e->searchPath = pstrdup(row[8]);

				context->eventQue = lappend(context->eventQue, e);
			}
		}

		/*
		 * We are using mysql_use_result() in send_stmt_to_cluster_meta() so when
		 * row is NULL need to see if there is an error.
		 * */
		if (mysql_errno(&conn->conn))
			handle_metadata_cluster_error(conn, false);
		else
			free_metadata_cluster_result(conn);

		if (list_head(context->eventQue))
			event = linitial(context->eventQue);
	}while (0);
	PG_CATCH();
	{
		EmitErrorReport();
		FlushErrorState();

		/* Make sure the transactions that were created temporarily in a corner are cleaned up.*/
		if (!in_txn && IsTransactionState())
			AbortCurrentTransaction();
	}
	PG_END_TRY();

end:
	MemoryContextSwitchTo(savedContext);

	return event;
}

static void pop_ddl_log_event(DDL_log_read_context *context)
{
	Assert(list_head(context->eventQue));
	DDL_log_event *e = linitial(context->eventQue);
	context->startpos = e->id;
	free_ddl_log_event(e);
	context->eventQue = list_delete_first(context->eventQue);
}

static int switch_authorization(const char *user, const char *role)
{
	int ret = -2;
	bool end_txn = false;

	/* Start transaction */
	if (!IsTransactionState())
	{
		StartTransactionCommand();
		end_txn = true;
	}

	/* Look up the user */
	HeapTuple roleTup = SearchSysCache1(AUTHNAME, PointerGetDatum(user));
	if (!HeapTupleIsValid(roleTup))
	{
		elog(WARNING, "DDL log event applier cannot find user '%s'", user);
		goto end;
	}

	Oid userid = HeapTupleGetOid(roleTup);
	bool is_superuser = ((Form_pg_authid)GETSTRUCT(roleTup))->rolsuper;

	ReleaseSysCache(roleTup);

	/* Lookup the role */
	Oid roleid;
	bool is_superrole;
	if (!role || strcmp(role, "none") == 0)
	{
		roleid = InvalidOid;
		is_superrole = false;
	}
	else
	{
		/* Look up the username */
		roleTup = SearchSysCache1(AUTHNAME, PointerGetDatum(role));
		if (!HeapTupleIsValid(roleTup))
		{
			elog(ERROR, "DDL log event applier failed to find role \"%s\"", role);
			goto end;
		}

		roleid = HeapTupleGetOid(roleTup);
		is_superrole = ((Form_pg_authid)GETSTRUCT(roleTup))->rolsuper;

		ReleaseSysCache(roleTup);

		/* Check if the user is member of the role */
		if (!is_member_of_role(userid, roleid))
		{
			/*
			 * Other sessions may have removed the current user from the role,
			 * it doesn't matter, the applier has ignore the acl check, and the
			 * user/role setting here is just to set the owner of table/view correctly,
			 * so just print warning and continue.
			 */
			elog(WARNING, "DDL log event applier found user '%s' is not member of role '%s'", user, role);
		}
	}

	/* Set session authorization to corresponding user */
	PG_TRY();
	{
		SetSessionAuthorization(userid, is_superuser);
		SetCurrentRoleId(roleid, is_superrole);
		ret = 0;
	}
	PG_CATCH();
	{
		elog(WARNING, "DDL log event applier failed to set session authorization '%s' and role '%s'", user, role);
	}
	PG_END_TRY();

end:
	/* Commit transaction */
	if (end_txn)
	{
		CommitTransactionCommand();
	}
	pgstat_report_activity(STATE_IDLE, NULL);

	return ret;
}
/**
 * @brief  parse the remap_shardid request, for example:
 *     {'1': '2', '2': '4' }
 *
 */
#define SKIP_SPACE(p) while(*p && isspace(*p)) ++p;
static void parse_remap_shardid_req(const char *req, List **pfrom, List **pto)
{
	char c;
	char *end;
	int shardid;
	bool iskey = true;
	List *from = NIL, *to = NIL;

	SKIP_SPACE(req);
	if (*req != '{')
		goto format_error;

	++req;
	while (*req)
	{
		SKIP_SPACE(req);
		c = 0;
		if (*req == '\'' || *req == '"')
		{
			c = *req;
			++req;
		}

		shardid = strtol(req, &end, 10);

		if (errno == ERANGE && (shardid == LONG_MAX || shardid == LONG_MIN) && shardid < 0)
			goto invalid_shardid;
		if (req == end)
			goto format_error;

		req = end;

		if (c)
		{
			if (*req != c)
				goto format_error;
			++req;
		}

		if (iskey)
		{
			from = lappend_oid(from, shardid);
			SKIP_SPACE(req);
			if (*req != ':')
				goto format_error;
			++req;
		}
		else
		{
			to = lappend_oid(to, shardid);
			SKIP_SPACE(req);
			/* end of request */
			if (*req == '}')
			{
				++req;
				SKIP_SPACE(req);
				if (*req != '\0')
					goto format_error;

				*pfrom = from;
				*pto = to;
				return;
			}

			if (*req != ',')
				goto format_error;
			++req;
		}

		iskey = !iskey;
	}

format_error:
	elog(ERROR, "The remap_shardid req is invalid");

invalid_shardid:
	elog(ERROR, "The shardid in remap_shardid req is invalid");
}

static int apply_log_internal(int id, const char *user, const char *role, const char *path, const char *op, const char *sql)
{
	bool end_txn = false;
	int ret = 0;

	/* Current state */
	pgstat_report_activity(STATE_RUNNING, "Applying ddl log records.");
	
	/* Saved the orignal User*/
	char *origUser = get_current_username();

	/* Switch user */
	switch_authorization(user, role);
	
	set_search_path(path);

	Assert(!IsTransactionBlock());

	if (!IsTransactionState())
	{
		end_txn = true;
		StartTransactionCommand();
	}

	if (strcasecmp(op, "remap_shardid") == 0)
	{
		List *from_shardids, *to_shardids;
		parse_remap_shardid_req(sql, &from_shardids, &to_shardids);
		change_cluster_shardids(from_shardids, to_shardids);
		ret = SPI_OK_UTILITY;
	}
	else
	{
		/* We can now execute queries via SPI */
		SPI_connect();
		ret = SPI_execute(sql, false, 0);
                SPI_finish();
	}

	PushActiveSnapshot(GetTransactionSnapshot());

	/* Switch back to the orignal user */	
	switch_authorization(origUser, NULL);

	/* Update local ddl progress */	
	if (ret == SPI_OK_UTILITY)
		update_ddl_applier_progress(id);

	PopActiveSnapshot();
	if (end_txn)
		CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
	return ret;
}

/*
 * Dynamically launch an SPI worker.
 */
Datum
apply_log_wrapper(PG_FUNCTION_ARGS)
{
	int64_t id = PG_GETARG_INT64(0);
	const char *user = text_to_cstring(PG_GETARG_TEXT_P(1));
	const char *role = text_to_cstring(PG_GETARG_TEXT_P(2));
	const char *path = text_to_cstring(PG_GETARG_TEXT_P(3));
	const char *op   = text_to_cstring(PG_GETARG_TEXT_P(4));
	const char *sql  = text_to_cstring(PG_GETARG_TEXT_P(5));

	int ret = apply_log_internal(id, user, role, path, op, sql);
	if (ret != SPI_OK_UTILITY) 
	{
		elog(ERROR, "failed to apply ddl log event, id:%lu, spi result: %d", id, ret);
	}
	
	PG_RETURN_INT32(0);
}


/**
 * @brief Get the applier connection object
 */
static PGconn* get_applier_connection(Oid dbid)
{
	static const char *sql =
		"SET REMOTE_REL.APPLY_DDL_LOG_MODE=1;"
		"SET LOCK_TIMEOUT = 10;"
		"CREATE OR REPLACE FUNCTION apply_log_wrapper(INT8, TEXT, TEXT, TEXT, TEXT, TEXT)"
		"  RETURNS INT4 STRICT AS '$libdir/remote_rel.so' LANGUAGE C;"
		"SET check_function_bodies=off;";

	Applier_conn *applier = NULL;
	int free_slot = -1;
	for (int i = 0; i < MAX_DBS_ALLOWED && !applier; ++i)
	{
		if (applier_conns[i].dbid == dbid)
			applier = &applier_conns[i];

		if (free_slot == -1 && applier_conns[i].dbid == InvalidOid)
			free_slot = i;
	}

	if (applier && PQstatus(applier->conn) == CONNECTION_OK)
	{
		return applier->conn;
	}

	if (!applier)
	{
		free_slot = (free_slot > 0) ? free_slot : 0;
		applier = &applier_conns[free_slot];
		applier->dbid = dbid;
	}

	if (applier->conn)
	{
		PQfinish(applier->conn);
		applier->conn = NULL;
	}

	{
		char conninfo[256];
		char *dbname = get_database_name(dbid);
		int len = snprintf(conninfo, sizeof(conninfo), "host=%s port=%d dbname=%s user=%s",
						   Unix_socket_directories, PostPortNumber, dbname, applier_user);
		Assert(len < sizeof(conninfo));

		applier->conn = PQconnectdb(conninfo);
		bool success = false;
		if (PQstatus(applier->conn) == CONNECTION_OK)
		{
			PGresult *res = PQexec(applier->conn, sql);
			if (!res)
			{
				elog(WARNING, "failed to initialize connection to '%s'", dbname);
			}
			else
			{
				if (PQresultStatus(res) != PGRES_COMMAND_OK)
					elog(WARNING, "failed to initialize connection to '%s': %s", dbname, PQresultErrorMessage(res));
				else
					success = true;
				PQclear(res);
			}
		}
		else
		{
			elog(WARNING, "failed to connect to database %s to get collect schema to deleted", dbname);
		}

		if (!success)
		{
			PQfinish(applier->conn);
			applier->dbid = InvalidOid;
			applier->conn = NULL;
		}
	}

	return applier->conn;
}

static void free_applier_connection(Oid dbid)
{
	for (int i=0; i<MAX_DBS_ALLOWED; ++i)
	{
		if (applier_conns[i].dbid == dbid)
		{
			PQfinish(applier_conns[i].conn);
			applier_conns[i].dbid = InvalidOid;
			applier_conns[i].conn = NULL;
			break;
		}
	}
}

  static void apply_ddl_log_for_database(Oid dbid, DDL_log_event *event)
{
	PGconn *conn = get_applier_connection(dbid);
	if (!conn)
	{
		/* TODO: check and resolve */
		elog(ERROR, "failed to apply event, because of missing of connection to %s", event->dbname);
	}

	StringInfoData sqlstr;
	initStringInfo(&sqlstr);
	char *user = PQescapeLiteral(conn, event->user, strlen(event->user));
	char *role = PQescapeLiteral(conn, event->role, strlen(event->role));
	char *path = PQescapeLiteral(conn, event->searchPath, strlen(event->searchPath));
	char *sql  = PQescapeLiteral(conn, event->sqlsrc, strlen(event->sqlsrc));
	const char *op = DDL_OP_TypeNames[event->optype];

	appendStringInfo(&sqlstr, "SELECT public.apply_log_wrapper(%lu, %s, %s, %s, '%s', %s)",
									 event->id, user, role, path, op, sql);

	free(user), free(role), free(path), free(sql);

	PGresult *res = PQexec(conn, sqlstr.data);
	if (!res)
	{
		elog(ERROR, "failed to send log event to applier, event_id:%lu, conn status:%d", event->id, PQstatus(conn));
	}

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		const char *error = pstrdup(PQresultErrorMessage(res));
		PQclear(res);
		elog(ERROR, "failed to execute log event, event_id:%lu, error:%s", event->id, error);
	}

	PQclear(res);
}

static void apply_ddl_log_local(Oid dbid, DDL_log_event *event)
{
	if (event->objtype == DDL_ObjType_db)
	{
		if (event->optype == DDL_OP_Type_drop)
		{
			/* Close connection to the database*/
			free_applier_connection(dbid);

			/* Check if database already dropped */
			if (dbid == InvalidOid)
				return;
		}
		if (event->optype == DDL_OP_Type_create)
		{
			/* Check if database already created */
			if (dbid != InvalidOid)
				return;
		}
	}

	const char *op = DDL_OP_TypeNames[event->optype];
	if (apply_log_internal(event->id,
						   event->user,
						   event->role,
						   event->searchPath,
						   op,
						   event->sqlsrc) != SPI_OK_UTILITY)
	{

		elog(ERROR, "failed to apply log event, id:%lu", event->id);
	}
}

static void dispatch_ddl_log_event(DDL_log_event *event)
{
	Oid dbid = InvalidOid;
	bool free_txn = false;
	if (!IsTransactionState())
	{
		free_txn = true;
		StartTransactionCommand();
	}
	dbid = get_database_oid(event->dbname, true);
	PG_TRY();
	{

		if (event->objtype == DDL_ObjType_db ||
			event->objtype == DDL_ObjType_user ||
			dbid == MyDatabaseId)
		{
			apply_ddl_log_local(dbid, event);
		}
		else
		{
			apply_ddl_log_for_database(dbid, event);
		}
	}
	PG_CATCH();
	{
		if (free_txn)
			AbortCurrentTransaction();
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (free_txn)
		CommitTransactionCommand();
}

void create_applier_message_queue(bool module_init)
{
	const Size queue_size = 128;
	bool found = false;

	Size size = sizeof(Applier_mq_context) + queue_size * sizeof(Oid);
	applier_mq_context =
		(Applier_mq_context *)ShmemInitStruct("ddl_applie_mq", size, &found);
	if (!found)
	{
		applier_mq_context->applier_pid = 0;
		applier_mq_context->queue = (Oid *)&applier_mq_context[1];
		applier_mq_context->queue_size = queue_size;
		applier_mq_context->head = 0;
		applier_mq_lock = &GetNamedLWLockTranche("DDL applier message queue lock")->lock;
	}
}

void notify_applier()
{
	if (applier_mq_context && applier_mq_context->applier_pid)
		kill(applier_mq_context->applier_pid, SIGUSR2);
}

bool notify_applier_dropped_database(Oid dbid)
{
	LWLockAcquire(applier_mq_lock, LW_EXCLUSIVE);
	Size pos = applier_mq_context->head;
	if (pos == applier_mq_context->queue_size)
	{
		LWLockRelease(applier_mq_lock);
		elog(INFO, "applier message queue is full, wait for a while and try again");
		return false;
	}

	applier_mq_context->queue[pos] = dbid;
	applier_mq_context->head += 1;
	LWLockRelease(applier_mq_lock);

	if (applier_mq_context->applier_pid)
		kill(applier_mq_context->applier_pid, SIGUSR2);

	return true;
}

static void handle_message_queue()
{
	List *list = NIL;
	LWLockAcquire(applier_mq_lock, LW_EXCLUSIVE);
	if (applier_mq_context->applier_pid != MyProcPid)
	{
		elog(WARNING, "ddl applier message queue's pid is changed ");
		applier_mq_context->applier_pid = MyProcPid;
	}
	
	for (Size pos = 0;
		 pos != applier_mq_context->head;
		 ++pos)
	{
		Size offset = pos % applier_mq_context->queue_size;
		list = lappend_oid(list, applier_mq_context->queue[offset]);
	}
	applier_mq_context->head = 0;
	LWLockRelease(applier_mq_lock);

	ListCell *lc;
	foreach(lc, list)
	{
		Oid dbid = lfirst_oid(lc);
		free_applier_connection(dbid);
	}
}

/* Signal handle */
static volatile sig_atomic_t got_sigterm = false;
static void
ddl_log_applier_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

static void
ddl_log_applier_siguser2(SIGNAL_ARGS)
{
	int save_errno = errno;
	SetLatch(MyLatch);
	errno = save_errno;
}

extern bool skip_top_level_check;

/* applier main loop */
void ddl_applier_service_main(void)
{
	pqsignal(SIGTERM, ddl_log_applier_sigterm);
	pqsignal(SIGUSR2, ddl_log_applier_siguser2);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection("postgres", NULL, 0);
	

	/* Set current user */
	char *curUser = get_current_username();
	strncpy(applier_user, curUser, sizeof(applier_user));
	pfree(curUser);

	apply_ddl_log_mode = 1;
	skip_top_level_check = true; /* allow execute create/drop db in transaction */
	memset(applier_conns, 0, sizeof(applier_conns));

	uint64_t startpos = get_ddl_applier_progress(false);
	DDL_log_read_context context;
	DDL_log_event *event;
	Init_ddl_log_read_context(&context, startpos);

	/*alloc memory context for loop */
	MemoryContext loopMemContext;
	loopMemContext =
		AllocSetContextCreate(TopMemoryContext,
							  "DDL log event queue memory context",
							  ALLOCSET_DEFAULT_SIZES);
	
	MemoryContext saved = MemoryContextSwitchTo(loopMemContext);
	
	while (!got_sigterm)
	{
		while (!got_sigterm && (event = Peek_ddl_log_event(&context)))
		{
			Debug_ddl_log_event(event);
			bool done = false;
			do
			{
				PG_TRY();
				{
					dispatch_ddl_log_event(event);
					done = true;
				}
				PG_CATCH();
				{
					EmitErrorReport();
					FlushErrorState();
					/* sleep a while, then retry */
					wait_latch(1000);

					/* maybe we force skip that event */
					if (event->id <= get_ddl_applier_progress(false))
						done = true;
				}
				PG_END_TRY();

			} while (!done && !got_sigterm);

			pop_ddl_log_event(&context);

			/* Free memory alloced in this loop */
			MemoryContextReset(loopMemContext);
		}

		if (got_sigterm)
			break;
		wait_latch(1000);
		handle_message_queue();

		/* Free memory alloced in this loop */
		MemoryContextReset(loopMemContext);
	}

	MemoryContextSwitchTo(saved);
}
