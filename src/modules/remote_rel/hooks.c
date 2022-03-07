/*-------------------------------------------------------------------------
 *
 * hook.c
 *
 * Entrypoints of the hooks in PostgreSQL, and dispatches the callbacks.
 *
 *
 * Copyright (c) 2021-2022 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/remote_meta.h"
#include "access/reloptions.h"
#include "access/htup_details.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_database.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "commands/sequence.h"
#include "miscadmin.h"
#include "tcop/utility.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/queryenvironment.h"
#include "utils/syscache.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "storage/ipc.h"

#include "common.h"
#include "log_utils.h"
#include "ddl_logger.h"
#include "ddl_applier.h"
#include "remote_ddl.h"
#include "sequence_service.h"
#include "pgstat.h"

PG_MODULE_MAGIC;

/*
 * Declarations
 */
void		_PG_init(void);

Remote_ddl_trans remote_ddl_trans_data;
Remote_ddl_trans *g_remote_ddl_trans = NULL;
Remote_ddl_context *g_remote_ddl_context = NULL;
int apply_ddl_log_mode = 0;
int str_key_part_len = 64;
char last_remote_sql[1024] = {0};
char *last_remote_sql_ptr = last_remote_sql;

/*
 * Saved hook entries (if stacked)
 */
static ProcessUtility_hook_type next_ProcessUtility_hook = NULL;
static object_access_hook_type next_object_access_hook = NULL;
static shmem_startup_hook_type next_shmem_startup_hook = NULL;

static void
remote_make_sql_delayed()
{
	if (!g_remote_ddl_context)
		return;
	if (GetCurrentCommandId(false) <= g_remote_ddl_context->lastest_commandid)
		return;

	bool is_remote_object = false;
	ListCell *lc;
	foreach (lc, g_remote_ddl_context->delay_created_object)
	{
		ObjectAddress *object = (ObjectAddress *)lfirst(lc);
		if (object->classId == DatabaseRelationId)
		{
			remote_create_database(get_database_name(object->objectId));
			is_remote_object = true;
		}
		else if (object->classId == NamespaceRelationId)
		{
			remote_create_schema(get_namespace_name(object->objectId));
			is_remote_object = true;
		}
		else if (object->classId == RelationRelationId)
		{
			Relation rel = relation_open(object->objectId, NoLock);
			if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP)
			{
				is_remote_object = true;
				if (rel->rd_rel->relkind == RELKIND_RELATION && object->objectSubId > 0)
					remote_alter_column(rel, object->objectSubId, OAT_POST_CREATE);
				else if (rel->rd_rel->relkind == RELKIND_RELATION)
					remote_create_table(rel);
				else if (rel->rd_rel->relkind == RELKIND_INDEX)
					remote_add_index(rel);
				else if (rel->rd_rel->relkind == RELKIND_SEQUENCE)
					remote_create_sequence(rel);
				else
					is_remote_object = false;
			}
			relation_close(rel, NoLock);
		}

		/* Mysql not support ddl in explict transaction */
		if (g_remote_ddl_trans->explict_txn && is_remote_object)
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("Can not execute such DDL statements in an explicit transaction in kunlun-db.")));
	}
	list_free(g_remote_ddl_context->delay_created_object);
	g_remote_ddl_context->delay_created_object = NIL;
}

static void
remoteddl_object_access(ObjectAccessType access,
						Oid classId,
						Oid objectId,
						int subId,
						void *arg)
{
	if (next_object_access_hook)
		(*next_object_access_hook)(access, classId, objectId, subId, arg);

	/**
	 *  Only generate remote sql under postmaster, but still check:
	 *  1、invalidate sequence cache when applying ddl log event;
	 *  2、reject to create large objects;
	 */
	if (!remoteddl_top_stmt())
	{
		if (access == OAT_DROP && classId == DatabaseRelationId)
		{
			invalidate_seq_shared_cache(objectId, InvalidOid, true);
		}
		else if (classId == RelationRelationId &&
				 (access == OAT_DROP || access == OAT_POST_ALTER))
		{
			Relation rel = relation_open(objectId, NoLock);
			if (rel->rd_rel->relkind == RELKIND_SEQUENCE)
			{
				invalidate_seq_shared_cache(MyDatabaseId, objectId, access == OAT_DROP);
			}
			relation_close(rel, NoLock);
		}
		/* Reject to create large objects */
		if (access == OAT_POST_CREATE && classId == LargeObjectRelationId)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("kunlun-db: large object is not supported.")));
		}
		return;
	}

	/* Ignore system object */
	if (objectId < FirstNormalObjectId)
		return;

	/**
	 * Make remote sql for the newly created object if the system catalog is visible now.
	 * And delay the creationg as late as possible. 
	 */
	if (access == OAT_DROP || access == OAT_POST_ALTER)
	{
		remote_make_sql_delayed();
	}

	ObjectAddress *object = NULL;
	/**
	 * track the created/modified object during ddl, we need these information to
	 * generate remote ddl && log sql. For example, when creating indics for each
	 * leaf table of partitioned table, we need these information to find out
	 * which index is the new created index and what the name the index is .
	 */
	if (access == OAT_POST_CREATE || access == OAT_DROP || access == OAT_POST_ALTER)
	{
		object = (ObjectAddress *)palloc(sizeof(ObjectAddress));
		object->classId = classId;
		object->objectId = objectId;
		object->objectSubId = subId;
		remoteddl_enque_access_object(object, access);
	}
	bool is_remote_object = false;
	switch (access)
	{
	case OAT_POST_CREATE:
	{
		if (classId == LargeObjectRelationId)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("kunlun-db: large object is not supported.")));
		}
		/**
		 * Delay the create of remote sql as the system catalog may not be ready now.
		 */
		g_remote_ddl_context->lastest_commandid = GetCurrentCommandId(false);
		g_remote_ddl_context->delay_created_object =
			lappend(g_remote_ddl_context->delay_created_object, object);
		break;
	}
	case OAT_DROP:
	{
		if (classId == DatabaseRelationId)
		{
			/**
			 * The database cannot be dropped here because a mutex is already held for
			 * this database and we still need to connect to it to enumerate the
			 *  schemas in it, which would cause a deadlock.
			 */
			// remote_drop_database(get_database_name(objectId));
			/* Remove all the sequence entry from cache */
			invalidate_seq_shared_cache(objectId, InvalidOid, true);
			is_remote_object = true;
		}
		else if (classId == NamespaceRelationId)
		{
			remote_drop_schema(get_namespace_name(objectId));
			is_remote_object = true;
		}
		else if (classId == RelationRelationId)
		{
			Relation rel = relation_open(objectId, NoLock);
			/**
			 * We cannot generate remote ddl query for these deleted object
			 * in remoteddl_utility_command(), because the relation of the
			 * object deleted cannot be accessed there.
			 */
			if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP)
			{
				is_remote_object = true;
				switch (rel->rd_rel->relkind)
				{
				case RELKIND_RELATION:
					/* if subid > 0, means drop attribute */
					if (subId == 0)
						remote_drop_table(rel);
					else
						remote_alter_column(rel, subId, OAT_DROP);
					break;
				case RELKIND_INDEX:
					remote_drop_index(rel);
					break;
				case RELKIND_SEQUENCE:
				{
					remote_drop_sequence(rel);
					/* Remove the sequence cache entries*/
					invalidate_seq_shared_cache(MyDatabaseId, objectId, true);
					break;
				}
				default:
					is_remote_object = false;
					break;
				}
			}
			relation_close(rel, NoLock);
		}
		break;
	}
	case OAT_POST_ALTER:
	{
		/* modify column defintion */
		if (classId == RelationRelationId)
		{
			Relation rel = relation_open(objectId, NoLock);
			if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP)
			{
				is_remote_object = true;
				switch (rel->rd_rel->relkind)
				{
				case RELKIND_RELATION:
					if (subId > 0)
						remote_alter_column(rel, subId, OAT_POST_ALTER);
					else
						remote_alter_table(rel);
					break;
				case RELKIND_INDEX:
					remote_alter_index(rel);
					break;
				case RELKIND_SEQUENCE:
				{
					remote_alter_sequence(rel);

					/* Mark the sequence cache entry should be reloaded */
					invalidate_seq_shared_cache(MyDatabaseId, objectId, false);
					break;
				}
				default:
					is_remote_object = false;
					break;
				}
			}
			relation_close(rel, NoLock);
		}
		else if (classId == TypeRelationId)
		{
			remote_alter_type(objectId);
		}
		break;
	}
	default:
		break;
	}

	/* Mysql not support ddl in explict transaction */
	if (g_remote_ddl_trans->explict_txn && is_remote_object)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Can not execute such DDL statements in an explicit transaction in kunlun-db.")));
}

static void
remoteddl_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
								 SubTransactionId parentSubid, void *arg)
{
	if (event == SUBXACT_EVENT_ABORT_SUB && g_remote_ddl_trans)
		remoteddl_rollback_to(mySubid);
}

static void
remoteddl_xact_callback(XactEvent event, void *arg)
{
	if (!g_remote_ddl_trans)
		return;

	if (event == XACT_EVENT_ABORT || event == XACT_EVENT_COMMIT)
	{
		ddl_log_release_lock();
		remoteddl_free_trans();
		return;
	}

	if (event != XACT_EVENT_PRE_COMMIT && event != XACT_EVENT_PARALLEL_ABORT)
		return;

	PG_TRY();
	{
		if (event == XACT_EVENT_PRE_COMMIT && !log_ddl_skip())
		{
			/* preapre a transaction to log the ddl into the meta server */
			uint64_t logid = log_ddl_prepare();

			PG_TRY();
			{
				/** do ddl on the remote storage nodes.
				 * (1) if anything wrong, who rollback the prepared transaction ?
				 * (2) how to rollback the changed made in storage nodes?
				 */
				execute_all_remote_ddl();
			}
			PG_CATCH();
			{
				log_ddl_rollback();
				PG_RE_THROW();
			}
			PG_END_TRY();

			update_ddl_applier_progress(logid);

			log_ddl_commit();
		}
	}
	PG_CATCH();
	{
		remoteddl_free_trans();
		ddl_log_release_lock();
		PG_RE_THROW();
	}
	PG_END_TRY();

	remoteddl_free_trans();
	ddl_log_release_lock();
}

extern bool skip_tidsync;
/**
 * remoteddl_utility_command
 *
 * It tries to rough-grained control on utility commands; some of them can
 * break whole of the things if nefarious user would use.
 */
static void
remoteddl_utility_command(PlannedStmt *pstmt,
						const char *queryString,
						ProcessUtilityContext context,
						ParamListInfo params,
						QueryEnvironment *queryEnv,
						DestReceiver *dest,
						char *completionTag)
{
	static bool first = true;
	static int deep = 0;
	Node	   *parsetree = pstmt->utilityStmt;

	if (is_ddl_query(parsetree))
	{
		if (!enable_remote_ddl() || skip_tidsync)
		{
			/* do nothing */
		}
		else if (deep == 0)
		{
			if (!g_remote_ddl_trans)
			{
				/* Cache up the latest meta before holding the lock */
				catch_up_latest_meta();
				
				/* Get global lock to serialize ddls between CNs */
				ddl_log_get_lock();

				/* Try again to cache up the latest meta after holding the lock  */
				catch_up_latest_meta();

				pgstat_report_activity(STATE_RUNNING, queryString);
			}
			else
			{
				/*TODO: check the ddl lock is still hold by ourself */
			}

			/*Alloc ddl context for current ddl statement */
			remoteddl_alloc_context(parsetree);

			if (first)
			{
				first = false;
				RegisterXactCallback(remoteddl_xact_callback, NULL);
				RegisterSubXactCallback(remoteddl_subxact_callback, NULL);
			}
		}
		else if (remoteddl_top_stmt() == 0)
		{
			/* Do not support execute ddl in non ddl stmts */
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("Kunlun-db: execute ddl in non-ddl stmts is not supported.")));
		}

		/**
		 * Pretend to be a top-level interactive command, even though 
		 * we replay  the ddl log event in a function.
		 */
		if (apply_ddl_log_mode && deep == 0 && !IsExplicitTxn())
		{
			context = PROCESS_UTILITY_TOPLEVEL;
		}
	}

	++ deep;
	PG_TRY();
	{
		if (g_remote_ddl_trans)
		{
			if (nodeTag(parsetree) == T_DropdbStmt)
			{
				/* Not support ddl in explict transaction */
				if (g_remote_ddl_trans->explict_txn)
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("Can not execute such DDL statements in an explicit transaction in kunlun-db.")));
				
				DropdbStmt *stmt = (DropdbStmt *)parsetree;
				Oid dbid = get_database_oid(stmt->dbname, true);
				if (dbid != InvalidOid)
				{
					/* Notify the applier to release the connection to the database*/
					notify_applier_dropped_database(dbid);
					
					/**
					 * We should find out the schema in the database before the database is completely deleted, 
					 * so that the corresponding data in the storage can be completely cleaned up (each schema 
					 * corresponds to a database on the storage node).
					 */
					remote_drop_database(stmt->dbname);
				}
			}

			/* Check & make ddl log event before normal handle */
			pre_handle_ddl(parsetree, queryString);
		}

		if (next_ProcessUtility_hook)
			(*next_ProcessUtility_hook)(pstmt, queryString,
										context, params, queryEnv,
										dest, completionTag);
		else
			standard_ProcessUtility(pstmt, queryString,
									context, params, queryEnv,
									dest, completionTag);

		if (g_remote_ddl_trans)
		{
			/**
			 * Make remote sql for the newly created object,
			 * the system catalog for these object is visible now
			 */
			remote_make_sql_delayed();

			/* Check & make ddl log event after normal handle */
			post_handle_ddl(parsetree, queryString);
		}
	}
	PG_CATCH();
	{
		--deep;

		/* mark the end of ddl */
		if (deep == 0)
			remoteddl_end_context(false);

		PG_RE_THROW();
	}
	PG_END_TRY();
	-- deep;

	if (deep == 0)
	{
		/* add extra information to the ddl log event */
		log_ddl_add_extra();

		/* Mark the end of the ddl statement */
		remoteddl_end_context(true);
	}
}

static void
assign_apply_ddl_log_mode(int newval, void *extra)
{
	apply_ddl_log_mode = newval;
}

static void
assign_str_key_part_len(int newval, void *extra)
{
	str_key_part_len = newval;
}

static char*
show_last_remote_sql()
{
	return last_remote_sql;
}

static int64_t
remoteddl_nextval_hook(Relation seqrel, bool *finished)
{
	if (seqrel->rd_rel->relkind != RELKIND_SEQUENCE ||
		seqrel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
	{
		*finished = false;
		return 0;
	}
	*finished = true;
	return remote_fetch_nextval(seqrel);
}

static void
remoteddl_setval_hook(Relation seqrel, int64_t next, bool called, bool *finished)
{
	if (seqrel->rd_rel->relkind != RELKIND_SEQUENCE ||
		seqrel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
	{
		*finished = false;
		return ;
	}
	*finished = true;
	remote_setval(seqrel, next, called);
}

static void
remoteddl_shm_startup(void)
{
	if (next_shmem_startup_hook)
		next_shmem_startup_hook();
	create_applier_message_queue(true);
	create_remote_sequence_shmmem();
}

/*
 * Module load/unload callback
 */
void
_PG_init(void)
{
	/*
	 * We allow to load the SE-PostgreSQL module on single-user-mode or
	 * shared_preload_libraries settings only.
	 */
	if (IsUnderPostmaster)
		ereport(WARNING,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("remoteddl must be loaded via shared_preload_libraries")));
	
	// if (!process_shared_preload_libraries_in_progress)
	//	return;

	/* get/set the ddl apply mode*/
	DefineCustomIntVariable("remote_rel.apply_ddl_log_mode",
							"Only apply ddl log from meta server.",
							NULL,
							&apply_ddl_log_mode,
							0, /* boost value */
							0, /*min value*/
							1, /*max value*/
							PGC_SUSET,
							0,
							NULL,
							assign_apply_ddl_log_mode,
							NULL);

	/* get/set str_key_part_len */
	DefineCustomIntVariable("remote_rel.str_key_part_len",
							"String key-part length suffix used in DDL statements sent to storage shard.",
							NULL,
							&str_key_part_len,
							64, /* boost value */
							1, /*min value*/
							64*1024, /*max value*/
							PGC_SUSET,
							0,
							NULL,
							assign_str_key_part_len,
							NULL);
	
	/* get/set remote_stmt_str */
	DefineCustomStringVariable("remote_rel.last_remote_sql",
							"The last remote sql statement generated for storage nodes",
							NULL,
							&last_remote_sql_ptr,
							NULL, /* boost value */
							PGC_INTERNAL,
							0,
							NULL,
							NULL,
							show_last_remote_sql);
	
	RequestNamedLWLockTranche("DDL applier message queue lock", 1);
	RequestNamedLWLockTranche("Remote Sequence request queue lock", 1);
	
	/* ProcessUtility hook */
	next_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = remoteddl_utility_command;

	/* Object access hook*/
	next_object_access_hook = object_access_hook;
	object_access_hook = remoteddl_object_access;

	/* Nextvalue hook */
	Nextval_hook = remoteddl_nextval_hook;
	Setval_hook = remoteddl_setval_hook;
	
	next_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = remoteddl_shm_startup;
	
	BackgroundWorker log_applier_main;
	/* set up common data for all our workers */
	memset(&log_applier_main, 0, sizeof(log_applier_main));
	log_applier_main.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	log_applier_main.bgw_start_time = BgWorkerStart_RecoveryFinished;
	log_applier_main.bgw_restart_time = 0;
	log_applier_main.bgw_notify_pid = 0;
	log_applier_main.bgw_main_arg = 0;
	sprintf(log_applier_main.bgw_library_name, "remote_rel");
	sprintf(log_applier_main.bgw_function_name, "ddl_applier_serivce_main");
	snprintf(log_applier_main.bgw_name, BGW_MAXLEN, "ddl log event applier service");
	snprintf(log_applier_main.bgw_type, BGW_MAXLEN, "ddl_log_applier");

	RegisterBackgroundWorker(&log_applier_main);
	
	BackgroundWorker seq_handler_main;
	/* set up common data for all our workers */
	memset(&seq_handler_main, 0, sizeof(seq_handler_main));
	seq_handler_main.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	seq_handler_main.bgw_start_time = BgWorkerStart_RecoveryFinished;
	seq_handler_main.bgw_restart_time = 0;
	seq_handler_main.bgw_notify_pid = 0;
	seq_handler_main.bgw_main_arg = 0;
	sprintf(seq_handler_main.bgw_library_name, "remote_rel");
	sprintf(seq_handler_main.bgw_function_name, "sequence_serivce_main");
	snprintf(seq_handler_main.bgw_name, BGW_MAXLEN, "sequence fetch serivce");
	snprintf(seq_handler_main.bgw_type, BGW_MAXLEN, "seq_fetch_service");

	RegisterBackgroundWorker(&seq_handler_main);
}
