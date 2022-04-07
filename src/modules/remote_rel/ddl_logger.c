/*-------------------------------------------------------------------------
 *
 * ddl_logger.c
 *
 *	Generate the corresponding  ddl log event for client ddl query
 *
 * Copyright (c) 2021-2022 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 *-------------------------------------------------------------------------
 */
#include "ddl_logger.h"

#include "miscadmin.h"
#include "access/remote_meta.h"
#include "access/heapam.h"
#include "access/reloptions.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "access/sysattr.h"
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
#include "libpq/crypt.h"
#include "sharding/sharding_conn.h"
#include "sharding/cluster_meta.h"
#include "tcop/utility.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/queryenvironment.h"
#include "utils/syscache.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

#include "common.h"
#include "log_utils.h"
#include "remote_ddl.h"

static void log_create_db(CreatedbStmt *stmt, const char *query);
static void log_drop_db(DropdbStmt *stmt, const char *query);
static void log_create_schema(CreateSchemaStmt *stmt, const char *query);
static void log_create_stmt(CreateStmt *stmt, const char *query);
static void log_create_index(IndexStmt *stmt, const char *query);
static void log_create_sequence(CreateSeqStmt *stmt, const char *query);
static void log_drop_stmt(DropStmt *stmt, const char *query);
static void log_alter_table(AlterTableStmt *stmt, const char *query);
static void log_alter_seq(AlterSeqStmt *stmt, const char *query);

void log_create_db(CreatedbStmt *stmt, const char *query)
{
	log_ddl_add(DDL_OP_Type_create,
				DDL_ObjType_db,
				stmt->dbname,
				NULL,
				stmt->dbname,
				query,
				InvalidOid,
				NULL);
}

void log_drop_db(DropdbStmt *stmt, const char *query)
{
	log_ddl_add(DDL_OP_Type_drop,
				DDL_ObjType_db,
				stmt->dbname,
				NULL,
				stmt->dbname,
				query,
				InvalidOid, NULL);
}

void log_create_schema(CreateSchemaStmt *stmt, const char *query)
{
	Assert(list_length(stmt->schemaElts) == 0);
	log_ddl_add(DDL_OP_Type_create,
				DDL_ObjType_schema,
				get_database_name(MyDatabaseId),
				stmt->schemaname,
				stmt->schemaname,
				query,
				InvalidOid,
				NULL);
}

/* Rewrite create table query to add option "SHARD=N" to it */
static void
rewrite_create_create_query(CreateStmt *stmt, Relation relation, const char *queryString, StringInfo query)
{
	ListCell *lc;
	bool found = false;
	Oid shardid = relation->rd_rel->relshardid;
	if (shardid != InvalidOid)
	{
		foreach (lc, stmt->options)
		{
			DefElem *elem = lfirst_node(DefElem, lc);
			if(strcasecmp(elem->defname, "SHARD") == 0)
			{
				found = true;
				break;
			}
		}
	}

	if (found || shardid == InvalidOid)
	{
		appendStringInfo(query, "%s", queryString);
		return;
	}

	/* Rewrite query to add shard=N to it */
	if (stmt->opts_location == -1 ||
			list_length(stmt->options) == 0)
	{
		int len = strlen(queryString);
		while (len)
		{
			char c = queryString[len - 1];
			if (!isspace(c) && c != ';')
				break;
			--len;
		}
		appendStringInfo(query, "%.*s WITH (SHARD=%d)",
										 len, queryString, shardid);
	}
	else
	{
		appendStringInfo(query, "%.*s WITH (SHARD=%d, ",
										 stmt->opts_location, queryString, shardid);

		DefElem *elem = (DefElem *)linitial(stmt->options);
		int loc = elem->location;

		/* Change WITH/WITHOUT OIDS to WITH (OIDS=TURE/FALSE) */
		do
		{
			if (strcasecmp(elem->defname, "OIDS") != 0)
				break;
			const char *ptr = queryString + loc;
			if (strncasecmp(ptr, STRING_WITH_LEN("WITHOUT ")) == 0)
			{
				ptr += sizeof("WITHOUT");
				appendStringInfo(query, "oids=false) ");
			}
			else if (strncasecmp(ptr, STRING_WITH_LEN("WITH ")) == 0)
			{
				ptr += sizeof("WITH");
				appendStringInfo(query, "oids=true) ");
			}
			else
			{
				break;
			}
			while (isspace(*ptr))
				++ptr;
			ptr += sizeof("OIDS") - 1;
			loc = ptr - queryString;
		} while (0);

		appendStringInfo(query, "%s", queryString + loc);
	}
}

void log_create_stmt(CreateStmt *stmt, const char *query)
{
	/* Open the relation to get the shardid of it */
	RangeVar *relation = stmt->relation;
	Oid relid = RangeVarGetRelid(relation, NoLock, false);
	Relation rel = relation_open(relid, NoLock);
	Oid shardid = rel->rd_rel->relshardid;

	/* is temp table, do nothing */
	if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP)
	{
		StringInfoData sql;
		initStringInfo(&sql);
		
		rewrite_create_create_query(stmt, rel, query, &sql);
		
		log_ddl_add(DDL_OP_Type_create,
					DDL_ObjType_table,
					get_database_name(MyDatabaseId),
					get_namespace_name(rel->rd_rel->relnamespace),
					relation->relname,
					sql.data,
					shardid,
					NULL);
	}

	relation_close(rel, NoLock);
}

void log_create_index(IndexStmt *stmt, const char *query)
{
	RangeVar *relation = stmt->relation;
	Oid relid = RangeVarGetRelid(relation, NoLock, false);
	Relation rel = relation_open(relid, NoLock);
	
	if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP)
	{
		log_ddl_add(DDL_OP_Type_create,
					DDL_ObjType_index,
					get_database_name(MyDatabaseId),
					get_namespace_name(rel->rd_rel->relnamespace),
					relation->relname,
					query,
					InvalidOid,
					NULL);
	}

	relation_close(rel, NoLock);
}

void log_alter_table(AlterTableStmt *stmt, const char *query)
{
	RangeVar *relvar = stmt->relation;
	Oid relid = RangeVarGetRelid(relvar, NoLock, stmt->missing_ok);
	if (relid == InvalidOid)
		return;
	Relation rel = relation_open(relid, NoLock);
	if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP)
	{
		log_ddl_add(DDL_OP_Type_alter,
					DDL_ObjType_table,
					get_database_name(MyDatabaseId),
					get_namespace_name(rel->rd_rel->relnamespace),
					rel->rd_rel->relname.data,
					query,
					rel->rd_rel->relshardid,
					NULL);
	}
	relation_close(rel, NoLock);
}

void log_alter_seq(AlterSeqStmt *stmt, const char *query)
{
	RangeVar *relvar = stmt->sequence;
	Oid relid = RangeVarGetRelid(relvar, NoLock, stmt->missing_ok);
	if (relid == InvalidOid)
		return;
	Relation rel = relation_open(relid, NoLock);
	if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP)
	{
		log_ddl_add(DDL_OP_Type_alter,
					DDL_ObjType_seq,
					get_database_name(MyDatabaseId),
					get_namespace_name(rel->rd_rel->relnamespace),
					rel->rd_rel->relname.data,
					query,
					rel->rd_rel->relshardid,
					NULL);
	}
	relation_close(rel, NoLock);
}

void log_create_sequence(CreateSeqStmt *stmt, const char *query)
{
	if (stmt->sequence->relpersistence == RELPERSISTENCE_TEMP)
		return;

	RangeVar *relation = stmt->sequence;
	Oid seq_relid = RangeVarGetRelid(relation, NoLock, false);
	Relation seq_rel = relation_open(seq_relid, NoLock);
	Oid shardid = seq_rel->rd_rel->relshardid;

	/* Append shard=N to the ddl */
	if (stmt->shardid == InvalidOid)
	{

		StringInfoData logsql;
		initStringInfo(&logsql);
		int len = strlen(query);
		while (len)
		{
			char c = query[len - 1];
			if (!isspace(c) && c != ';')
				break;
			--len;
		}
		appendStringInfo(&logsql, "%.*s shard %u", len, query, shardid);
		query = donateStringInfo(&logsql);
	}

	log_ddl_add(DDL_OP_Type_create,
				DDL_ObjType_seq,
				get_database_name(MyDatabaseId),
				get_namespace_name(seq_rel->rd_rel->relnamespace),
				relation->relname,
				query,
				seq_rel->rd_rel->relshardid,
				NULL);
	relation_close(seq_rel, NoLock);
}

void log_drop_stmt(DropStmt *stmt, const char *query)
{
	DDL_ObjTypes type = DDL_ObjType_schema;
	if (stmt->removeType != OBJECT_SCHEMA)
	{
		bool allistmp;
		bool hastmp = check_temp_object(stmt->removeType, stmt->objects, &allistmp);
		if (hastmp && !allistmp)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("Kunlun: mix temperary object with normal object is not support")));
		}
		if (hastmp)
			return;
		type = DDL_ObjType_generic;
	}

	log_ddl_add(DDL_OP_Type_drop,
				type,
				get_database_name(MyDatabaseId),
				"",
				NULL,
				query,
				InvalidOid,
				NULL);
}

static void
log_create_alter_role(Node *stmt, const char *query)
{
	List *options;
	const char *role;
	bool iscreate = false;

	if (IsA(stmt, CreateRoleStmt))
	{
		iscreate = true;
		options = ((CreateRoleStmt *)stmt)->options;
		role = ((CreateRoleStmt*)stmt)->role;
	}
	else
	{
		Assert(IsA(stmt, AlterRoleStmt));
		options = ((AlterRoleStmt*)stmt)->options;
		role =  ((AlterRoleStmt*)stmt)->role->rolename;
	}

	DefElem *passwd_elem;
	int next_location = -1;
	ListCell *lc;
	foreach (lc, options)
	{
		DefElem *elem = lfirst_node(DefElem, lc);
		if (strcmp(elem->defname, "password") == 0)
		{
			passwd_elem = elem;
			if (!passwd_elem->arg)
			{
				/* empty password do nothing */
				passwd_elem = NULL;
			}
			else if (lnext(lc))
			{
				/* get the location of the next option element*/
				next_location = lfirst_node(DefElem, lnext(lc))->location;
			}
			break;
		}
	}

	/* rewrite password option to encrypted password */
	if (passwd_elem)
	{
		char *log = 0;
		char *shadow_pwd;
		StringInfoData buff;
		initStringInfo(&buff);

		shadow_pwd = get_role_password(role, &log);
		if (log)
		{
			elog(WARNING, "failed to make encrypted password: %s", log);
		}
		else
		{
			appendStringInfo(&buff, "%.*s encrypted password '%s' %s",
					passwd_elem->location, query,  /* query body before password*/
					shadow_pwd,                            /* encrypted password */
					(next_location > 0 ? query + next_location : "")); /* remaining options */

			query = donateStringInfo(&buff);
		}
	}

	log_ddl_add(iscreate ?  DDL_OP_Type_create : DDL_OP_Type_alter,
			DDL_ObjType_user,
			get_database_name(MyDatabaseId),
			"",
			role,
			query,
			InvalidOid,
			NULL);
}


static
bool is_alter_table_supported(AlterTableStmt *stmt)
{
	const static AlterTableType banned_alcmds[] = {
		/*AT_AttachPartition, */

		AT_SetStatistics,
		AT_SetStorage,

		/**
		 *  If we allow adding/changing constraints to existing table, we would
		 *  need to verify them against existing rows by either pushing
		 *  constraints down(issue: mysql may not have needed
		 *  functions/functionality required by such constraints) to storage
		 *  shards or pulling rows up to computing nodes(issue: hurts system
		 *  performance). So we will not support constraint changes for now.
		 *  Probably we can do below stmt to validate new constraints:
		 *  'select exists(select * from target_table where NOT (CHECK-CONSTRAINT-EXPRESSION))'
		 *  if it's true the new contraints doesn't pass.
		 *  this requires mysql support for full constraint expression, or
		 *  push down supported portions and check remaining in computing node.
		 *  In performance perspective, such constraint validation could be
		 *  expensive so probably we should leave them to client software.
		 */
		AT_AddConstraint,
		AT_AlterConstraint,
		AT_ValidateConstraint,
		AT_AddIndexConstraint,

		/**
		 * This could be supported in future because mysql allows switching
		 * a table's storage engine.
		 */
		AT_SetLogged,	/* SET LOGGED */
		AT_SetUnLogged, /* SET UNLOGGED */

		AT_ClusterOn,	/* CLUSTER ON */
		AT_DropCluster, /* SET WITHOUT CLUSTER */
		AT_AddOids,
		AT_SetTableSpace, /* SET TABLESPACE */

		/**
		 *  mysql doesn't allow changing table storage parameters. and pg's
		 *  original storage params are not used now.
		 */
		AT_SetRelOptions,	  /* SET (...) -- AM specific parameters */
		AT_ResetRelOptions,	  /* RESET (...) -- AM specific parameters */
		AT_ReplaceRelOptions, /* replace reloption list in its entirety */

		/**
		 * triggers and foreign keys will never be supported.
		 */
		AT_EnableTrig,		  /* ENABLE TRIGGER name */
		AT_EnableAlwaysTrig,  /* ENABLE ALWAYS TRIGGER name */
		AT_EnableReplicaTrig, /* ENABLE REPLICA TRIGGER name */
		AT_DisableTrig,		  /* DISABLE TRIGGER name */
		AT_EnableTrigAll,	  /* ENABLE TRIGGER ALL */
		AT_DisableTrigAll,	  /* DISABLE TRIGGER ALL */
		AT_EnableTrigUser,	  /* ENABLE TRIGGER USER */
		AT_DisableTrigUser,	  /* DISABLE TRIGGER USER */

		AT_EnableReplicaRule,

		/**
		 * table types, composite types and table inheritance will never be
		 * supported.
		 */
		AT_AddInherit,		/* INHERIT parent */
		AT_DropInherit,		/* NO INHERIT parent */
		AT_AddOf,			/* OF <type_name> */
		AT_DropOf,			/* NOT OF */
		AT_ReplicaIdentity, /* REPLICA IDENTITY */
		AT_GenericOptions,

		/**
		 *  Need to disable this feature because we don't support policy and we
		 *  can't enforce such rules for updates.
		 */
		AT_EnableRowSecurity,
		AT_DisableRowSecurity,
		AT_ForceRowSecurity,
		AT_NoForceRowSecurity};

	ListCell *lc;
	foreach (lc, stmt->cmds)
	{
		AlterTableCmd *subcmd = lfirst_node(AlterTableCmd, lc);
		if (subcmd->subtype == AT_AddConstraint)
		{
			/* Primary/unique will transform to index, see transformIndexConstraints*/
			Constraint *constraint = (Constraint *)subcmd->def;
			if (constraint->contype == CONSTR_PRIMARY ||
				constraint->contype == CONSTR_UNIQUE)
				continue;
		}
		else if (subcmd->subtype == AT_AlterColumnType)
		{
			ColumnDef *columndef = (ColumnDef *)subcmd->def;
			if (columndef->raw_default)
			{
				TypeCast *cast;
				ColumnRef *column;
				Value *value;

				if (!IsA((cast = (TypeCast *)columndef->raw_default), TypeCast) ||
						!IsA((column = (ColumnRef *)cast->arg), ColumnRef) ||
						!IsA((value = (Value *)linitial(column->fields)), String) ||
						strcasecmp(strVal(value), subcmd->name) != 0)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("Can not alter column type using non-TypeCast expression")));
				}
			}
		}

		for (int i = 0; i < sizeof(banned_alcmds) / sizeof(banned_alcmds[0]); i++)
			if (subcmd->subtype == banned_alcmds[i])
				return false;
	}

	return true;
}

static bool grant_on_global_object(Node *stmt)
{
    bool res = false;
    switch (nodeTag(stmt))
    {
    case T_GrantStmt:
    {
        ObjectType objtype = castNode(GrantStmt, stmt)->objtype;
        if (objtype != OBJECT_DATABASE && objtype != OBJECT_TABLESPACE)
            break;
        /*pass through*/
    }
    case T_CreateRoleStmt:
    case T_AlterRoleStmt:
    case T_DropRoleStmt:
    case T_GrantRoleStmt:
    case T_AlterRoleSetStmt:
    {
        res = true;
        break;
    }
    default:
        break;
    }

    return res;
}

static bool grant_on_temp_object(List *objaddr_list, bool *allistmp)
{
	ListCell *lc;
	int num = 0;
	foreach(lc, objaddr_list)
	{
		ObjectAddress *address = (ObjectAddress *)lfirst(lc);
		if (depend_on_temp_object(address->classId, address->objectId, address->objectSubId))
			++num;
	}
	*allistmp = (num == list_length(objaddr_list));
	return num > 0;
}


/*
  These ddl stmts don't need storage shards actions, simply append them to
  ddl log for other computing nodes to replicate and execute.
*/
static
bool is_simple_ddl(Node* stmt)
{
	switch (nodeTag(stmt))
	{
	case T_DefineStmt:
	case T_GrantRoleStmt:
	case T_CreateRoleStmt:
	case T_AlterRoleStmt:
	case T_AlterRoleSetStmt:
	case T_DropRoleStmt:

	case T_ReassignOwnedStmt:
	case T_CreateExtensionStmt:
	case T_AlterExtensionStmt:
	case T_AlterExtensionContentsStmt:
	case T_CreateFdwStmt:
	case T_AlterFdwStmt:
	case T_CreateForeignServerStmt:
	case T_AlterForeignServerStmt:
	case T_CreateUserMappingStmt:
	case T_AlterUserMappingStmt:
	case T_DropUserMappingStmt:
	case T_ImportForeignSchemaStmt:

	case T_CompositeTypeStmt:
	case T_CreateEnumStmt:	   /* CREATE TYPE AS ENUM */
	case T_CreateRangeStmt:	   /* CREATE TYPE AS RANGE */
	case T_ViewStmt:		   /* CREATE VIEW */
	case T_CreateFunctionStmt: /* CREATE FUNCTION */
	case T_AlterFunctionStmt:  /* ALTER FUNCTION */
	case T_RefreshMatViewStmt:
	case T_CreatePLangStmt:
	case T_CreateConversionStmt:
	case T_CreateCastStmt:
	case T_CreateOpClassStmt:
	case T_CreateOpFamilyStmt:
	case T_CreateTransformStmt:
	case T_AlterOpFamilyStmt:
	case T_AlterTSDictionaryStmt:
	case T_AlterTSConfigurationStmt:
	case T_RenameStmt:
	case T_AlterObjectDependsStmt:
	case T_AlterOwnerStmt:
	case T_AlterOperatorStmt:
	case T_CommentStmt:
	case T_GrantStmt:
	case T_AlterObjectSchemaStmt:
	case T_AlterDefaultPrivilegesStmt:
	case T_CreateForeignTableStmt:
	/*
	   Policies can't be supported for update stmts: alghouth they can be
	   supported for insert stmts: we have to disable them.
	 */
	case T_SecLabelStmt:
	case T_CreateAmStmt:
	case T_AlterCollationStmt:
	case T_AlterSystemStmt:
	// case T_LoadStmt: 
	case T_AlterDatabaseStmt:
	case T_AlterDatabaseSetStmt:
	case T_AlterEnumStmt:
		return true;
	default:
		return false;
	}
}

void pre_handle_ddl(Node *parsetree, const char *query)
{
	bool is_top_stmt = (remoteddl_top_stmt() == parsetree);

	switch (nodeTag(parsetree))
	{
	case T_DropStmt:
	{
		/* Check if the removed object is temp, and do log if neccessary */
		DropStmt *stmt = (DropStmt *)parsetree;
		if (is_top_stmt)
			log_drop_stmt(stmt, query);
		break;
	}
	case T_RenameStmt:
	{
		RenameStmt *stmt = (RenameStmt *)parsetree;
		switch (stmt->renameType)
		{
		case OBJECT_DATABASE:
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("Can not rename database in Kunlun database cluster, storage shards don't support it.")));
			break;
		case OBJECT_SCHEMA:
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("Schemas can't be renamed because storage shard DBs can't be renamed.")));
			break;
		default:
			break;
		}
		break;
	}
	case T_CreateSchemaStmt:
	{
		CreateSchemaStmt *stmt = (CreateSchemaStmt *)parsetree;
		/**
		 * User may create tables in schema elements, it is complex
		 * to add shard=N option to the create table before log into
		 * the meta server, so just reject it
		 */
		if (list_length(stmt->schemaElts))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("Kunlun-db: Define new object while creating a schema is not supported")));
		}
		break;
	}
	case T_AlterTableStmt:
	{
		AlterTableStmt *stmt = (AlterTableStmt *)parsetree;
		if (!is_alter_table_supported(stmt))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("Kunlun-db: Alter table is not supported")));
		}
		break;
	}
	case T_AlterDatabaseStmt:
	{
		AlterDatabaseStmt *stmt = (AlterDatabaseStmt*)parsetree;
		ListCell *option;
		foreach(option, stmt->options)
		{
			DefElem    *defel = (DefElem *) lfirst(option);
			if (strcmp(defel->defname, "tablespace") == 0)
			{
				ereport(WARNING,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot alter tablespace to DBs stored in storage shards, this action is skipped and ignored.")));
				break;
			}
		}
		break;
	}

	/* Banned ddl */
	case T_CreateTableSpaceStmt:
	case T_DropTableSpaceStmt:
	case T_AlterTableSpaceOptionsStmt:
	case T_ClusterStmt:
	case T_CreateTrigStmt:
	case T_AlterTableMoveAllStmt:
	case T_CreatePublicationStmt:
	case T_AlterPublicationStmt:
	case T_CreateSubscriptionStmt:
	case T_AlterSubscriptionStmt:
	case T_DropSubscriptionStmt:
	case T_CreateEventTrigStmt:
	case T_AlterEventTrigStmt:
	case T_VacuumStmt:
	case T_ReindexStmt:
	case T_RuleStmt:
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
	case T_CreateDomainStmt:
	case T_AlterDomainStmt:

	case T_TruncateStmt:  /* may support in future but not now */
	case T_DropOwnedStmt: // DropOwnedObjects
	case T_CreateTableAsStmt:
	case T_CreateStatsStmt:
	/*
	  Policies can't be supported for update stmts, alghouth they can be
	  supported for insert stmts, we have to disable them.
	*/
	case T_CreatePolicyStmt:
	case T_AlterPolicyStmt:
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Statement '%s' is not supported in Kunlun.", CreateCommandTag((Node *)parsetree))));
		break;
	}
	default:
		break;
	}
}

void post_handle_ddl(Node *parsetree, const char *query)
{
	/* For now, only log client ddl into meta server here */
	if (remoteddl_top_stmt() != parsetree)
		return;

	switch (nodeTag(parsetree))
	{
	case T_DropStmt:
	{
		break;
	}
	case T_CreatedbStmt:
	{
		CreatedbStmt *create_stmt = (CreatedbStmt *)parsetree;
		log_create_db(create_stmt, query);
		break;
	}
	case T_DropdbStmt:
	{
		DropdbStmt *stmt = (DropdbStmt *)parsetree;
		log_drop_db(stmt, query);
		break;
	}
	case T_CreateSchemaStmt:
	{
		CreateSchemaStmt *create_stmt = (CreateSchemaStmt *)parsetree;
		log_create_schema(create_stmt, query);
		break;
	}
	case T_CreateStmt:
	{
		CreateStmt *create_stmt = (CreateStmt *)parsetree;
		log_create_stmt(create_stmt, query);
		break;
	}
	case T_IndexStmt:
	{
		IndexStmt *index_stmt = (IndexStmt *)parsetree;
		log_create_index(index_stmt, query);
		break;
	}
	case T_CreateSeqStmt:
	{
		CreateSeqStmt *seq_stmt = (CreateSeqStmt *)parsetree;
		log_create_sequence(seq_stmt, query);
		break;
	}
	case T_AlterTableStmt:
	{
		AlterTableStmt *alter_stmt = (AlterTableStmt *)parsetree;
		log_alter_table(alter_stmt, query);
		break;
	}
	case T_AlterSeqStmt:
	{
		AlterSeqStmt *alter_stmt = (AlterSeqStmt*)parsetree;
		log_alter_seq(alter_stmt, query);
		break;
	}
	case T_CreateRoleStmt:
	case T_AlterRoleStmt:
	{
		/* encrypt password before write ddl query into log */
		log_create_alter_role(parsetree, query);
		break;
	}
	case T_GrantStmt:
	{
		GrantStmt *stmt = (GrantStmt *)parsetree;
		bool hastmp = false;
		if (stmt->targtype == ACL_TARGET_OBJECT)
		{
			bool allistmp;
			List *objects = object_name_to_objectaddress(stmt->objtype, stmt->objects);
			hastmp = grant_on_temp_object(objects, &allistmp);
			if (hastmp && !allistmp)
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
												errmsg("Kunlun-db: Statement '%s' not support temporary object mixied normal object",
															 CreateCommandTag(parsetree))));
			}
		}

		if (hastmp)
			break;
		/* pass through */
	}
	default:
	{
		/* If the modified/created object depend on temp table/type,, ignore it */
		remoteddl_reanalyze_temp_dependent();
		List *objects = g_remote_ddl_context->access_object_list;
		bool all_is_temp = (list_length(objects) == g_remote_ddl_context->temp_object_num);
		if (g_remote_ddl_context->temp_object_num > 0)
		{
			if (!all_is_temp)
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("Kunlun-db: Statement '%s' not support temporary object mixied normal object",
									   CreateCommandTag(parsetree))));
		}
		else if (is_simple_ddl(parsetree))
		{
			/* Mark statement related to privilege and global object.
			 *
			 * Note: the following types of statements only relate to objects in local database
			 * and are executed as ordinary DDL statements.
			 * (1) T_AlterDefaultPrivilegesStmt:
			 *	  grant/revoke privilege on objects in local database
			 * (2) T_ReassignOwnedStmt:
			 *    change the ownership of local database objects
			 * (3) T_GrantStmt:
			 *    the object type is database or tablespace
			 */
			DDL_ObjTypes obj = DDL_ObjType_generic;
			if (grant_on_global_object(parsetree))
				obj = DDL_ObjType_user;

			log_ddl_add(DDL_OP_Type_generic, obj,
						get_database_name(MyDatabaseId),
						NULL, NULL, query, 0, NULL);
		}
		break;
	}
	}
}
