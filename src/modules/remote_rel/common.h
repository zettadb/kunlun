/*-------------------------------------------------------------------------
 *
 * common.h
 *
 *	Structs used to track all the generated ddl log/remote sqls during
 *	transaction
 *
 *
 * Copyright (c) 2021-2022 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 *-------------------------------------------------------------------------
 */

#ifndef REMOTE_DDL_COMMON_H
#define REMOTE_DDL_COMMON_H

#include "postgres.h"

#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "catalog/objectaddress.h"
#include "utils/memutils.h"

#include "utils.h"

#undef STRING_WITH_LEN
#define STRING_WITH_LEN(x) (x), ((size_t)(sizeof(x) - 1))

struct DDLLogContext;

/**
 * The context of a ddl statement
 */
typedef struct Remote_ddl_context
{
	Node *top_stmt;

	/* The sub transaction */
	SubTransactionId subxid;

	/* Used to unify the shardid newly created relation during ddl.*/
	Oid first_obj_shardid;

	/**
	 * The objects accessed in the ddl statement
	 */
	List *access_object_list;
	List *access_temp_dependent;
	int temp_object_num;

	/**
	 * The objects for which remote sql is temporarily not
	 * generated due to missing system catalog
	 */
	List *delay_created_object;
	CommandId lastest_commandid;

	/* The ddl log event to be logged into the meta server */
	struct DDLLogContext *ddllog_context;

	/* The remote sqls to be executed in the storage nodes */
	List *ddlremote_list;

	/* The ddl statement executed */
	bool ended;
} Remote_ddl_context;

/**
 * All of the ddl context in the transaction
 */
typedef struct Remote_ddl_trans
{
	MemoryContext mem_ctx;
	/* True if this is a explict transaction */
	bool explict_txn;

	/* All of the ddl context in the transaction */
	List *ddl_context_list;

	/* The XA id of the ddl log trans */
	char *xa_txnid;
} Remote_ddl_trans;

extern Remote_ddl_trans remote_ddl_trans_data;
extern Remote_ddl_trans *g_remote_ddl_trans;
extern Remote_ddl_context *g_remote_ddl_context;

extern int apply_ddl_log_mode;
extern bool enable_remote_relations;

static inline void
remoteddl_alloc_trans(void)
{
	static bool first = true;
	Assert(!g_remote_ddl_trans);
	g_remote_ddl_trans = &remote_ddl_trans_data;
	g_remote_ddl_trans->explict_txn = false;
	g_remote_ddl_trans->ddl_context_list = NIL;
	g_remote_ddl_trans->xa_txnid = NULL;
	g_remote_ddl_trans->explict_txn = (IsTransactionState() && IsExplicitTxn());

	if (first)
	{
		first = false;
		g_remote_ddl_trans->mem_ctx =
			AllocSetContextCreate(TopMemoryContext,
								  "remoteddl txn context",
								  ALLOCSET_DEFAULT_SIZES);
	}
}

static inline void
remoteddl_free_trans(void)
{
	MemoryContextReset(g_remote_ddl_trans->mem_ctx);
	g_remote_ddl_context = NULL;
	g_remote_ddl_trans = NULL;
}
static inline void
remoteddl_alloc_context(Node *top_stmt)
{
	if (!g_remote_ddl_trans)
	{
		remoteddl_alloc_trans();
		Assert(g_remote_ddl_trans);
	}

	if (g_remote_ddl_context == NULL)
	{
		g_remote_ddl_context =
			MemoryContextAllocZero(g_remote_ddl_trans->mem_ctx,
								   sizeof(*g_remote_ddl_context));
	}
	else
	{
		/* Reuse the last ddl context*/
		Assert(!g_remote_ddl_context->ended);
	}

	g_remote_ddl_context->top_stmt = top_stmt;
	g_remote_ddl_context->subxid = InvalidSubTransactionId;
	g_remote_ddl_context->first_obj_shardid = InvalidOid;
	g_remote_ddl_context->temp_object_num = 0;
	g_remote_ddl_context->access_object_list = NIL;
	g_remote_ddl_context->access_temp_dependent = NIL;
	g_remote_ddl_context->delay_created_object = NIL;
	g_remote_ddl_context->lastest_commandid = 0;
	g_remote_ddl_context->ddllog_context = NULL;
	g_remote_ddl_context->ddlremote_list = NIL;
	g_remote_ddl_context->ended = false;
}

/* mark the end of the execution */
static inline void
remoteddl_end_context(bool done)
{
	if (g_remote_ddl_context && done)
	{
		MemoryContext oldCtx = MemoryContextSwitchTo(g_remote_ddl_trans->mem_ctx);
		g_remote_ddl_context->ended = true;
		g_remote_ddl_context->subxid = GetCurrentSubTransactionId();
		g_remote_ddl_trans->ddl_context_list =
			lappend(g_remote_ddl_trans->ddl_context_list, g_remote_ddl_context);

		MemoryContextSwitchTo(oldCtx);
	}
	g_remote_ddl_context = NULL;
}

static inline void
remoteddl_rollback_to(SubTransactionId subtxn_id)
{
	if (g_remote_ddl_trans)
	{
		ListCell *lc = NULL;
		Size size = 0;
		Remote_ddl_context *context;
		foreach (lc, g_remote_ddl_trans->ddl_context_list)
		{
			context = (Remote_ddl_context *)lfirst(lc);
			if (context->subxid > subtxn_id)
				break;
			size++;
		}
		g_remote_ddl_trans->ddl_context_list =
			list_truncate(g_remote_ddl_trans->ddl_context_list, size);
	}
}

/* get the parsetree of the top stmt*/
static inline Node *remoteddl_top_stmt(void)
{
	if (g_remote_ddl_context)
		return g_remote_ddl_context->top_stmt;
	return NULL;
}

/* set the shardid of the first created object */
static inline Oid remoteddl_set_shardid(Oid shardid)
{
	if (g_remote_ddl_context->first_obj_shardid == InvalidOid)
		g_remote_ddl_context->first_obj_shardid = shardid;

	return g_remote_ddl_context->first_obj_shardid;
}

/* track created object in ddl */

static inline void
remoteddl_enque_access_object(ObjectAddress *object,
							  int type)
{
	g_remote_ddl_context->access_object_list =
		lappend(g_remote_ddl_context->access_object_list, object);

	if (depend_on_temp_object(object->classId, object->objectId,
							  object->objectSubId))
	{
		g_remote_ddl_context->temp_object_num++;
		g_remote_ddl_context->access_temp_dependent =
			lappend_int(g_remote_ddl_context->access_temp_dependent, 1);
	}
	else
	{
		g_remote_ddl_context->access_temp_dependent =
			lappend_int(g_remote_ddl_context->access_temp_dependent, 0);
	}
}

/* reanalyze if the accessed object is dependent on other temp object*/
static inline bool remoteddl_reanalyze_temp_dependent()
{
	ListCell *lc1, *lc2;
	forboth(lc1, g_remote_ddl_context->access_object_list,
			lc2, g_remote_ddl_context->access_temp_dependent)
	{
		if (lfirst_int(lc2))
			continue;
		ObjectAddress *object = (ObjectAddress *)lfirst(lc1);
		if (depend_on_temp_object(object->classId, object->objectId,
								  object->objectSubId))
		{
			lc2->data.int_value = 1;
			g_remote_ddl_context->temp_object_num++;
		}
	}

	return g_remote_ddl_context->temp_object_num;
}

/*
  whether current txn was started by current command. When executing DDLs we
  require the stmt be an autocommit txn.

  -1: UNKNOWN; 0: FALSE; 1: TRUE
*/
static inline bool enable_remote_ddl(void)
{
	return enable_remote_relations && IsNormalProcessingMode() &&
		   IsUnderPostmaster && apply_ddl_log_mode == 0;
}
#endif
