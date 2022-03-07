/*-------------------------------------------------------------------------
 *
 * hook.c
 *
 * Helper function to use when logging ddl events.
 *
 * Copyright (c) 2021-2022 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 *-------------------------------------------------------------------------
 */
#ifndef REMOTE_DDL_LOG_H
#define REMOTE_DDL_LOG_H

#include "postgres.h"

#include "sharding/cluster_meta.h"

static const char *DDL_OP_TypeNames[] = {
		"Invalid_operation",
		"create",
		"drop",
		"rename",
		"alter",
		"replace",
		"others" // generic.
};

static const char *DDL_ObjTypeNames[] = {
    "Invalid_object",
    "db",
    "index",
    "matview",
    "partition",
    "schema",
    "seq",
    "table",
    "func",
    "role_or_group",
    "proc",
    "stats",
    "user",
    "view",
    "others" // generic
};

/**
 * @brief Check if the given parsetree is from a ddl statement
 */
extern bool is_ddl_query(Node *parsetree);


/**
 * @brief Get the global ddl lock to seralized ddl
 */
extern void ddl_log_get_lock(void);

extern void ddl_log_release_lock(void);

/**
 * @brief Wait until lock meta data is up to dat
 */
extern void catch_up_latest_meta(void);

/**
 * @brief Get the progress of current ddl aplier
 */
extern uint64_t get_ddl_applier_progress(bool sharelock);

/**
 * @brief Update the progress of current ddl aplier
 */
extern void update_ddl_applier_progress(uint64_t newid);

/**
 * @brief Add ddl log event to be logged to the meta server.
 */
extern void log_ddl_add(DDL_OP_Types op,
                 DDL_ObjTypes objtype,
                 const char *db,
                 const char *schema,
                 const char *object,
                 const char *query,
                 Oid shardid,
                 const char *info); 

/**
 * @brief Add remote sqls to the current ddl log event
 */
extern void log_ddl_add_extra(void);

/**
 * @brief Check if there is not ddl to be loggged to the meta server.
 */
extern bool log_ddl_skip(void);

/**
 * @brief Prepare a transaction which log the ddl information to the meta server.
 * 
 * @return uint64_t The log id of the ddl query
 */
extern uint64_t log_ddl_prepare(void);

/**
 * @brief Commit the transcation which log the ddl information to the meta server.
 * 
 */
extern void log_ddl_commit(void);

/**
 * @brief Rollback the prepared transaction which log the ddl information to the meta server
 * 
 */
extern void log_ddl_rollback(void);
#endif
