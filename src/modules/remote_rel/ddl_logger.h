/*-------------------------------------------------------------------------
 *
 *  ddl_logger.h 
 *
 *		Generate the corresponding ddl log event for client ddl query
 *
 * Copyright (c) 2021-2022 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 *-------------------------------------------------------------------------
 */
#ifndef DDL_LOGGER_H
#define DDL_LOGGER_H


#include "postgres.h"

#include "nodes/parsenodes.h"
#include "utils/relcache.h"

/**
 * @brief Check and log client ddl statements before ProcessUtility()
 */
extern void pre_handle_ddl(Node *parsetree, const char *query);

/**
 * @brief Check and log client ddl statements after ProcessUtility() 
 */
extern void post_handle_ddl(Node *parsetree, const char *query);

#endif
