/*-------------------------------------------------------------------------
 *
 * utils.h
 *
 *	Helper function implementation
 *
 * Copyright (c) 2021-2022 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 *-------------------------------------------------------------------------
 */
#ifndef REMOTE_DDL_UTILS_H
#define REMOTE_DDL_UTILS_H

#include "postgres.h"

#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "lib/stringinfo.h"

typedef bool (*FeatureFunc)(Node *);

/**
 * @brief Get the mysql escape string object
 */
extern char* escape_mysql_string(const char *from);

/**
 * @brief Helper function, print attribute defination in mysql format based on tuple in pg_attribute
 */
extern void print_pg_attribute(Oid relOid, int attnum, bool justname, StringInfo str);

/**
 * @brief Helper function, check if the given object depend on temp object
 */
extern bool depend_on_temp_object(Oid classid, Oid objId, Oid subId);

/**
 * @brief Heler function, check if the give objects depend on temp object
 */
extern bool check_temp_object(ObjectType objtype, List *objects, bool *allistemp);

/**
 * @brief Helper function, modify relation's shardid option in pg_class
 */
extern void change_relation_shardid(Oid relid, Oid shardid);

/**
 * @brief Get the current username
 */
extern char* get_current_username(void);

#endif
