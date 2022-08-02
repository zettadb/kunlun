/*-------------------------------------------------------------------------
 *
 * remote_ddl.h
 *
 * Generate sql for creating/deleting/altering objects in storage nodes
 *
 *
 * Copyright (c) 2021-2022 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 *-------------------------------------------------------------------------
 */
#ifndef REMOTE_DDL_H
#define REMOTE_DDL_H

#include "postgres.h"

#include "catalog/objectaccess.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "nodes/nodes.h"
#include "utils/relcache.h"

#include "sharding/mysql/server/private/sql_cmd.h"

/**
 * @brief  Add the ddl that will be executed on Kunlun storage to the queue.
 */
extern void enque_remote_ddl(enum enum_sql_command sql_command, Oid shard, StringInfo query, bool replace); 

/**
 * @brief Send all the ddl in the queue to the kunlun storage for execution
 */
extern void execute_all_remote_ddl(void);

/**
 * @brief Return all the sql in the queue 
 */
extern char *dump_all_remote_ddl(void);

extern void change_relation_shardid(Oid relid, Oid shardid);

/**
 *  Helper function to generate remote ddl for the storage nodes and add it to the queue
 */
extern void remote_create_database(const char *dbname);
extern void remote_drop_database(const char *dbname);

extern void remote_create_schema(const char *schema);
extern void remote_drop_schema(const char *schema);

extern void remote_drop_table(Relation relation);
extern void remote_create_table(Relation relation);

extern void remote_create_sequence(Relation rel);
extern void remote_drop_sequence(Relation rel);
extern void remote_drop_nonnative_sequence_in_schema(const char *db, const char *schema);

extern void remote_add_index(Relation relation);
extern void remote_drop_index(Relation relation);

extern void remote_alter_table(Relation);
extern void remote_alter_sequence(Relation);
extern void remote_alter_index(Relation);
extern void remote_alter_column(Relation rel, int attrnum, ObjectAccessType type);

extern void remote_alter_type(Oid typeid);
extern bool remote_truncate_table(TruncateStmt *stmt);

#endif
