/*-------------------------------------------------------------------------
 *
 * cluster_meta.h
 *	  definitions of types, and declarations of functions, that are used in
 *	  meta data cluster SQL statement send and result receive.
 *
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * src/include/sharding/cluster_meta.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef CLUSTER_META_H
#define CLUSTER_META_H

#include "postgres.h"
#include "sharding/mysql/mysql.h"
#include "nodes/nodes.h"
#include "postmaster/bgworker.h"

extern Oid cluster_id;
extern Oid comp_node_id;
extern NameData g_cluster_name;

#define METADATA_SHARDID 0xFFFFFFFF
#define MAX_META_SHARD_NODES 19

/*
 * What status to check, flag bits for check_mysql_instance_status.
 * */
#define CHECK_KEEP_ALIVE 0x1
#define CHECK_NOT_READONLY 0x2
#define CHECK_IS_READONLY 0x4
#define CHECK_SET_NAMES 0x8
#define CHECK_MGR_MASTER 0x10
#define CHECK_RBR_MASTER 0x20

/*
 * A mysql connection for synchronous communication, currently only used to
 * communicate with mysql metadata cluster.
 * */
typedef struct MYSQL_CONN
{
	bool connected;
	bool inside_err_hdlr;
	char node_type; // type of connected mysql instance. 1: primary; 0: replica; -1: unknown
	CmdType cmd;
	MYSQL_RES *result;
	int nrows_affected;
	int nwarnings;
	/*
	 * If mysql returns this error number, ignore it, don't report or throw.
	 * for now we only need to ignore one error, make it an array if need more
	 * in future.
	 * */
	int ignore_err;
	MYSQL conn;
} MYSQL_CONN;

typedef enum DDL_ObjTypes
{
	DDL_ObjType_Invalid,
	DDL_ObjType_db,
	DDL_ObjType_index,
	DDL_ObjType_matview,
	DDL_ObjType_partition,
	DDL_ObjType_schema,
	DDL_ObjType_seq,
	DDL_ObjType_table,
	DDL_ObjType_func,
	DDL_ObjType_role_or_group,
	DDL_ObjType_proc,
	DDL_ObjType_stats,
	DDL_ObjType_user,
	DDL_ObjType_view,
	DDL_ObjType_generic
} DDL_ObjTypes;

typedef enum DDL_OP_Types
{
	DDL_OP_Type_Invalid,
	DDL_OP_Type_create,
	DDL_OP_Type_drop,
	DDL_OP_Type_rename,
	DDL_OP_Type_alter,
	DDL_OP_Type_replace,
	DDL_OP_Type_remap_shardid,
	DDL_OP_Type_generic
} DDL_OP_Types;

extern int connect_meta_master(MYSQL_CONN *mysql, const char *host,
	uint16_t port, const char *user, const char *password, bool is_bg);
extern int connect_meta_slave(MYSQL_CONN *mysql, const char *host,
	uint16_t port, const char *user, const char *password, bool is_bg);
extern bool send_stmt_to_cluster_meta(MYSQL_CONN*conn, const char *stmt,
	size_t len, CmdType cmd, bool isbg);
extern bool check_mysql_instance_status(MYSQL_CONN*conn,
	uint64_t checks, bool isbg);
extern TransactionId get_max_txnid_cluster_meta(MYSQL_CONN*conn, bool *done);
extern MYSQL_CONN* get_metadata_cluster_conn(bool isbg);
extern void close_metadata_cluster_conn(MYSQL_CONN* conn);
/*
 * the handler returns true to go on with next db, false stops the scan.
 * */
typedef bool (*db_handler_t)(Oid dbid, const char *db, void*param);
extern const char *GetClusterName2(void);
extern bool UpdateCurrentMetaShardMasterNodeId(void);
extern int FindCurrentMetaShardMasterNodeId(Oid *pmaster_nodeid, Oid *old_master_nodeid);
extern void disconnect_metadata_shard(void);
extern void KillMetaShardConn(char type, uint32_t connid);
extern void free_metadata_cluster_result(MYSQL_CONN *conn);
extern bool handle_metadata_cluster_result(MYSQL_CONN *conn, bool isbg);
extern int  handle_metadata_cluster_error(MYSQL_CONN *conn, bool throw_error);
#endif // !CLUSTER_META_H
