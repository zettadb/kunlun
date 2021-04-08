/*-------------------------------------------------------------------------
 *
 * cluster_meta.h
 *	  definitions of types, and declarations of functions, that are used in
 *	  meta data cluster SQL statement send and result receive.
 *
 *
 * Copyright (c) 2019 ZettaDB inc. All rights reserved.
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
	DDL_OP_Type_generic
} DDL_OP_Types;

extern int connect_mysql_master(MYSQL_CONN *mysql, const char *host,
	uint16_t port, const char *user, const char *password, bool is_bg);
extern int connect_mysql_slave(MYSQL_CONN *mysql, const char *host,
	uint16_t port, const char *user, const char *password, bool is_bg);
extern bool send_stmt_to_cluster_meta(MYSQL_CONN*conn, const char *stmt,
	size_t len, CmdType cmd, bool isbg);
extern bool check_mysql_instance_status(MYSQL_CONN*conn,
	uint64_t checks, bool isbg);
extern TransactionId get_max_txnid_cluster_meta(MYSQL_CONN*conn, bool *done);
extern MYSQL_CONN* get_metadata_cluster_conn(bool isbg);
extern void close_metadata_cluster_conn(MYSQL_CONN* conn);
extern bool check_ddl_op_conflicts_rough(MYSQL_CONN *conn, const char *db, DDL_ObjTypes objtype);
extern uint64_t log_ddl_op(MYSQL_CONN *conn, const char *xa_txnid, const char *db,
					const char *schema, const char *obj, DDL_ObjTypes obj_type,
					DDL_OP_Types optype, const char *sql_src, const char *sql_src_storage_node,
					Oid target_shardid);
extern void update_my_max_ddl_op_id(uint64_t opid, bool is_db_ddl);
extern Size ClusterMetaShmemSize(void);
extern void MetadataClusterShmemInit(void);
extern void NotifyNextDDLOp(uint64_t opid);
typedef int (*log_apply_func_t)(uint64_t newpos, const char *sqlstr,
	DDL_OP_Types optype, DDL_ObjTypes objtype, const char *objname, bool*execed);
extern int fetch_apply_cluster_ddl_logs(Oid dbid, const char *dbname, uint64_t startpos,
	log_apply_func_t apply, bool is_main_applier, bool is_recovery);
/*
 * the handler returns true to go on with next db, false stops the scan.
 * */
typedef bool (*db_handler_t)(Oid dbid, const char *db, void*param);
extern void scan_all_dbs(db_handler_t dbhdlr, void *param);
extern bool SetLatestDDLOperationId(uint64_t opid);
extern uint64_t GetLatestDDLOperationId(uint64_t *local_max);
extern const char *GetClusterName2(void);
extern void delete_ddl_log_progress(Oid dbid);
extern void insert_ddl_log_progress(Oid dbid, uint64_t maxopid);
extern bool UpdateCurrentMetaShardMasterNodeId(void);
extern int FindCurrentMetaShardMasterNodeId(Oid *pmaster_nodeid, Oid *old_master_nodeid);
extern void disconnect_metadata_shard(void);
extern void KillMetaShardConn(char type, uint32_t connid);
#endif // !CLUSTER_META_H
