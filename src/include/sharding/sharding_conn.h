/*-------------------------------------------------------------------------
 *
 * sharding_conn.h
 *	  definitions of types, and declarations of functions, that are used in
 *	  table sharding connection functionality.
 *
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * src/include/sharding/sharding_conn.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SHARDING_CONN_H
#define SHARDING_CONN_H

#include "sharding/mysql/mysql.h"
#include "sharding/mysql/server/private/sql_cmd.h"
#include "sharding/sharding.h"
#include "nodes/nodes.h"

/* GUC options. */
extern int mysql_connect_timeout;
extern int mysql_read_timeout;
extern int mysql_write_timeout;
extern int mysql_max_packet_size;
extern bool mysql_transmit_compress;

typedef struct ShardConnection
{
	Oid shard_id; // Connections to one shard's all nodes.
	uint8_t num_nodes; // number of payload slots in the 3 arrays below.
	/*
	 * Whenever a connection to storage node is made once, the nodeids[i],
	 * conns[i] and conn_flags[i] always belong to the same storage node.
	 * */
	Oid nodeids[MAX_NODES_PER_SHARD]; // shard node ids. insert in order
	MYSQL *conns[MAX_NODES_PER_SHARD]; // conn[i] is nodeids[i]'s connection.

	// conn_flags[i]: flag bits of conns[i].
// Connection is valid. if not, need to reconnect at next use of the connection.
#define CONN_VALID 0x1
// Connection is reset. if so, need to resend SET NAMES and cached
// mysql session vars before sending any stmt through it
#define CONN_RESET 0x2
	char conn_flags[MAX_NODES_PER_SHARD];
	MYSQL conn_objs[MAX_NODES_PER_SHARD]; // append only
} ShardConnection;


typedef struct StmtElem
{
	char *stmt;
	size_t stmt_len;
	bool owns_stmt_mem;
	CmdType cmd;
	enum enum_sql_command sqlcom;
} StmtElem;

typedef struct StmtQueue
{
	StmtElem *queue;
	/*
	 * head: 'queue' array index of 1st valid element
	 * tail: 'queue' array index of last valid element + 1
	 * capacity: total number of slots in 'queue' array.
	 * head >= 0, head < end, [head, end) are valid elements.
	 * end <= capacity, when end == capacity, queue is full, needs realloc.
	 * */
	int head;
	int end;
	int capacity;
} StmtQueue;

/*
 * A communication port with one storage node, which mostly is a master.
 * It should be reset/cleared at start of each statement.
 * */
typedef struct AsyncStmtInfo
{
	Oid shard_id, node_id;
	int status; // mysql async API wait&cont status.

	/*
	 * In this channel, the NO. of stmts executed correctly and got its/their
	 * results.
	 * */
	int executed_stmts;
	
	bool result_pending; // true iff there is result we need to wait for

	/*
	 * Whether current remote txn in this channel need to be aborted when
	 * current txn is to be aborted. disconnected channels can't do XA ROLLBACK.
	 * */
	bool need_abort;

	/*
	  need to rewind, most likely its resultset is used as inner node result
	  of a join. If so store result rather than use result.
	*/
	bool will_rewind;

	/*
	 * If MySQL returns this error, ignore it, do not throw, only log a
	 * warning message. So far we only need to ignore
	 * one or 0 error for a stmt.
	 * If 0, it's not effective.
	 * */
	int ignore_error;

	MYSQL *conn;

	/*
	 * Info about current stmt to work on, and the result of the commands.
	 * The 'nrows' and 'nwarnings' are accumulated using results of all stmts
	 * in the queue.
	 */
	char *stmt;
	size_t stmt_len;

	CmdType cmd;

	/*
	 * Concrete sql command type in MySQL terms.
	 * */
	enum enum_sql_command sqlcom;

	/*
	 * Inserted/Deleted/Modified rows of current INSERT/DELETE/UPDATE stmt,
	 * and returned NO. of rows of current SELECT stmt.
	 * */
	uint32_t stmt_nrows;

	/*
	 * Inserted/Deleted/Modified rows of INSERT/DELETE/UPDATE stmts executed in
	 * current txn and NOT SET for returned NO. of rows for SELECT.
	 * */
	uint32_t txn_wrows;

	/*
	 * Result of current SELECT 'stmt' ready to be used, NULL if not ready
	 * yet or for other types of commands. It is retrieved in stream mode using
	 * mysql_use_result().
	 * */
	MYSQL_RES *mysql_res;

	/*
	 * NO. of warnings from storage node query execution.
	 */
	uint32_t nwarnings;

	/*
	 * Whether write(INSERT/UPDATE/DELETE) and read(SELECT) commands were
	 * executed in current pg stmt.
	 * */
	bool did_write;
	bool did_read;
	/*
	 * Set if an DDL is executed in this shard in current txn.
	 * */
	bool did_ddl;
	/*
	 * if true, 'stmt' will be pfree'd after it's sent and reply received.
	 * */
	bool owns_stmt_mem;

	// stmts queue, its items will be assigned to 'stmt' and 'stmt_len'
	// fields to be executed.
	StmtQueue stmtq;
} AsyncStmtInfo;

inline static bool ASIAccessed(AsyncStmtInfo *asi)
{
	return asi->did_write || asi->did_ddl || asi->did_read;
}

inline static bool ASIReadOnly(AsyncStmtInfo *asi)
{
	return !asi->did_write && !asi->did_ddl && asi->did_read;
}

inline static bool ASIConnected(AsyncStmtInfo *asi)
{ return asi && asi->conn != NULL; }

extern StmtElem *append_async_stmt(AsyncStmtInfo *asi, char *stmt,
	size_t stmt_len, CmdType cmd, bool ownsit, enum enum_sql_command sqlcom);
extern int work_on_next_stmt(AsyncStmtInfo *asi);
extern void ResetCommunicationHub(void);
extern void ResetCommunicationHubStmt(bool ended_clean);
extern AsyncStmtInfo *GetAsyncStmtInfoByIndex(int i);
extern int GetAsyncStmtInfoUsed(void);
extern AsyncStmtInfo *GetAsyncStmtInfo(Oid shardid);
extern AsyncStmtInfo *GetAsyncStmtInfoNode(Oid shardid, Oid shardNodeId, bool req_chk_onfail);

extern void send_remote_stmt(AsyncStmtInfo *asi, char *stmt, size_t len,
	CmdType cmdtype, bool owns_it, enum enum_sql_command sqlcom, int ignore_err);
extern void send_stmt_to_all_inuse(char *stmt, size_t len, CmdType cmdtype, bool owns_it,
	enum enum_sql_command sqlcom, bool written_only);
extern void
send_stmt_to_all_shards(char *stmt, size_t len, CmdType cmdtype, bool owns_it,
	enum enum_sql_command sqlcom);
extern const char *make_qualified_name(Oid nspid, const char *objname, int *plen);
extern void InitShardingSession(void);
extern int send_stmt_to_multi_start(AsyncStmtInfo *asis, size_t shard_cnt);
extern int send_stmt_to_multi_try_wait(AsyncStmtInfo *asis, size_t shard_cnt);
extern void send_multi_stmts_to_multi(void);
extern MYSQL_RES *GetRemoteRows(AsyncStmtInfo *pasi);
extern uint64_t GetRemoteAffectedRows(void);
extern void free_mysql_result(AsyncStmtInfo *pasi);
extern bool MySQLQueryExecuted(void);
extern void CancelAllRemoteStmtsInQueue(bool freestmts);
extern void cleanup_asi_work_queue(AsyncStmtInfo *pasi);
extern bool IsConnReset(AsyncStmtInfo *asi);
extern void check_mysql_fetch_row_status(AsyncStmtInfo *asi);
extern void disconnect_storage_shards(void);
extern void request_topo_checks_used_shards(void);
extern void ResetASIInternal(AsyncStmtInfo *asi);
extern void send_stmt_to_multi_try_wait_all(void);
#endif // !SHARDING_CONN_H
