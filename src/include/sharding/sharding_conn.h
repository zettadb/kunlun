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
#include "sharding/mat_cache.h"
#include "nodes/nodes.h"

/* GUC options. */
extern int mysql_connect_timeout;
extern int mysql_read_timeout;
extern int mysql_write_timeout;
extern int mysql_max_packet_size;
extern bool mysql_transmit_compress;

/**
 * CONN_VALID	: Connection is valid. if not, need to reconnect at next use of the connection.
 * CON_RESET	: Connection is reset. if so, need to resend SET NAMES and cached.
 * 				  mysql session vars before sending any stmt through it
 */
#define CONN_VALID 0x1
#define CONN_RESET 0x2
typedef struct ShardConnection
{
	Oid shard_id;	   // Connections to one shard's all nodes.
	uint8_t num_nodes; // number of payload slots in the 3 arrays below.
	/*
	 * Whenever a connection to storage node is made once, the nodeids[i],
	 * conns[i] and conn_flags[i] always belong to the same storage node.
	 * */
	Oid nodeids[MAX_NODES_PER_SHARD];  // shard node ids. insert in order
	MYSQL *conns[MAX_NODES_PER_SHARD]; // conn[i] is nodeids[i]'s connection.

	char conn_flags[MAX_NODES_PER_SHARD];
	MYSQL conn_objs[MAX_NODES_PER_SHARD]; // append only
} ShardConnection;

typedef struct MatCache MatCache;
typedef struct StmtHandle
{
	struct AsyncStmtInfo *asi;
        SubTransactionId subxactid;
	int refcount;
	char *stmt;
	size_t stmt_len;
	bool owns_stmt_mem;
	CmdType cmd;
	enum enum_sql_command sqlcom;
	bool is_dml_write;
	bool support_rewind;

	int status;
	int status_req;
	bool first_packet;  // true if the first packet recved
	bool fetch;			// true if the stmt is going to fetch results?
	bool nextres;		// true if need get the next result
	bool finished;		// true if finish read from socket
	bool cancel;
	bool read_cache;	// true if should read from matcache
	
	int ignore_errno;
	uint32_t affected_rows;
	uint32_t warnings_count;

	MYSQL_RES *res;
	size_t *lengths;
	enum enum_field_types *types;
	MYSQL_ROW row;
	int field_count;

	/* Used by materialize */
	StringInfoData read_buff;
	StringInfoData buff;
	MatCache *cache;
	/* True if reset the next start position to read */
	bool reset_cache_pos;
	/* Read position of the cache */
	MatCachePos cache_pos;

} StmtHandle;

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

	/* The mysql client conn */
	MYSQL *conn;

	/*
	 * Inserted/Deleted/Modified rows of INSERT/DELETE/UPDATE stmts executed in
	 * current txn and NOT SET for returned NO. of rows for SELECT.
	 * */
	uint32_t txn_wrows;

	/*
	 * Inserted/Deleted/Modified rows of INSERT/DELETE/UPDATE stmts executed in
	 * current user stmt and NOT SET for returned NO. of rows for SELECT.
	 * */
	uint32_t stmt_wrows;

	/*
	 * NO. of warnings from storage node query execution. this field should
	 * be returned to pg's client, and in pg's equivalent of 'show warnings'
	 * we should fetch warnings from each storage node that executed part of
	 * the last stmt, and assemble them together as final result to client. TODO
	 */
	uint32_t nwarnings;

	/*
	 * Whether write(INSERT/UPDATE/DELETE) and read(SELECT) commands were
	 * executed in current pg stmt. Transaction mgr should accumulate the
	 * group of storage nodes written and read-only by collecting them from
	 * this object at end of each stmt, in order to do 2PC.
	 * */
	bool did_write;
	bool did_read;
	/*
	 * Set if an DDL is executed in this shard in current txn. Note that we use
	 * CMD_UTILITY to denote DDLs as pg does, but CMD_UTILITY includes many
	 * other types of commands, including CALL stmt. Maybe in future we need
	 * to distinguish stored proc/func CALL stmts from DDL stmts.
	 * */
	bool did_ddl;

	/* Indicate if a xa in progress on the conn */
	bool txn_in_progress;

	/* The current statment work on */
	StmtHandle *curr_stmt;

	/* The pending statements to send */
	List *stmt_queue;

	/* The handle still in use */
	List *stmt_inuse;
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
{
	return asi && asi->conn != NULL;
}

inline static bool ASITxnInProgress(AsyncStmtInfo *asi)
{
	return asi && asi->txn_in_progress;
}

extern const char *make_qualified_name(Oid nspid, const char *objname, int *plen);
extern void InitShardingSession(void);

extern void ResetCommunicationHub(void);
extern void ResetCommunicationHubStmt(bool ended_clean);

extern AsyncStmtInfo *GetAsyncStmtInfoByIndex(int i);
extern int GetAsyncStmtInfoUsed(void);
extern AsyncStmtInfo *GetAsyncStmtInfo(Oid shardid);
extern AsyncStmtInfo *GetAsyncStmtInfoNode(Oid shardid, Oid shardNodeId, bool req_chk_onfail);

/**
 * @brief Stmthandle with epoch information  
 */
typedef struct
{
	StmtHandle *handle;
	int32_t epoch;
} StmtSafeHandle;

#define RAW_HANDLE(h) (h.handle)

#define INVALID_STMT_HANLE ((StmtSafeHandle){NULL, 0})

static inline bool stmt_handle_valid(StmtSafeHandle handle) { return handle.handle != NULL; }

/**
 * @brief Sets the ignored errors for subsequent statements added by calling add;
 */
extern int set_stmt_ignored_error(int mysql_errno);

/**
 * @brief Append 'stmt' into asi's job queue. 'stmt' will be sent later when its turn comes.
 * 
 * @return A handle of the stmt. t can be used by the caller to receive results from stmt,
 *  which must be released when used up.
 *  
 * NOTE: Handles are automatically released after the (sub)transaction ends, so don't share
 *  handles across (sub)transactions.
 *  If 'ownsit' set to true, caller transfers the responsibility of freeing the memory of stmt to
 *  this function, which automatically frees the memory after the stmt is sent. In this case, 
 *  the function restricts that stmt must be allocated from TopMemoryContext. 
 */
extern StmtSafeHandle send_stmt_async(AsyncStmtInfo *asi, char *stmt, size_t stmt_len,
				      CmdType cmd, bool ownsit, enum enum_sql_command sqlcom, bool materialize) __attribute__((warn_unused_result));

/**
 * @brief Wait until the result of some stmts can be read
 */
extern StmtSafeHandle wait_for_readable_stmt(StmtSafeHandle *handles, int size);

/**
 * @brief Get the next row of the statement
 * 
 * @param handle 		handle returned from send_stmt_async
 * @return MYSQL_ROW  	Null if EOF
 */
extern MYSQL_ROW get_stmt_next_row(StmtSafeHandle handle);

/**
 * @brief Same as get_stmt_next_row(), but no wait if the result is not ready yet 
 */
extern MYSQL_ROW try_get_stmt_next_row(StmtSafeHandle handle);

/**
 * @brief Get the field types of stmt result
 */
enum enum_field_types *get_stmt_field_types(StmtSafeHandle handle);

/**
 * @brief Get the row lengths array of the current row
 * 
 * @param handle 		handle returned from send_stmt_async
 */
size_t *get_stmt_row_lengths(StmtSafeHandle handle);

/**
 * @brief Get the affected rows of the statement
 */
extern int32 get_stmt_affected_rows(StmtSafeHandle handle);

/**
 * @brief Rewind the result of the statement
 * 
 * @param handle 		handle returned from send_stmt_async
 */
extern void stmt_rewind(StmtSafeHandle handle);

/**
 * @brief Check if the statement is rewindable
 * 
 * @param handle 		handle returned from send_stmt_async
 */
extern bool is_stmt_rewindable(StmtSafeHandle handle);

/**
 * @brief Check EOF the the statement
 * 
 * @param handle 		handle returned from send_stmt_async
 */
extern bool is_stmt_eof(StmtSafeHandle handle);

/**
 * @brief Mark the statement to be canceled, and no longer cares whether it executes
 *    or its returned result (which may still be executed).
 */
extern void cancel_stmt_async(StmtSafeHandle handle);

/**
 * @brief Marked statement to be canceled, and free all the results remained in the sockets
 */
extern void cancel_stmt(StmtSafeHandle handle);

/**
 * @brief Release the statement handle if no longer use it
 */
extern void release_stmt_handle(StmtSafeHandle handle);

/**
 * @brief Append 'stmt' into asi's job queue. 'stmt' will be sent later when its turn comes.
 * 
 *  Same as send_stmt_async(), but not return a handle. Typically used to execute sql 
 *  that does not return results.
 */
extern void send_stmt_async_nowarn(AsyncStmtInfo *asi, char *stmt,
			    size_t stmt_len, CmdType cmd, bool ownsit, enum enum_sql_command sqlcom);

/**
 * @brief Append 'stmt' into asi's job queue. and wait for the completion of 'stmt'
 */
extern void send_remote_stmt_sync(AsyncStmtInfo *asi, char *stmt, size_t len,
				  CmdType cmdtype, bool owns_it, enum enum_sql_command sqlcom, int ignore_err);

/**
 * @brief Send statement to all the shard currently in use, and wait for the completion of the statement
 *
 *   @param written_only Only send to shards which is written is current transaction
 */
extern void send_stmt_to_all_inuse_sync(char *stmt, size_t len, CmdType cmdtype, bool owns_it,
					enum enum_sql_command sqlcom, bool written_only);
/**
 * @brief Send statement to all of the shards in current cluster, and wait for the completion of the statement
 */
extern void send_stmt_to_all_shards_sync(char *stmt, size_t len, CmdType cmdtype, bool owns_it,
					 enum enum_sql_command sqlcom);

/**
 * @brief Wait for all of the statements in queue to be completed
 */
extern void flush_all_stmts(void);

/**
 * @brief Cancel all of the statements in queue, and wait for the completion of the running statements
 * 
 *  Note: 
 *  This is used internally and is mainly used when the asi is finally cleaned up, so even if
 *  the running stmt eventually reports an error, or even if the connection is disconnected,
 *  no exception will be thrown.
 */
extern void cancel_all_stmts(void);

extern uint64_t GetRemoteAffectedRows(void);
extern bool MySQLQueryExecuted(void);

extern uint64_t GetTxnRemoteAffectedRows(void);
extern Oid GetCurrentNodeOfShard(Oid shard);

extern bool IsConnReset(AsyncStmtInfo *asi);
extern void disconnect_storage_shards(void);
extern void request_topo_checks_used_shards(void);

#endif // !SHARDING_CONN_H
