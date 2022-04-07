/*-------------------------------------------------------------------------
 *
 * cluster_meta.c
 *		routines managing cluster states.
 *
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * IDENTIFICATION
 *	  src/backend/sharding/cluster_meta.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "sharding/cluster_meta.h"
#include "sharding/mysql/mysql.h"
#include "sharding/mysql/errmsg.h"
#include "sharding/mysql/mysqld_error.h"
#include "access/htup.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "nodes/nodes.h"
#include "access/genam.h"
#include "access/transam.h"
#include "catalog/pg_ddl_log_progress.h"
#include "utils/syscache.h"
#include "catalog/pg_cluster_meta.h"
#include "catalog/pg_database.h"
#include "utils/fmgroids.h"
#include "utils/relcache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "storage/lwlock.h" 
#include "storage/lockdefs.h"
#include "storage/proc.h"
#include "catalog/indexing.h"
#include "utils/algos.h"
#include "executor/spi.h"
#include "postmaster/bgworker.h"
#include "commands/dbcommands.h"
#include "pgstat.h"
#include "utils/snapmgr.h"
#include "catalog/pg_cluster_meta_nodes.h"
#include "utils/builtins.h"
#include "postmaster/xidsender.h"
#include "storage/ipc.h"

extern int mysql_connect_timeout;
extern int mysql_read_timeout;
extern int mysql_write_timeout;
extern int mysql_max_packet_size;
extern bool mysql_transmit_compress;

Oid cluster_id = 0;
Oid comp_node_id = 0;
/*
 * Cached name of the pg_cluster_meta field.
 * */
NameData g_cluster_name = {'\0'};

/* Only used for error reporting. */
static char *cur_meta_hostaddr = NULL;
static uint16_t cur_meta_port;

static void WaitForDDLTxnCommit(uint64_t opid);
static void fetch_cluster_meta(void);
static bool mysql_get_next_result(MYSQL_CONN *conn, bool isbg);
static bool check_ddl_logs_complete(bool is_recovery, uint64_t min_opid, uint64_t *apply_max, uint64_t *local_max);
struct CMNConnInfo;
static int FindMetaShardAllNodes(struct CMNConnInfo *cmnodes, size_t n);

inline static const char *GetClusterName()
{
	if (cluster_id == 0)
		fetch_cluster_meta();
	return g_cluster_name.data;
}

const char *GetClusterName2()
{
	return GetClusterName();
}

/*
 * @retval true if conn&peer(i.e. the mysql instance) is valid and has
 * expected status, false otherwise.
 * */
bool check_mysql_instance_status(MYSQL_CONN *conn, uint64_t checks, bool isbg)
{
	if (!conn || !conn->connected)
		return false;

	int ret;

	if (checks & CHECK_KEEP_ALIVE) // must always be the 1st one.
	{
		ret = mysql_query(&conn->conn, "select 1");
		if (ret && handle_metadata_cluster_error(conn, !isbg))
			return false;

		// simply consume the result to get correct state in MYSQL conn object.
		MYSQL_RES *res = mysql_store_result(&conn->conn);
		if (res)
		{
			mysql_fetch_row(res);
			mysql_free_result(res);
		}
		else if (handle_metadata_cluster_error(conn, !isbg))
			return false;
	}

	bool result;
	if ((checks & CHECK_IS_READONLY) || (checks & CHECK_NOT_READONLY))
	{

		Assert(!((checks & CHECK_IS_READONLY) && (checks & CHECK_NOT_READONLY)));
		ret = mysql_query(&conn->conn, "select @@read_only");
		if (ret && handle_metadata_cluster_error(conn, !isbg))
			return false;

		MYSQL_RES *res = mysql_store_result(&conn->conn);

		if (res == NULL)
		{
			handle_metadata_cluster_error(conn, !isbg);
			return false;
		}

		MYSQL_ROW row = mysql_fetch_row(res);
		if (row == NULL || row[0] == NULL)
		{
			mysql_free_result(res);
			handle_metadata_cluster_error(conn, !isbg);
			return false;
		}

		char res_fld = row[0][0];
		if ((res_fld == '0' && (checks & CHECK_IS_READONLY)) ||
			(res_fld == '1' && (checks & CHECK_NOT_READONLY)) ||
			(res_fld != '0' && res_fld != '1'))
		{
			result = false;
		}
		else
		{
			result = true;
		}

		mysql_free_result(res);

		if (result == false) return result;
	}

	if (checks & (CHECK_MGR_MASTER | CHECK_RBR_MASTER))
	{
	    const char *stmt = NULL;
		if (checks & CHECK_MGR_MASTER) stmt = "select MEMBER_HOST, MEMBER_PORT from performance_schema.replication_group_members where channel_name='group_replication_applier' and MEMBER_STATE='ONLINE' and MEMBER_ROLE='PRIMARY'";
		else if (checks & CHECK_RBR_MASTER)
		{
			stmt = "select host, port, Channel_name  from mysql.slave_master_info ";
			Assert(false); // TODO: may need extra work.
		}

	    size_t stmtlen = stmt ? strlen(stmt) : 0;

		ret = mysql_real_query(&conn->conn, stmt, stmtlen);
		if (ret)
		{
			handle_metadata_cluster_error(conn, !isbg);
			return false;
		}

		MYSQL_RES *res = mysql_store_result(&conn->conn);
		if (res == NULL)
		{
			mysql_free_result(res);
			handle_metadata_cluster_error(conn, !isbg);
			return false;
		}

		MYSQL_ROW row = mysql_fetch_row(res);
		if (row == NULL || row[0] == NULL || row[1] == NULL)
		{
			/*
			  This is not an error here, when a node isn't in a MGR cluster,
			  this happens.
			 */
			mysql_free_result(res);
			if (row == NULL) handle_metadata_cluster_error(conn, !isbg);
			return false;
		}

		char *endptr = NULL;
		int port = strtol(row[1], &endptr, 10);
		Assert(*endptr == '\0');

		if (strcmp(row[0], conn->conn.host) == 0 && port == conn->conn.port)
			result = true;
		else
			result = false;

		mysql_free_result(res);
		if (result == false)
			return result;
	}

	if (checks & CHECK_SET_NAMES)
	{
		ret = mysql_query(&conn->conn, "SET NAMES 'utf8'");
		if (ret)
			handle_metadata_cluster_error(conn, !isbg);
		ret = mysql_query(&conn->conn, "set session autocommit = true");
		if (ret)
			handle_metadata_cluster_error(conn, !isbg);
		/*
		 * Before mysql-8.0, the name was tx_isolation.
		 * */
		ret = mysql_query(&conn->conn, "set transaction_isolation='read-committed'");
		if (ret)
		{
			conn->ignore_err = ER_UNKNOWN_SYSTEM_VARIABLE;
			if (ER_UNKNOWN_SYSTEM_VARIABLE == handle_metadata_cluster_error(conn, !isbg))
			{
				ret = mysql_query(&conn->conn, "set tx_isolation='read-committed'");
				if (ret)
					handle_metadata_cluster_error(conn, !isbg);
			}
			conn->ignore_err = 0;
		}
		
	}

	return true;
}

/*
 * connect to mysql node. 'is_bg' is true if called by a background process, i.e. xid sender; false
 * if called by backend user session processes.
 * @param check_master 1 to check that the node is a master, 0 to check if the node is a slave; -1 to skip such checks.
 * @retval 0: connected to metadata cluster master node;
 *        -1: connection failed; -2: the mysql instance check for master/slave doesn't match 'check_master'.
 * */
static int connect_mysql(MYSQL_CONN *mysql, const char *host, uint16_t port,
	const char *user, const char *password, bool is_bg, const int check_master)
{
	Assert(check_master == 1 || check_master == 0 || check_master == -1);
	mysql->nrows_affected = 0;
	mysql->nwarnings = 0;
	mysql->result = NULL;
	mysql->connected = false;
	mysql->cmd = CMD_UNKNOWN;
	mysql->inside_err_hdlr = false;
	mysql->ignore_err = 0;

	mysql_init(&mysql->conn);
	//mysql_options(mysql, MYSQL_OPT_NONBLOCK, 0); always sync send stmts to cluster meta nodes.
	mysql_options(&mysql->conn, MYSQL_OPT_CONNECT_TIMEOUT, &mysql_connect_timeout);
	mysql_options(&mysql->conn, MYSQL_OPT_READ_TIMEOUT, &mysql_read_timeout);
	mysql_options(&mysql->conn, MYSQL_OPT_WRITE_TIMEOUT, &mysql_write_timeout);
	mysql_options(&mysql->conn, MYSQL_OPT_MAX_ALLOWED_PACKET, &mysql_max_packet_size);

	if (mysql_transmit_compress)
		mysql_options(&mysql->conn, MYSQL_OPT_COMPRESS, NULL);

	// Never reconnect, because that messes up txnal status.
	my_bool reconnect = 0;
	mysql_options(&mysql->conn, MYSQL_OPT_RECONNECT, &reconnect);
#define MAX_HOSTADDR_LEN 8192 // align with def in metadata tables.
	if (cur_meta_hostaddr == NULL)
		cur_meta_hostaddr = MemoryContextAlloc(TopMemoryContext, MAX_HOSTADDR_LEN);
	strncpy(cur_meta_hostaddr, host, MAX_HOSTADDR_LEN - 1);
	cur_meta_hostaddr[MAX_HOSTADDR_LEN - 1] = '\0';
	cur_meta_port = port;

	/* Returns 0 when done, else flag for what to wait for when need to block. */
	MYSQL *ret = mysql_real_connect(&mysql->conn, host, user, password, NULL,
								  port, NULL, CLIENT_MULTI_STATEMENTS | (mysql_transmit_compress ? MYSQL_OPT_COMPRESS : 0));
	if (!ret)
	{
		RequestShardingTopoCheck(METADATA_SHARDID);
		handle_metadata_cluster_error(mysql, !is_bg);
		return -1;
	}

	mysql->node_type = -1;

	// Whether and how to check master status.
	Storage_HA_Mode ha_mode = storage_ha_mode();
	int master_bit = 0;
	if (ha_mode == HA_MGR) master_bit = CHECK_MGR_MASTER;
	else if (ha_mode == HA_RBR) master_bit = CHECK_RBR_MASTER;

	mysql->connected = true; // check_mysql_instance_status() needs this set to true here.
	if (check_master != -1 &&
		!check_mysql_instance_status(mysql, check_master == 1 ?
			(CHECK_NOT_READONLY | master_bit) : CHECK_IS_READONLY, is_bg))
	{
		/*
		  Recorded node type is not true any more, request topology check.
		*/
		RequestShardingTopoCheck(METADATA_SHARDID);

		close_metadata_cluster_conn(mysql);
		return -2;
	}

	if (check_master == 1 || check_master == 0)
		mysql->node_type = check_master;

	check_mysql_instance_status(mysql, CHECK_SET_NAMES, is_bg);

	elog(LOG, "Connected to metadata cluster mysql instance at %s:%u", host, port);
	return 0;

}

int connect_mysql_master(MYSQL_CONN *mysql, const char *host, uint16_t port, const char *user, const char *password, bool is_bg)
{
	return connect_mysql(mysql, host, port, user, password, is_bg, 1);
}

int connect_mysql_slave(MYSQL_CONN *mysql, const char *host, uint16_t port, const char *user, const char *password, bool is_bg)
{
	return connect_mysql(mysql, host, port, user, password, is_bg, 0);
}


#define IS_MYSQL_CLIENT_ERROR(err) (((err) >= CR_MIN_ERROR && (err) <= CR_MAX_ERROR) || ((err) >= CER_MIN_ERROR && (err) <= CER_MAX_ERROR))

int handle_metadata_cluster_error(MYSQL_CONN *conn, bool throw_error)
{
	int ret = mysql_errno(&conn->conn);
	if (ret == conn->ignore_err || ret == 0 || conn->inside_err_hdlr)
		return ret;

	static char errmsg_buf[512];
	errmsg_buf[0] = '\0';
	strncat(errmsg_buf, mysql_error(&conn->conn), sizeof(errmsg_buf) - 1);

	/*
	  The handling of an error in this function may call another mysql client
	  function(e.g. free_metadata_cluster_result()), which may trigger
	  another mysql client error, causing infinite recursive calls.

	  So don't handle further errors when already inside this function.
	 */
	conn->inside_err_hdlr = true;

	/*
	 * Only break the connection for client errors. Errors returned by server
	 * caused by sql stmt simply report to client, those are not caused by
	 * the connection.
	 * */
	const bool is_mysql_client_error = IS_MYSQL_CLIENT_ERROR(ret);
	if (is_mysql_client_error ||
		(ret == ER_OPTION_PREVENTS_STATEMENT &&
		 (strcasestr(errmsg_buf, "--read-only") ||
		  strcasestr(errmsg_buf, "--super-read-only"))))
	{
		/*
		 * On client error close the connection to keep clean states.
		 *
		 * On master switch and when we have obsolete master info we will
		 * reach here.  And we request topology check to update
		 * pg_shard/pg_shard_node with latest master info.
		 *
		 * Close conn before the exception is thrown.
		 * */
		if (ret == CR_SERVER_LOST || ret == CR_SERVER_GONE_ERROR ||
			ret == CR_SERVER_HANDSHAKE_ERR || ret == ER_OPTION_PREVENTS_STATEMENT)
		    RequestShardingTopoCheck(METADATA_SHARDID);

		if (ret == CR_SERVER_LOST || ret == CR_SERVER_GONE_ERROR)
		{
			ShardConnKillReq *req = makeMetaConnKillReq(2/*query*/, get_cluster_conn_thread_id());
			if (req)
			{
				appendShardConnKillReq(req);
				pfree(req);
			}
		}

		free_metadata_cluster_result(conn);
		close_metadata_cluster_conn(conn);
		elog(LOG, "Disconnected from metadata cluster node because %s.",
			 is_mysql_client_error ? "MySQL client error" : "the MySQL node is not a master");
	}

	free_metadata_cluster_result(conn);
#define NDTYP(conn) (conn->node_type == 1 ? "primary" : (conn->node_type == 0 ? "replica" : "unknown"))
	if (!throw_error)
	{
		if (ret == CR_SERVER_LOST || ret == CR_SERVER_GONE_ERROR)
			elog(WARNING, "Kunlun-db: Connection with metadata shard %s node (%s, %u) is gone: %d, %s.",
				 NDTYP(conn), cur_meta_hostaddr, cur_meta_port, ret, errmsg_buf);
		else if (ret == CR_UNKNOWN_ERROR)
			elog(WARNING, "Kunlun-db: Unknown error from metadata shard %s node (%s, %u) : %d, %s.",
				 NDTYP(conn), cur_meta_hostaddr, cur_meta_port, ret, errmsg_buf);
		else if (ret == CR_COMMANDS_OUT_OF_SYNC)
			elog(WARNING, "Kunlun-db: Command out of sync from metadata shard %s node (%s, %u) : %d, %s.",
				 NDTYP(conn), cur_meta_hostaddr, cur_meta_port, ret, errmsg_buf);
		else if (IS_MYSQL_CLIENT_ERROR(ret))
			elog(WARNING, "Kunlun-db: MySQL client error from metadata shard %s node (%s, %u) : %d, %s.",
				 NDTYP(conn), cur_meta_hostaddr, cur_meta_port, ret, errmsg_buf);
		else
			elog(WARNING, "Kunlun-db: MySQL client error from metadata shard %s node (%s, %u) : %d, %s.",
				 NDTYP(conn), cur_meta_hostaddr, cur_meta_port, ret, errmsg_buf);
		conn->inside_err_hdlr = false;
		return ret;
	}

	conn->inside_err_hdlr = false;
	if (ret == CR_SERVER_LOST || ret == CR_SERVER_GONE_ERROR)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("Kunlun-db: Connection with metadata shard %s node (%s, %u) is gone: %d, %s. Resend the statement.",
						NDTYP(conn), cur_meta_hostaddr, cur_meta_port, ret, errmsg_buf)));
	else if (ret == CR_UNKNOWN_ERROR)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: Unknown MySQL client error from metadata shard %s node (%s, %u) : %d, %s. Resend the statement.",
						NDTYP(conn), cur_meta_hostaddr, cur_meta_port, ret, errmsg_buf)));
	else if (ret == CR_COMMANDS_OUT_OF_SYNC)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: Commands out of sync from metadata shard %s node (%s, %u) : %d, %s. Resend the statement.",
						NDTYP(conn), cur_meta_hostaddr, cur_meta_port, ret, errmsg_buf)));
	else if (IS_MYSQL_CLIENT_ERROR(ret))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: MySQL client error from metadata shard %s node (%s, %u) : %d, %s. Resend the statement.",
						NDTYP(conn), cur_meta_hostaddr, cur_meta_port, ret, errmsg_buf)));
	else
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: MySQL client error from metadata shard %s node (%s, %u) : %d, %s.",
						NDTYP(conn), cur_meta_hostaddr, cur_meta_port, ret, errmsg_buf)));
	conn->inside_err_hdlr = false;
	return ret;
}

/*
 * Send SQL statement [stmt, stmt_len) to cluster meta node in sync, never asyncly, because
 * 1. for worker processes, they only write when executing DDLs, which are
 *    rare and time consuming any way.
 * 2. for bg xid sender processes, the sender has nothing else to do besides
 *    sending the stmts.
 * 'isbg' is true if called by a background process(xidsender), false if
 * called by backend worker processes.
 * @retval if 'isbg' is true, true on error, false on success.
 *         if !isbg, false on success; exception is thrown on error.
 * */
bool send_stmt_to_cluster_meta(MYSQL_CONN *conn, const char *stmt, size_t len, CmdType cmd, bool isbg)
{
	if (!conn->connected)
	{
		if (isbg)
			return true;
		else
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
					errmsg("Kunlun-db: Invalid metadata cluster connection state.")));
	}

	// previous result must have been freed.
	Assert(conn->result == NULL);
	conn->nrows_affected = 0;
	conn->nwarnings = 0;
	conn->cmd = cmd;

	int ret = mysql_real_query(&conn->conn, stmt, len);
	if (ret != 0)
	{
		handle_metadata_cluster_error(conn, !isbg);
		return true;
	}
	if (handle_metadata_cluster_result(conn, isbg))
		return true;
	return false;
}

void free_metadata_cluster_result(MYSQL_CONN *conn)
{
	// all results can be mixed together.
	//Assert(conn->result && conn->cmd == CMD_SELECT);

	//if (mysql_more_results(&conn->conn))
	//    mysql_reset_connection(&conn->conn);

	// consume remaining results if any to keep mysql conn state clean.
	// reset_conn() doesn't work like expected in this case.

	while (mysql_get_next_result(conn, false))
		;

	// free the last result
	if (conn->result)
	{
		mysql_free_result(conn->result);
		conn->result = NULL;
	}


	conn->cmd = CMD_UNKNOWN;
}

/*
 * @retval: whether there are more results of any stmt type.
 * */
static bool mysql_get_next_result(MYSQL_CONN *conn, bool isbg)
{
	int status = 0;

	if (conn->result) {
		mysql_free_result(conn->result);
		conn->result = NULL;
	}

	while (true)
	{
		if (mysql_more_results(&conn->conn))
		{
			if ((status = mysql_next_result(&conn->conn)) > 0)
			{
				handle_metadata_cluster_error(conn, !isbg);
				return false;
			}
		}
		else
			return false;

		conn->nwarnings += mysql_warning_count(&conn->conn);
		MYSQL_RES *mysql_res = mysql_use_result(&conn->conn);
		if (mysql_res)
		{
			conn->result = mysql_res;
			break;
		}
		else if (mysql_errno(&conn->conn))
		{
			handle_metadata_cluster_error(conn, !isbg);
		}
		else// this query isn't a select stmt
			if (mysql_field_count(&conn->conn) != 0)
			{
				Assert(false);
				return false;
			}

	}

	return true;
}

/*
 * Receive mysql result from mysql server.
 * For SELECT stmt, make MYSQL_RES result ready to conn->result; For others,
 * update affected rows.
 *
 * @retval if 'isbg' is true, true on error, false on success.
 *         if !isbg, false on success; exception is thrown on error.
 * */
bool handle_metadata_cluster_result(MYSQL_CONN *conn, bool isbg)
{
	int status = 1;

	if (conn->cmd == CMD_SELECT)
	{
		/*
		 * The old impl can only allow one SELECT STMT after N other types of stmts,
		 * now there is no such restriction. there can be as many SELECT stmts
		 * as needed among N other types of stmts in any order.
		 *
		 * Iff the cmd isn't a SELECT stmt, mysql_use_result() returns NULL and
		 * mysql_errno() is 0.
		 * */
		MYSQL_RES *mysql_res = mysql_use_result(&conn->conn);
		if (mysql_res)
		{
			if (conn->result)
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("Kunlun-db: Invalid metadata cluster connection state.")));
			else
			{
				conn->nwarnings += mysql_warning_count(&conn->conn);
				conn->result = mysql_res;
				goto end;
			}
		}
		else if (mysql_errno(&conn->conn))
		{
			handle_metadata_cluster_error(conn, !isbg);
		}
		else
			Assert(mysql_field_count(&conn->conn) == 0);

		/*
		 * The 1st result isn't SELECT result, fetch more for it.
		 * */
		if (!mysql_get_next_result(conn, isbg) && !conn->result)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Kunlun-db: A SELECT statement returned no results.")));
		}
	}
	else
	{
		do {
	        uint64_t n = mysql_affected_rows(&conn->conn);
	        conn->nwarnings = mysql_warning_count(&conn->conn);
	        if (n == (uint64_t)-1)
			{
	           handle_metadata_cluster_error(conn, !isbg);
			   return true;
			}
	        conn->nrows_affected += n;
	        // TODO: handle RETURNING result later, and below Assert will need be removed.
	        Assert(mysql_field_count(&conn->conn) == 0);
			/*
			 * more results? -1 = no, >0 = error, 0 = yes (keep looping)
			 * Note that mariadb's client library doesn't return -1 to indicate
			 * no more results, we have to call mysql_more_results() to see if
			 * there are more results.
			 * */
			if (mysql_more_results(&conn->conn))
			{
			    if ((status = mysql_next_result(&conn->conn)) > 0)
				{
	                handle_metadata_cluster_error(conn, !isbg);
					return true;
				}
			}
			else
				break;
		} while (status == 0);
	}
end:
	return false;
}

TransactionId get_max_txnid_cluster_meta(MYSQL_CONN *conn, bool *completed)
{
	*completed = false;
	char stmt[256];

	int ret = snprintf(stmt, sizeof(stmt),
		"select max(txn_id) as max_txnid from " KUNLUN_METADATA_DBNAME ".commit_log_%s where comp_node_id=%u",
		GetClusterName(), comp_node_id);
	Assert(ret < sizeof(stmt));
	TransactionId res = InvalidTransactionId;
	bool done = !send_stmt_to_cluster_meta(conn, stmt, ret, CMD_SELECT, true);

	if (done)
	{
		Assert(conn->result);
		MYSQL_ROW row = mysql_fetch_row(conn->result);
		
		if (!row)
		{
			// this query must return a row
			handle_metadata_cluster_error(conn, false);
			goto end;
		}

		char *endptr = NULL;
		if (row && row[0])
		{
			res = strtoul(row[0], &endptr, 10);
			res = (res & 0x00000000ffffffff);
			elog(INFO, "Found max transaction id %u from " KUNLUN_METADATA_DBNAME ".commit_log_%s", res, GetClusterName());
		}
		// OK to return NULL row[0], ie. no rows in commit_log.
		*completed = true;

		free_metadata_cluster_result(conn);
	}
end:
	return res;
}


static void fetch_cluster_meta()
{
	bool need_txn = false;
	if (!IsTransactionState())
	{
		need_txn = true;
	}

	if (need_txn)
	{    
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());
	}

	HeapTuple ctup = SearchSysCache1(CLUSTER_META, comp_node_id);
	if (!HeapTupleIsValid(ctup))
	{
		ereport(ERROR, 
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: Cache lookup failed for pg_cluster_meta by comp_node_id %u", comp_node_id),
				 errhint("comp_node_id variable must equal to pg_cluster_meta's single row's comp_node_id field.")));
	}

	Form_pg_cluster_meta cmeta = (Form_pg_cluster_meta)GETSTRUCT(ctup);
	cluster_id = cmeta->cluster_id;
	g_cluster_name = cmeta->cluster_name;

	ReleaseSysCache(ctup);

	if (need_txn)
	{
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
	}
}

/*
 * A backend process only interacts with metadata cluster to execute DDL
 * statements, which are rare; and metadata cluster can be connected by
 * throusands, so disconnect right after use in backend processes.
 * */
void close_metadata_cluster_conn(MYSQL_CONN* conn)
{
	/*
	  Make sure connection state is right. Results can be released when
	  connection is closed. Can't close a mysql connection multiple times.
	  Error could be thrown while retrieving results and work on it,
	  e.g. applying received DDL stmts.
	*/
	if (conn->result)
	{
		Assert(conn->connected);
		conn->result = NULL;
	}

	if (conn->connected)
	{
		mysql_close(&conn->conn);
		conn->connected = false;
	}
}

typedef struct CMNConnInfo {
	NameData usr;
	char *hostaddr;
	char* pwd;
	uint16_t port;
	bool is_primary;
	Oid nodeid;
} CMNConnInfo;

/*
  Connect to (ip, port) and fetch the target master M's ip&port that it knows,
  and find M from [cis , cis + num_ci) array and return the array index.
  @retval >=0 : M's index in ci array;
		-1: M not found in array cis.
  		-2: can't find valid ip&port from (ip,port) node.
		-3: can't connect to ci
*/
static int check_metashard_master(MYSQL_CONN *cnconn, CMNConnInfo *ci, CMNConnInfo *cis, int num_ci)
{
	int cret = connect_mysql(cnconn, ci->hostaddr, ci->port, ci->usr.data, ci->pwd, true, -1);
	if (cret < 0)
	{
		Assert(cret == -1);
		return -3;
	}
	Assert(cret == 0);
	cnconn->node_type = (ci->is_primary ? 1 : 0);
	const char *stmt = NULL;
	Storage_HA_Mode ha_mode = storage_ha_mode();

	if (ha_mode == HA_MGR) stmt = "select MEMBER_HOST, MEMBER_PORT from performance_schema.replication_group_members where channel_name='group_replication_applier' and MEMBER_STATE='ONLINE' and MEMBER_ROLE='PRIMARY'";
	else if (ha_mode == HA_RBR)
	{
		stmt = "select host, port, Channel_name from mysql.slave_master_info ";
		Assert(false);
	}
	else
	{
		Assert(ha_mode == HA_NO_REP);// the only master is itself.
		close_metadata_cluster_conn(cnconn);
		return 0;
	}

	size_t stmtlen = 0;
	if (stmt) stmtlen = strlen(stmt);

	bool done = !send_stmt_to_cluster_meta(cnconn, stmt, stmtlen, CMD_SELECT, true);
	int ret = -1;

	if (done)
	{
		Assert(cnconn->result);
		MYSQL_ROW row = mysql_fetch_row(cnconn->result);

		// row may be NULL, it may or may not be an error.
		if (!row && handle_metadata_cluster_error(cnconn, false))
			return -3;

		if (!row || !row[0] || !row[1])
		{
			ret = -2;
			goto end;
		}

		char *endptr = NULL;
		const char *hostaddr = row[0];
		uint16_t port = strtoul(row[1], &endptr, 10);
		for (int i = 0; i < num_ci; i++)
		{
			if (strcmp(cis[i].hostaddr, hostaddr) == 0 && port == cis[i].port)
			{
				ret = i;
				break;
			}
		}

		// when (ip, port) not found, ret is -1 here as expected.
		if (ret == -1)
			elog(WARNING, "Found a new primary node(%s, %u) of metadata shard not in pg_cluster_meta_node, "
				 "meta data in pg_cluster_meta_node isn't up to date, retry later.", hostaddr, port);

end:
		free_metadata_cluster_result(cnconn);
	}
	else
		ret = -3;// connection broken before sending the stmt.
	close_metadata_cluster_conn(cnconn);
	return ret;
}


/*
  Connect to each node of the metadata shard and find which one is the latest master
  and return the master node id via pmaster_nodeid.


  @retval >=0: Number of masters found.
		  if more than one masters found, the 1st one
		  is returned via pmaster_nodeid and the rest are ignored, and warning
		  msg is logged in this case, and caller need to retry later.
		  -1: found a primary node not stored in pg_cluster_meta_nodes, caller need retry later.
		  -2: some of the existing established connections broken when sending
		  stmts to it, or other errors. this number is n't for not returned by
		  this function, it's assumed if error thrown out of this function.
*/
int FindCurrentMetaShardMasterNodeId(Oid *pmaster_nodeid, Oid *old_master_nodeid)
{
	Relation cmr = heap_open(ClusterMetaNodesRelationId, RowExclusiveLock);

	HeapTuple tup = NULL;
	SysScanDesc scan;

	ScanKeyData key;

	ScanKeyInit(&key,
				Anum_pg_cluster_meta_nodes_cluster_id,
				BTEqualStrategyNumber,
				F_OIDEQ, cluster_id);
	scan = systable_beginscan(cmr, InvalidOid, false, NULL, 1, &key);
	static CMNConnInfo cmnodes[MAX_META_SHARD_NODES];
	int cur_idx = 0;
	Oid master_nodeid = InvalidOid;
	bool isNull;

	*old_master_nodeid = InvalidOid;
	while (cur_idx < MAX_META_SHARD_NODES && (tup = systable_getnext(scan)) != NULL)
	{
		Form_pg_cluster_meta_nodes cmn = ((Form_pg_cluster_meta_nodes) GETSTRUCT(tup));

		CMNConnInfo *pci = cmnodes + cur_idx;
		Datum hostaddr_dat = SysCacheGetAttr(CLUSTER_META_NODES, tup,
			Anum_pg_cluster_meta_nodes_hostaddr, &isNull);
		Assert(!isNull);

		pci->usr = cmn->user_name;
		Datum pwd_value = heap_getattr(tup, Anum_pg_cluster_meta_nodes_passwd,
			RelationGetDescr(cmr), &isNull);

		MemoryContext old_memcxt = MemoryContextSwitchTo(CurTransactionContext);
		pci->hostaddr = TextDatumGetCString(hostaddr_dat);
		if (!isNull)
			pci->pwd = TextDatumGetCString(pwd_value); // palloc's memory for the string
		else
			pci->pwd = NULL;
		MemoryContextSwitchTo(old_memcxt);

		pci->port = cmn->port;
		pci->nodeid = cmn->server_id;
		if (cmn->is_master)
		{
			if (*old_master_nodeid != InvalidOid)
			{
				elog(ERROR, "Multiple primary nodes found in pg_cluster_meta_nodes: node IDs: %u and %u.",
					 *old_master_nodeid, cmn->server_id);
				Assert(false);
			}
			*old_master_nodeid = cmn->server_id;
		}
		pci->is_primary = cmn->is_master;
		cur_idx++;
	}
	/*
	  if the metadata shard has more than MAX_META_SHARD_NODES, we won't
	  consult the extras for their known master node. the only issue that
	  could thus arise is a neglected brain split, which is trivially possible
	  for MGR.
	*/

	systable_endscan(scan);
	heap_close(cmr, RowExclusiveLock);

	int num_nodes = cur_idx;
	int num_masters = 0; // NO. of unique master nodes found from storage shard nodes.
	int num_quorum = 0; // NO. of shard nodes which affirm master_nodeid to be new master.
	int num_unknowns = 0; // NO. of shard nodes who doesn't know about current master.
	int num_new_masters = 0; // NO. of found masters not in pg_shard_node
	int num_unavails = 0; // NO. of nodes can't be connected
	
	// check_metashard_master always only used/valid in each call of check_metashard_master().
	static MYSQL_CONN cnconn;
	
	const char *master_ip = NULL;
	uint16_t master_port = 0;
	/*
	  Check every node and see which is master.
	*/
	for (int i = 0; i < num_nodes; i++)
	{
		CMNConnInfo *cmn = cmnodes + i;

		int master_idx = check_metashard_master(&cnconn, cmn, cmnodes, num_nodes);
		/* If the connection is still invalid, close the connection to avoid possible link leaks.*/
		if (cnconn.connected)
		{
			close_metadata_cluster_conn(&cnconn);
		}

		if (master_idx == -1) // new master
		{
			num_new_masters++;
		}
		else if (master_idx == -2) // master unknown in this node
		{
			elog(WARNING, "Primary node unknown in metadata shard node (%s, %u, %u).",
				 cmn->hostaddr, cmn->port, cmn->nodeid);
			num_unknowns++;
		}
		else if (master_idx == -3)
		{
			elog(WARNING, "Metadata shard node (%s, %u, %u) can't be connected.",
				 cmn->hostaddr, cmn->port, cmn->nodeid);
			num_unavails++;
		}
		else if (master_nodeid == InvalidOid) // 1st master
		{
			master_nodeid = cmnodes[master_idx].nodeid;
			master_ip = cmnodes[master_idx].hostaddr;
			master_port = cmnodes[master_idx].port;
			Assert(num_masters == 0);
			num_masters++;
			num_quorum++;
		}
		else if (master_nodeid != cmnodes[master_idx].nodeid)
		{
			elog(WARNING, "Found a new primary node(%s, %u, %u) of metadata shard when we already found a new primary node (%s, %u, %u),"
			 	 " might be a brain split bug of MGR, but more likely a master switch is happening right now, retry later.",
			 	 cmn->hostaddr, cmn->port, cmn->nodeid, master_ip, master_port, master_nodeid);
			num_masters++;
		}
		else
		{
			Assert(master_nodeid == cmnodes[master_idx].nodeid);
			num_quorum++;
		}
	}

	if (num_new_masters > 0)
	{
		elog(WARNING, "Found %d new primary nodes in metadata shard which are not registered in pg_cluster_meta/pg_cluster_meta_nodes and can't be used by current computing node. Primary node is unknown in %d nodes, with %d unavailable nodes. Retry later.", num_new_masters, num_unknowns, num_unavails);
		num_masters = -1;
		goto end;
	}
	if (num_masters == 0)
		elog(WARNING, "Primary node not found in metadata shard, it's unknown in %d nodes, with %d unavailable nodes. Retry later.", num_unknowns, num_unavails);
	if (num_masters > 1)
		elog(WARNING, "Multiple(%d) primary nodes found in metadata shard. It's unknown in %d nodes, with %d unavailable nodes. Retry later.", num_masters, num_unknowns, num_unavails);
	if (num_masters == 1)
		elog(LOG, "Found new primary node (%s, %u, %u) in metadata shard, affirmed by %d nodes of the shard. The primary is unknown in %d nodes, with %d unavailable nodes.",
			 master_ip, master_port, master_nodeid, num_quorum, num_unknowns, num_unavails);

	*pmaster_nodeid = master_nodeid;
end:
	
	for (int i = 0; i < num_nodes; i++)
	{
		CMNConnInfo *cmn = cmnodes + i;
		pfree(cmn->hostaddr);
		pfree(cmn->pwd);
	}
	return num_masters;
}


/*
  Find current master id by querying every known nodes of metadata shard about
  its current known master.
  If not same as in pg_cluster_meta/pg_cluster_meta_nodes, update master info
  of the 2 tables.
  @retval true on error, false on sucess.
*/
bool UpdateCurrentMetaShardMasterNodeId()
{
	bool ret = true, end_txn = false;
	Oid master_nodeid = InvalidOid, old_master_nodeid = InvalidOid;
	char strmsg[64];

	SetCurrentStatementStartTimestamp();
	if (!IsTransactionState())
	{
		StartTransactionCommand();
		end_txn = true;
	}
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	snprintf(strmsg, sizeof(strmsg), "Finding and updating new primary node for metadata shard.");
	pgstat_report_activity(STATE_RUNNING, strmsg);
	int num_masters = 0;
	PG_TRY();
	{
		if (cluster_id == 0)
			fetch_cluster_meta();
		num_masters = FindCurrentMetaShardMasterNodeId(&master_nodeid, &old_master_nodeid);
	}
	PG_CATCH();
	{
		num_masters = -2;
		HOLD_INTERRUPTS();
		downgrade_error();
		errfinish(0);
		FlushErrorState();
		RESUME_INTERRUPTS();
	}
	PG_END_TRY();

	if (num_masters != 1)
		goto end;
	if (old_master_nodeid == master_nodeid)
	{
		ret = false;
		elog(DEBUG1, "Found new primary node of metadata shard the same as old primary(%u) in pg_cluster_meta.", master_nodeid);
		goto end;
	}

	// Step 1: Update pg_cluster_meta.master_id field to master_nodeid.
	Relation cmr = heap_open(ClusterMetaRelationId, RowExclusiveLock);

	uint64_t ntups = 0;
	HeapTuple tup = NULL, tup0 = 0;
	SysScanDesc scan;
	ScanKeyData keys[2];

	ScanKeyInit(&keys[0],
				Anum_pg_cluster_meta_comp_node_id,
				BTEqualStrategyNumber,
				F_OIDEQ, comp_node_id);

	ScanKeyInit(&keys[1],
				Anum_pg_cluster_meta_cluster_id,
				BTEqualStrategyNumber,
				F_OIDEQ, cluster_id);

	scan = systable_beginscan(cmr, InvalidOid, false, NULL, 2, keys);

	while ((tup = systable_getnext(scan)) != NULL)
	{
		ntups++;
		tup0 = tup;
		break;
	}

	if (ntups == 0)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: Can not find row in pg_cluster_meta")));
		goto end;
	}

	if (ntups > 1)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: Found %lu valid rows in pg_cluster_meta, but only 1 row is expected.", ntups)));
		goto end;
	}

	Datum values[Natts_pg_cluster_meta];
	bool nulls[Natts_pg_cluster_meta];
	bool replaces[Natts_pg_cluster_meta];
	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));
	memset(replaces, 0, sizeof(replaces));

	replaces[Anum_pg_cluster_meta_cluster_master_id - 1] = true;
	values[Anum_pg_cluster_meta_cluster_master_id - 1] = UInt32GetDatum(master_nodeid);

	HeapTuple newtuple =
		heap_modify_tuple(tup0, RelationGetDescr(cmr),
	                      values, nulls, replaces);
	CatalogTupleUpdate(cmr, &newtuple->t_self, newtuple);
	systable_endscan(scan);
	heap_close(cmr, RowExclusiveLock);

	// Step 2: update pg_cluster_meta_nodes.is_master field for the rows of old and new master nodes.
	Relation cmnr = heap_open(ClusterMetaNodesRelationId, RowExclusiveLock);
	ScanKeyData key;

	ScanKeyInit(&key,
				Anum_pg_cluster_meta_nodes_cluster_id,
				BTEqualStrategyNumber,
				F_OIDEQ, cluster_id);
	scan = systable_beginscan(cmnr, InvalidOid, false, NULL, 1, &key);
	Datum values1[7] = {0, 0, 0, 0, 0, 0, 0};
	bool nulls1[7] = {false, false, false, false, false, false, false};
	bool replaces1[7] = {false, false, false, false, false, false, false};
	replaces1[Anum_pg_cluster_meta_nodes_is_master - 1] = true;
	while ((tup = systable_getnext(scan)) != NULL)
	{
		Form_pg_cluster_meta_nodes cmn = ((Form_pg_cluster_meta_nodes) GETSTRUCT(tup));
		if (cmn->server_id == master_nodeid)
		{
			//Assert(cmn->is_master == false);
			values1[Anum_pg_cluster_meta_nodes_is_master - 1] = BoolGetDatum(true);
			HeapTuple newtuple1 =
				heap_modify_tuple(tup, RelationGetDescr(cmnr),
								  values1, nulls1, replaces1);
			CatalogTupleUpdate(cmnr, &newtuple1->t_self, newtuple1);
		}
		else if (cmn->server_id == old_master_nodeid)
		{
			//Assert(cmn->is_master == true);
			values1[Anum_pg_cluster_meta_nodes_is_master - 1] = BoolGetDatum(false);
			HeapTuple newtuple2 =
				heap_modify_tuple(tup, RelationGetDescr(cmnr),
								  values1, nulls1, replaces1);
			CatalogTupleUpdate(cmnr, &newtuple2->t_self, newtuple2);
		}
	}

	systable_endscan(scan);
	heap_close(cmnr, RowExclusiveLock);
	ret = false;
end:
	SPI_finish();
	PopActiveSnapshot();
	if (end_txn) CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
	if (num_masters == 1 && old_master_nodeid != master_nodeid)
		elog(LOG, "Updated primary node id of metadata shard from %u to %u",
			 old_master_nodeid, master_nodeid);
	return ret;
}


void KillMetaShardConn(char type, uint32_t connid)
{
	Assert(type == 1 || type == 2);
	static CMNConnInfo cmnodes[MAX_META_SHARD_NODES];
	static MYSQL_CONN mysql_conn;
	MYSQL_CONN *cnconn = &mysql_conn;

	char stmt[64];
	int slen = snprintf(stmt, sizeof(stmt), "kill %s %u",
		type == 1 ? "connection" : "query", connid);
	Assert(slen < sizeof(stmt));

	int cnt = FindMetaShardAllNodes(cmnodes, MAX_META_SHARD_NODES);
	for (int i = 0; i < cnt; i++)
	{
		CMNConnInfo *ci = cmnodes+i;
		int cret = connect_mysql(cnconn, ci->hostaddr, ci->port, ci->usr.data, ci->pwd, true, -1);
		if (cret < 0)
		{
			continue;
		}
		Assert(cret == 0);
		cnconn->ignore_err = ER_NO_SUCH_THREAD;
		cnconn->node_type = (ci->is_primary ? 1 : 0);
	    bool done = !send_stmt_to_cluster_meta(cnconn, stmt, slen, CMD_UTILITY, true);
		cnconn->ignore_err = 0;
		close_metadata_cluster_conn(cnconn);
		pfree(ci->pwd);
		pfree(ci->hostaddr);

		elog(INFO, "%s %sconnection %u on metadata cluster %s node (%s, %d) as requested.",
			done ? "Killed" : "Failed to kill", type == 1 ? "" : "query on ",
			connid, NDTYP(cnconn), ci->hostaddr, ci->port);
	}
}

static int FindMetaShardAllNodes(CMNConnInfo *cmnodes, size_t n)
{
	bool need_txn = false;
	MemoryContext curctx = CurrentMemoryContext;
	if (!IsTransactionState())
	{
		need_txn = true;
	}

	if (need_txn)
	{    
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());
	}
	
	Relation cmr = heap_open(ClusterMetaNodesRelationId, RowExclusiveLock);

	HeapTuple tup = NULL;
	SysScanDesc scan;

	ScanKeyData key;

	ScanKeyInit(&key,
				Anum_pg_cluster_meta_nodes_cluster_id,
				BTEqualStrategyNumber,
				F_OIDEQ, cluster_id);
	scan = systable_beginscan(cmr, InvalidOid, false, NULL, 1, &key);
	int cur_idx = 0;
	bool isNull;

	while (cur_idx < MAX_META_SHARD_NODES && (tup = systable_getnext(scan)) != NULL)
	{
		Form_pg_cluster_meta_nodes cmn = ((Form_pg_cluster_meta_nodes) GETSTRUCT(tup));
		CMNConnInfo *pci = cmnodes + cur_idx;

		Datum hostaddr_dat = SysCacheGetAttr(CLUSTER_META_NODES, tup,
			Anum_pg_cluster_meta_nodes_hostaddr, &isNull);
		Assert(!isNull);

		pci->usr = cmn->user_name;

		Datum pwd_value = heap_getattr(tup, Anum_pg_cluster_meta_nodes_passwd,
			RelationGetDescr(cmr), &isNull);

		MemoryContext old_memcxt = MemoryContextSwitchTo(CurTransactionContext);
		pci->hostaddr = TextDatumGetCString(hostaddr_dat);
		pci->hostaddr = MemoryContextStrdup(curctx, pci->hostaddr);
		if (!isNull)
		{
			// pwd should be alloc'ed in curctx rather than the current txn ctx
			// which will be destroyed at the end of this func.
			pci->pwd = TextDatumGetCString(pwd_value); // palloc's memory for the string
			pci->pwd = MemoryContextStrdup(curctx, pci->pwd);
		}
		else
			pci->pwd = NULL;
		MemoryContextSwitchTo(old_memcxt);

		pci->port = cmn->port;
		pci->nodeid = cmn->server_id;
		pci->is_primary = cmn->is_master;
		cur_idx++;
	}

	systable_endscan(scan);
	heap_close(cmr, RowExclusiveLock);
	if (need_txn)
	{
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
	}
	return cur_idx;
}

Storage_HA_Mode storage_ha_mode()
{
	bool need_txn = !IsTransactionState();
	if (need_txn)
	{
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
	}

	HeapTuple ctup = SearchSysCache1(CLUSTER_META, comp_node_id);
	if (!HeapTupleIsValid(ctup))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: Cache lookup failed for pg_cluster_meta by comp_node_id %u", comp_node_id),
				 errhint("comp_node_id variable must equal to pg_cluster_meta's single row's comp_node_id field.")));
	}
	Form_pg_cluster_meta cmeta = (Form_pg_cluster_meta)GETSTRUCT(ctup);
	Storage_HA_Mode ret = cmeta ? cmeta->ha_mode : HA_NO_REP;
	if (ctup) ReleaseSysCache(ctup);
	if (need_txn)
	{
		PopActiveSnapshot();
		CommitTransactionCommand();
	}
	return ret;
}

