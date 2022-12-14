# Copyright (c) 2019 ZettaDB inc. All rights reserved.
# This source code is licensed under Apache 2.0 License,
# combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

import os
import psycopg2
import mysql.connector
import argparse
import json
import time
import threading
import signal

# test storage shard and metadata shard master timeout handling in computing nodes.
# the Kunlun DDC must be well created, with 2 storage shards, at least 1 computing node
# and a metadata cluster, and also require cluster_mgr running.
# Must use debug build for kunlun-percona-mysql and all nodes of the 2
# storage shards must have 'debug-sync-timeout=N' setting to enable debug_sync.

def prepare_data(conn):
	cur = conn.cursor()

	# these blockers may be left uncleaned by previous runs. the 2 threads have not been started yet.
	#meta_thd.append_mysql_stmt("set session debug_sync='now signal resume'", 0)
	###mysql_thd.append_mysql_stmt("set session debug_sync='now signal resume'", 0)
	exec_stmt(conn, cur, "set session_debug=''")
	exec_stmt(conn, cur, "set global_debug=''")
	#time.sleep(3)

	# clean up left data by last run
	cur.execute("drop table if exists test_timeout_tbl")

	cur.execute("create table test_timeout_tbl(a int primary key, b int) partition by hash(a)")
	cur.execute("CREATE TABLE test_timeout_tbl_part_0 PARTITION OF test_timeout_tbl FOR VALUES WITH (MODULUS 4, REMAINDER 0)")
	cur.execute("CREATE TABLE test_timeout_tbl_part_1 PARTITION OF test_timeout_tbl FOR VALUES WITH (MODULUS 4, REMAINDER 1)")
	cur.execute("CREATE TABLE test_timeout_tbl_part_2 PARTITION OF test_timeout_tbl FOR VALUES WITH (MODULUS 4, REMAINDER 2)")
	cur.execute("CREATE TABLE test_timeout_tbl_part_3 PARTITION OF test_timeout_tbl FOR VALUES WITH (MODULUS 4, REMAINDER 3)")
	cur.execute("start transaction")
	for i in range(16):
		cur.execute("insert into test_timeout_tbl values({},{})".format(i, 100+i))
	cur.execute("commit")
	cur.close()


def test_teardown(conn):
	cur = conn.cursor()
	cur.execute("drop table test_timeout_tbl")
	cur.close()

# execute 'stmt' via cursor 'cur'. if an error returned, return if not 'txnal', reexecute it if 'txnal'
# return True if stmt executed successfully; false if stmt execution failed.
def exec_stmt(conn, cur, stmt):
	try:
		cur.execute(stmt)
		print stmt
		return True
	except psycopg2.Error as pgerr:
		print "Statement '{}' execution failed: {}".format(stmt, str(pgerr))
		return False

class Mysql_thread(threading.Thread):
	def __init__(self, name):
		threading.Thread.__init__(self)
		self.cond = threading.Condition()
		self.mysql_conns = []
		self.done = False
		self.stmt = ''
		self.secs_later = 0
		self.name = name

	def append_mysql_conn(self, conn_args):
		mconn = mysql.connector.connect(**conn_args)
		mconn.autocommit=True
		self.mysql_conns.append(mconn)

	def set_mysql_conn(self, conn):
		self.mysql_conns.append(conn)

	def append_mysql_stmt(self, stmt, secs_later):
		self.cond.acquire()
		# using a queue of 'one slot', this is sufficient for now, we can
		# later use a queue which sorts stmts by execution time
		while self.stmt != '':
			self.cond.release()
			print "Mysql_thread {}: Waiting 1 second for '{}' to be consumed by Mysql_thread.".format(self.name, self.stmt)
			time.sleep(1)
			secs_later-=1
			self.cond.acquire()

		self.stmt = stmt
		self.secs_later = secs_later
		self.cond.notify()
		self.cond.release()

	def run(self):
		while not self.done:
			self.cond.acquire()
			if self.stmt == '':
				self.cond.wait()
			secs_later = self.secs_later
			stmt = self.stmt
			self.stmt = ''
			self.secs_later = 0
			self.cond.release()

			if secs_later > 0:
				print "Mysql_thread {}: Sleeping {} seconds before executing stmt '{}'".format(self.name, secs_later, stmt)
				time.sleep(secs_later)
			self.exec_stmt(stmt)

	def exec_stmt(self, stmt):
		for conn in self.mysql_conns:
			master_cursor = conn.cursor()
			print "Mysql_thread {} ({}): {}".format(self.name, str(conn), stmt)
			master_cursor.execute(stmt)
			master_cursor.close()


# do insert/delete/select/update in autocommit stmt(stmt txn)
class DML_thread(threading.Thread):
	def __init__(self, conn_args):
		threading.Thread.__init__(self)
		self.conn_args = conn_args
		self.conn = psycopg2.connect(host=conn_args.ip, port=conn_args.port, user=conn_args.usr, database='postgres', password=conn_args.pwd)
		self.conn.autocommit = True
		self.keep_running = True
		self.storage_shards = []
		self.meta_shard = Shard()
		self.meta_shard.set_info(0, 'meta')
		self.get_storage_shards(self.conn)
		self.get_meta_shard(self.conn)
		self.done = False

	def reconnect(self, cur):
		cur.close()
		self.conn.close()
		self.conn = psycopg2.connect(host=self.conn_args.ip, port=self.conn_args.port, user=self.conn_args.usr, database='postgres', password=self.conn_args.pwd)
		self.conn.autocommit = True
		cur = self.conn.cursor()
		print "DML_thread: reconnected"
		return cur

	def add_shard(self, s):
		self.storage_shards.append(s)
	def get_shard_by_id(self, sid):
		for s in self.storage_shards:
			if s.id == sid:
				return s
		return None

	def get_storage_shards(self, conn):
		cur = conn.cursor()
		cur.execute("select n.id , port , shard_id, hostaddr, user_name , passwd , s.name as shard_name,  s.master_node_id from pg_shard_node n, pg_shard s where s.id = n.shard_id")
		shard_nodes = cur.fetchall()
		for shard_node in shard_nodes:
			#sn = ShardNode(shard_node['id'], shard_node['ip'], shard_node['port'], shard_node['user_name'], shard_node['passwd'])
			sn = ShardNode(shard_node[0], shard_node[3], shard_node[1], shard_node[4], shard_node[5])
			sd = self.get_shard_by_id(shard_node[2])
			if sd == None:
				sd = Shard()
				sd.set_info(shard_node[2], shard_node[6])
				self.add_shard(sd)
				sd.set_master_node_id(shard_node[7])
			sd.add_node(sn)

			#connect to storage shard master nodes
			if shard_node[0] == shard_node[7]:
				mysql_conn_params = {
					'host':shard_node[3],
					'port':shard_node[1],
					'user':shard_node[4],
					'password':shard_node[5]
				}
				mysql_thd.append_mysql_conn(mysql_conn_params)

		cur.close()

		for shard in self.storage_shards:
			shard.init()

	def get_meta_shard(self, conn):
		cur = conn.cursor()
		cur.execute("select*from pg_cluster_meta_nodes")
		shard_nodes = cur.fetchall()
		for shard_node in shard_nodes:
			#server_id | cluster_id | is_master | port |    user_name     | hostaddr | passwd
			sn = ShardNode(shard_node[0], shard_node[5], shard_node[3], shard_node[4], shard_node[6])
			self.meta_shard.add_node(sn)
			if shard_node[2]:
				self.meta_shard.set_master_node_id(sn.node_id)
		cur.close()
		self.meta_shard.init()

	def select_rows(self):
		cur = self.conn.cursor()
		try:
			if exec_stmt(self.conn, cur, "select* from test_timeout_tbl order by a"):
				resrows = cur.fetchall()
				for row in resrows:
					print "{} | {}".format(row[0], row[1])
			cur.close()
		except Exception as e:
			print str(e)
			cur.close()

	def run(self):
		cur = self.conn.cursor()
		# set timeouts short for quick test execution
		exec_stmt(self.conn, cur, "set statement_timeout=2000")
		exec_stmt(self.conn, cur, "set mysql_read_timeout=5")
		exec_stmt(self.conn, cur, "set mysql_write_timeout=5")
		exec_stmt(self.conn, cur, "set mysql_connect_timeout=5")
		exec_stmt(self.conn, cur, "set shard session innodb_lock_wait_timeout=4")
		self.select_rows()

		# consume 'resume' signals if any
		exec_stmt(self.conn, cur, "set shard session debug_sync='bgc_before_flush_stage wait_for resume'")
		exec_stmt(self.conn, cur, "update test_timeout_tbl set b=b+1 ")
		exec_stmt(self.conn, cur, "set shard session debug_sync='before_execute_sql_command wait_for resume'")
		exec_stmt(self.conn, cur, "update test_timeout_tbl set b=b+1 ")
		self.select_rows()

		#time.sleep(20)
		sync_pts = []
		sync_pts.append('bgc_before_flush_stage')
		sync_pts.append('before_execute_sql_command')
		for i in range(4):
			exec_stmt(self.conn, cur, "set shard session debug_sync='{} wait_for resume'".format(sync_pts[i%2]))

			# release the waiters right after query times out before query is killed so that
			#the txn commit successful. THere are some randomness here, the query could be
			# killed before it's released. this is true for all below such tricks.
			# when i is 0/1, although the conns to storage shards are killed(kill conn stmt executed),
			#the conns and txns remain alive until cluster_mgr ends them after one or 2 minutes, so
			# we can see 'lock wait timeout' errors.
			if i > 1:
				mysql_thd.append_mysql_stmt("set session debug_sync='now signal resume'", 2)
			exec_stmt(self.conn, cur, "update test_timeout_tbl set b=b+1 ")
			print "Wait 5 secs for this thread's connection's backend connections to storage shards to be killed"
			time.sleep(5)
			self.select_rows()
			if reconnect_after_each_case:
				cur = self.reconnect(cur)
			exec_stmt(self.conn, cur, "begin")
			exec_stmt(self.conn, cur, "update test_timeout_tbl set b=b+1")
			exec_stmt(self.conn, cur, "set shard session debug_sync='{} wait_for resume'".format(sync_pts[i%2]))
			if i > 1:
				mysql_thd.append_mysql_stmt("set session debug_sync='now signal resume'", 2)
			exec_stmt(self.conn, cur, "commit")
			print "Wait 5 secs for this thread's connection's backend connections to storage shards to be killed"
			time.sleep(5)

			self.select_rows()
			if reconnect_after_each_case:
				cur = self.reconnect(cur)

		exec_stmt(self.conn, cur, "set shard session debug_sync='before_execute_sql_command wait_for resume'")
		cur.close()

		# the debug_sync can be consumed in one shard for each sent stmt, so after 2 failed tries we can succeed at 3rd try.
		for i in range(3):
			self.select_rows()

		print "Wait 5 secs for this thread's connection's backend connections to storage shards to be killed"
		time.sleep(5)

		dbgpts = []
		dbgpts.append('test_2nd_phase_timeout')
		dbgpts.append('test_metadata_svr_commit_log_append_timeout') # this one needs global_debug because it's executed by gtss process.
		dbgtype = []
		dbgtype.append('session_debug')
		dbgtype.append('global_debug')


		cur = self.conn.cursor()
		for i in range(4):
			exec_stmt(self.conn, cur, "set {}=''".format(dbgtype[i%2])) # clear first
			exec_stmt(self.conn, cur, "set {}='+d,{}'".format(dbgtype[i%2], dbgpts[i%2]))

			if i == 2:# signal 2 secs later so that the query times out and 'xa commit' can be executed successfully.
				mysql_thd.append_mysql_stmt("set session debug_sync='now signal resume'", 2)
			elif i == 3: # release waiters after stmt times out (the wait for commit log write times out) in computing node and commit log write succeeds.
				meta_thd.append_mysql_stmt("set session debug_sync='now signal resume'", 2)
			exec_stmt(self.conn, cur, "update test_timeout_tbl set b=b+1 ")

			print "Wait 5 secs for this thread's connection's backend connections to storage shards to be killed"
			time.sleep(5)
			self.select_rows()
			if reconnect_after_each_case:
				cur = self.reconnect(cur)

			exec_stmt(self.conn, cur, "begin")
			exec_stmt(self.conn, cur, "update test_timeout_tbl set b=b+1")
			
			if i == 2:# signal 2 secs later so that the stmt times out and 'xa commit' can be executed successfully.
				mysql_thd.append_mysql_stmt("set session debug_sync='now signal resume'", 2)
			elif i == 3: # release waiters after stmt times out (the wait for commit log write times out) in computing node and commit log write succeeds.
				meta_thd.append_mysql_stmt("set session debug_sync='now signal resume'", 2)
			exec_stmt(self.conn, cur, "commit")
			
			print "Wait 5 secs for this thread's connection's backend connections to storage shards to be killed"
			time.sleep(5)
			self.select_rows()
			if reconnect_after_each_case:
				cur = self.reconnect(cur)

		exec_stmt(conn, cur, "set session_debug=''")
		exec_stmt(conn, cur, "set global_debug=''")
		meta_thd.append_mysql_stmt("set session debug_sync='now signal resume'", 0)
		mysql_thd.append_mysql_stmt("set session debug_sync='now signal resume'", 0)
		print "dml thread done"

		meta_thd.done=True
		cur.close()
		self.conn.close()
		self.done = True

class ShardNode:
	def __init__(self, node_id, ip, port, usr, pwd):
		self.node_id = node_id
		self.ip = ip
		self.port = port
		self.usr = usr
		self.pwd = pwd
		self.uuid = ''
		self.mysql_conn = None
	def set_uuid(self, uuid):
		self.uuid = uuid

	def connect_mysql(self):
		if self.mysql_conn != None:
			return self.mysql_conn

		mysql_conn_params = {
			'host':self.ip,
			'port':self.port,
			'user':self.usr,
			'password':self.pwd
		}

		try:
			self.mysql_conn = mysql.connector.connect(**mysql_conn_params)
		except mysql.connector.errors.InterfaceError as err:
			print "Unable to connect to {}, error: {}".format(str(mysql_conn_params), str(err))

		return self.mysql_conn


class Shard:
	def __init__(self):
		self.nodes = []
		self.id = 0
		self.name = ''
		self.master_node_id = 0 #unknown for now

	def set_info(self, sid, name):
		self.id = sid
		self.name = name

	def set_master_node_id(self, nid):
		self.master_node_id = nid
	def get_master_node(self):
		return self.get_node_by_id(self.master_node_id)

	def add_node(self, sn):
		self.nodes.append(sn)

	def get_node_by_ip_port(self, ip, port):
		for node in self.nodes:
			if node.ip == ip and node.port == port:
				return node
		return None

	def get_node_by_id(self, nid):
		for node in self.nodes:
			if node.node_id == nid:
				return node
		return None

	def get_node_by_uuid(self, uuid):
		for node in self.nodes:
			if node.uuid == uuid:
				return node
		return None

	# connect to a mysql shard node to fetch uuids of all nodes of this shard, and also
	# check that shard topology info in computing node is up to date(same as in the storage shrad)
	# only to be called after all shard nodes are added to the shard
	# return True if all checks pass, false if some check fails
	def init_uuids(self):
		master = self.get_node_by_id(self.master_node_id)

		master_conn = master.connect_mysql()
		master_cursor = master_conn.cursor(buffered=True, dictionary=True)
		master_cursor.execute("select*from performance_schema.replication_group_members")
		nodes = master_cursor.fetchall()
		for node in nodes:
			shard_node = self.get_node_by_ip_port(node['MEMBER_HOST'], node['MEMBER_PORT'])
			if shard_node == None:
				print "Shard node ({}:{}) of storage shard unknown in computing node".format(node['MEMBER_HOST'], node['MEMBER_PORT'])
				return False
			shard_node.set_uuid(node['MEMBER_ID'])
			if node['MEMBER_ROLE'] == 'PRIMARY':
				if master.ip != node['MEMBER_HOST'] or master.port != node['MEMBER_PORT']:
					print "Shard ({}, {}) master node in computing node({}:{}) is NOT same as in storage shard({}:{})".format(self.name, self.id, master.ip, master.port, node['MEMBER_HOST'], node['MEMBER_PORT'])
					return False
		master_cursor.close()
		return True

	def init(self):
		self.init_uuids()

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Test storage shard and metadata shard master timeout processing in computing nodes.')
	parser.add_argument('ip', type=str, help="computing node ip")
	parser.add_argument('port', type=int, help="computing node port")
	parser.add_argument('usr', type=str, help="computing node user name")
	parser.add_argument('pwd', type=str, help="computing node password")
	parser.add_argument('reconnect', type=str, help="reconnect after each test case")

	args = parser.parse_args()
	reconnect_after_each_case = int(args.reconnect)
	conn = psycopg2.connect(host=args.ip, port=args.port, user=args.usr, database='postgres', password=args.pwd)
	conn.autocommit = True
	prepare_data(conn)

	mysql_thd = Mysql_thread("storage")
	mysql_thd.start()
	dmlthd = DML_thread(args)
	dmlthd.start()

	meta_thd = Mysql_thread("meta")
	meta_thd.set_mysql_conn(dmlthd.meta_shard.get_master_node().connect_mysql())
	meta_thd.start()

	while not dmlthd.done:
		time.sleep(2)
	dmlthd.join()
	mysql_thd.done = True
	mysql_thd.join();
	meta_thd.join();
	test_teardown(conn)
	conn.close()
