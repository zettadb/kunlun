# Copyright (c) 2019 ZettaDB inc. All rights reserved.
# This source code is licensed under Apache 2.0 License,
# combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

import psutil
import os
import psycopg2
import mysql.connector
import argparse
import json
import time
import threading
import random
import signal

# test storage shard and metadata shard master failover processing in computing nodes.
# the Kunlun DDC must be well created, with 2 storage shards, at least 1 computing node
# and a metadata cluster.

def find_mysqld_proc_by_port(port):
	key = 'port=' + str(port)
	for proc in psutil.process_iter():
		if proc.name() == 'mysqld':
			for cmd in proc.cmdline():
				if cmd.find(key) >= 0:
					return proc.pid, proc.ppid
	return 0,0


def kill_mysqld_proc_by_port(port, kill_pp):
	pid, ppid = find_mysqld_proc_by_port(port)
	if pid == 0:
		print "Can not find mysqld with port={}".format(str(port))
		return
	os.kill(pid, signal.SIGKILL)
	#do not kill mysqld_safe, we can't easily pull it up during the test when shard nodes are on other machines.
	#if kill_pp:
	#	os.kill(ppid)
	return pid,ppid


def prepare_data(conn):
	cur = conn.cursor()

	# clean up left data by last run
	cur.execute("drop table if exists test_failover_tbl")
	cur.execute("drop database if exists dbxx")
	cur.execute("drop schema if exists schemaxx")
	cur.execute("drop table if exists single_tab1")
	cur.execute("drop table if exists part_tab1")

	cur.execute("create table test_failover_tbl(a int primary key, b int) partition by hash(a)")
	cur.execute("CREATE TABLE test_failover_tbl_part_0 PARTITION OF test_failover_tbl FOR VALUES WITH (MODULUS 4, REMAINDER 0)")
	cur.execute("CREATE TABLE test_failover_tbl_part_1 PARTITION OF test_failover_tbl FOR VALUES WITH (MODULUS 4, REMAINDER 1)")
	cur.execute("CREATE TABLE test_failover_tbl_part_2 PARTITION OF test_failover_tbl FOR VALUES WITH (MODULUS 4, REMAINDER 2)")
	cur.execute("CREATE TABLE test_failover_tbl_part_3 PARTITION OF test_failover_tbl FOR VALUES WITH (MODULUS 4, REMAINDER 3)")
	cur.execute("start transaction")
	for i in range(1000):
		cur.execute("insert into test_failover_tbl values({},{})".format(i, 100+i))
	cur.execute("commit")
	cur.close()


def test_teardown(conn):
	cur = conn.cursor()
	cur.execute("drop table test_failover_tbl")
	cur.close()

def kick_start_topocheck(conn):
	ac = conn.autocommit
	conn.autocommit = True
	csr = conn.cursor()
	nretries = 0

	while nretries < 7:
		try:
			nretries = nretries + 1
			csr.execute("update test_failover_tbl set b=b+1")
# sometimes above DML can't trigger a metadata shard topo check because xidsender process has been using the latest master since before last master switch(es) so above stmt will simply succeed.
# so do a DDL to ensure we can trigger a metadata shard topo check.
			csr.execute("create table if not exists kickstarter (a int primary key)")
			csr.execute("drop table if exists kickstarter ")
			time.sleep(0.3)
		except psycopg2.Error as pgerr:
			# now a topo check is incurrd
			print "kick_start_topocheck: {} attempts to incur a topo check.".format(nretries)
			break;

	conn.autocommit = ac
	csr.close()

# execute 'stmt' via cursor 'cur'. if an error returned, return if not 'txnal', reexecute it if 'txnal'
# return True if stmt executed successfully; false if stmt execution failed.
def exec_stmt(conn, cur, stmt, txnal):
	nretries = 0
	while True:
		try:
			cur.execute(stmt)

			#print "Executed statement: '{}' with {} retries.".format(stmt, nretries)

			# We might succeed despite a master switch was just performed.
			# This could happen in the case descrbed in kick_start_topocheck.
			# In such a case start a topology check manually because we will be waiting for
			# metadata tables to be updated which won't happen automatically
			# without a ddl/dml stmt execution error caused by storage node
			# connection errors, and such errors didn't happen in this case.
			# we don't need such operation in real use, only need it in this
			# test, so use a dedicated table and its dml to achieve this.
			if nretries == 0:
				pass
				#kick_start_topocheck(conn)
			else:
				print "Executed statement: '{}' with {} retries.".format(stmt, nretries)
			return True
		except psycopg2.Error as pgerr:
			# in case of a master failover, if this is an autocommit txn, wait a while and reexecute it.
			if txnal:
				print "Autocommit statement '{}' execution failed and will be retried: {}".format(stmt, str(pgerr))
				if cur.closed:
					cur = conn.cursor()
				time.sleep(1)
				nretries = nretries+1
			else:
				print "Statement '{}' execution failed: {}".format(stmt, str(pgerr))
				time.sleep(1) # wait otherwise cpu goes high
				raise pgerr;
				return False


class DB_Operation:
	def __init__(self):
		self.stmt = ''
	def set_stmt_cursor(self, stmt):
		self.stmt = stmt

	def execute(self, conn):
		csr = conn.cursor()
		exec_stmt(conn, csr, self.stmt, True)
		# what if conn is closed, can it reconnect automatically?
		csr.close()


class DB_ops_thread(threading.Thread):
	def __init__(self, conn_args):
		threading.Thread.__init__(self)
		self.conn = psycopg2.connect(host=conn_args.ip, port=conn_args.port, user=conn_args.usr, database='postgres', password=conn_args.pwd)
		self.conn.autocommit = True
		self.keep_running = True
		self.randgen = random.Random()
		self.randgen.seed(int(time.time()))
		self.randgen.jumpahead(7)



# do insert/delete/select/update in autocommit stmt(stmt txn)
class DML_thread1(DB_ops_thread):
	def __init__(self, conn_args):
		DB_ops_thread.__init__(self, conn_args)

	def run(self):
		cur = self.conn.cursor()
		while self.keep_running:
			x = int(1000 + self.randgen.random() * 10000)
			y = int(self.randgen.random() * 1000)
			exec_stmt(self.conn, cur, "insert into test_failover_tbl values({},{})".format(x, 100+x), True)
			if exec_stmt(self.conn, cur, "select* from test_failover_tbl where a={}".format(y), True):
				resrows = cur.fetchall()
			exec_stmt(self.conn, cur, "update test_failover_tbl set b=b+1 where a={}".format(y), True)
			if exec_stmt(self.conn, cur, "select* from test_failover_tbl where a={}".format(x), True):
				resrows = cur.fetchall()
			exec_stmt(self.conn, cur, "delete from test_failover_tbl where a={}".format(x), True)
		cur.close()
		self.conn.close()

# do insert/delete/select/update in explicit txn
class DML_thread2(DB_ops_thread):
	def __init__(self, conn_args):
		DB_ops_thread.__init__(self, conn_args)
	def run(self):
		cur = self.conn.cursor()
		while self.keep_running:
			try:
				exec_stmt(self.conn, cur, "start transaction", False)
	 			x = int(1000 + self.randgen.random() * 10000)
	 			y = int(self.randgen.random() * 1000)
				exec_stmt(self.conn, cur, "insert into test_failover_tbl values({},{})".format(x, 100 + x), False)
				if exec_stmt(self.conn, cur, "select* from test_failover_tbl where a={}".format(y), False):
					resrows = cur.fetchall()
				exec_stmt(self.conn, cur, "update test_failover_tbl set b=b+1 where a={}".format(y), False)
				if exec_stmt(self.conn, cur, "select* from test_failover_tbl where a={}".format(x), False):
					resrows = cur.fetchall()
				exec_stmt(self.conn, cur, "delete from test_failover_tbl where a={}".format(x), False)
				exec_stmt(self.conn, cur, "commit", False)
			#except mysql.connector.errors.Error as err:
			except psycopg2.Error as pgerr:
				if cur.closed:
					cur = self.conn.cursor()
				exec_stmt(self.conn, cur, "rollback", False)
		cur.close()
		self.conn.close()

#execute all supported DDL stmts, make sure we can handle master switch in such scenarios.
class DDL_thread(DB_ops_thread):
	def __init__(self, conn_args):
		DB_ops_thread.__init__(self, conn_args)
	def run(self):
		cur = self.conn.cursor()
		while self.keep_running:
			x = int(1000 + self.randgen.random() * 10000)
			y = int(self.randgen.random() * 1000)
			exec_stmt(self.conn, cur, "create database dbxx", True)
			exec_stmt(self.conn, cur, "create schema schemaxx", True)
			exec_stmt(self.conn, cur, "create table single_tab1(a int primary key, b int)", True)
			exec_stmt(self.conn, cur, "create index single_tab1_b on single_tab1(b)", True)
			exec_stmt(self.conn, cur, "drop index single_tab1_b", True)
			exec_stmt(self.conn, cur, "drop table single_tab1", True)
			exec_stmt(self.conn, cur, "create table part_tab1(a int primary key, b int) partition by hash(a)", True)
			exec_stmt(self.conn, cur, "create table part_tab10 partition of part_tab1 FOR VALUES WITH (MODULUS 2, REMAINDER 0)", True)
			exec_stmt(self.conn, cur, "create table part_tab11 partition of part_tab1 FOR VALUES WITH (MODULUS 2, REMAINDER 1)", True)
			exec_stmt(self.conn, cur, "create index part_tab1_b on part_tab1(b)", True)
			exec_stmt(self.conn, cur, "drop index part_tab1_b", True)
			exec_stmt(self.conn, cur, "drop table part_tab1", True)
			exec_stmt(self.conn, cur, "drop schema schemaxx", True)
			exec_stmt(self.conn, cur, "drop database dbxx", True)
		cur.close()
		self.conn.close()

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

		nretries = 0
		while True:
			try:
				if nretries > 5:
					os.system("{}/startmysql.sh {}".format(mysql_install_path, str(self.port)))
				
				self.mysql_conn = mysql.connector.connect(**mysql_conn_params)
				break
			except mysql.connector.errors.InterfaceError as err:
				print "Unable to connect to {}, error: {}".format(str(mysql_conn_params), str(err))
				nretries += 1
				time.sleep(1)

		return self.mysql_conn

	def kill_mysqld(self):
		pid, ppid = kill_mysqld_proc_by_port(self.port, False)
		print "Killed mysqld of pid {} on {}:{}".format(pid, self.ip, self.port)
		self.mysql_conn = None

	def pullup_mysqld(self):
		conn = self.connect_mysql()
		master_cursor = conn.cursor()

		if have_cluster_manager:
			while True:
				master_cursor.execute("select member_state from performance_schema.replication_group_members where member_host='{}' and member_port={}".format(self.ip, self.port))
				row = master_cursor.fetchone()
				
				if row != None and row[0] != None:
					node_stat = row[0]
				else:
					node_stat = "unknown"

				if node_stat == 'ONLINE':
					break;
				else:
					print "Waiting for cluster_manager to add shard node({}:{} of status {}) back to MGR cluster.".format(self.ip, self.port, node_stat);
					time.sleep(1)
		else:
			master_cursor.execute("start group_replication")
		master_cursor.close()

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

	def kill_all_mysqld(self):
		for node in self.nodes:
			node.kill_mysqld()
	def wait_for_all_mysqld(self):
		for node in self.nodes:
			node.pullup_mysqld()

	#randomly pick a shard node as new master and switch
	def switch_master(self):
		while True:
			master_id = int(random.random()*100) % len(self.nodes)
			new_master = self.nodes[master_id]
			if new_master.node_id != self.master_node_id:
				break;

		conn = new_master.connect_mysql()
		cur = conn.cursor()
		print "new_master uuid:".format(new_master.uuid)
		cur.execute("select group_replication_set_as_primary('{}')".format(new_master.uuid))
		resrow = cur.fetchall();
		#keep it old because this is how it is when master is killed. we must keep such behavior consistent
		#self.master_node_id = new_master.node_id;
		cur.close()
		old_master = self.get_node_by_id(self.master_node_id)
		print "Switched master node from ({}:{},{}) to ({}:{},{}).".format(old_master.ip,old_master.port,old_master.node_id, new_master.ip,new_master.port,new_master.node_id)

	# after a master node is killed, call this method to refresh master node id.
	# may need to retry a few times because master election takes a few seconds
	# to start and then finish.
	# return True if master updated; false otherwise
	def refresh_master_node(self, conn):
		ret = False
		cur = conn.cursor()
		if self.id == 0:
			cur.execute("select cluster_master_id from pg_cluster_meta ")
		else:
			cur.execute("select master_node_id from pg_shard where id=" + str(self.id))
		row = cur.fetchone()
		if row == None:
			print "Can't get master_node_id for shard {}".format(self.id)
			cur.close()
			return False

		if int(row[0]) != self.master_node_id :
			self.master_node_id = int(row[0])
			ret = True
		else:
			print "Got old master_node_id {} from pg meta table for shard {}".format(self.master_node_id, self.id)
		cur.close()
		return ret

	def wait_for_computing_node_refresh(self, conn):
		i = 0
#don't keep waiting, when gtss and clas processes all happen to connect to current latest master,
#no DDL/DML could trigger a topo check and this is not an issue, except the pg_cluster_meta doesn't show latest
#shard master. this could happen when prev stmt is a DML which happen to write to one shard only.
#when next master switch happens the pg_cluster_meta/pg_cluster_meta_nodes  tables will be updated.
		while i < 11:
			i=i+1
			if self.refresh_master_node(conn):
				break;
			print "Trouble maker: waiting for computing node to refresh master info for shard {} {}".format(self.name, self.id)
			time.sleep(2)

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
		#self.init_nodes_settings()

	def init_nodes_settings(self):
		for node in self.nodes:
			conn = node.connect_mysql()
			csr = conn.cursor()
			csr.execute("set global group_replication_member_expel_timeout=0")
			csr.close()


class Trouble_maker:
	def __init__(self, conn_args):
		self.conn = psycopg2.connect(host=conn_args.ip, port=conn_args.port, user=conn_args.usr, database='postgres', password=conn_args.pwd)
		self.conn.autocommit = True
		self.keep_running = True
		self.storage_shards = []
		self.meta_shard = Shard()
		self.meta_shard.set_info(0, 'meta')
		self.get_storage_shards(self.conn)
		self.get_meta_shard(self.conn)

	def add_shard(self, s):
		self.storage_shards.append(s)
	def get_shard_by_id(self, sid):
		for s in self.storage_shards:
			if s.id == sid:
				return s
		return None

	def reconnect(self):
			# not needed now because connections to computing nodes are never broken
			pass

	def get_storage_shards(self, conn):
		cur = conn.cursor()
		cur.execute("select n.id , port , shard_id, ip, user_name , passwd , s.name as shard_name,  s.master_node_id from pg_shard_node n, pg_shard s where s.id = n.shard_id")
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
		cur.close()

		for shard in self.storage_shards:
			shard.init()

	def get_meta_shard(self, conn):
		cur = conn.cursor()
		cur.execute("select*from pg_cluster_meta_nodes")
		shard_nodes = cur.fetchall()
		for shard_node in shard_nodes:
			#server_id | cluster_id | is_master | port |    ip     | user_name | passwd
			#sn = ShardNode(shard_node['server_id'], shard_node['ip'], shard_node['port'], shard_node['user_name'], shard_node['passwd'])
			sn = ShardNode(shard_node[0], shard_node[4], shard_node[3], shard_node[5], shard_node[6])
			self.meta_shard.add_node(sn)
			if shard_node[2]:
				self.meta_shard.set_master_node_id(sn.node_id)
		cur.close()
		self.meta_shard.init()

	def make_trouble_all(self, opcode, dbop):
		if opcode < 3:
			self.make_trouble_meta_only(opcode, dbop)
		else:
			self.make_trouble(opcode - 3, dbop)

	def make_trouble_meta_only(self, opcode, dbop):
		if opcode == 0:
			self.meta_shard.switch_master()
			if (dbop != None):
				dbop.execute(self.conn)
			self.meta_shard.wait_for_computing_node_refresh(self.conn)
		elif opcode == 1:
			meta_master = self.meta_shard.get_master_node()
			meta_master.kill_mysqld()
			if (dbop != None):
				dbop.execute(self.conn)
			self.meta_shard.wait_for_computing_node_refresh(self.conn)
			meta_master.pullup_mysqld()
		elif opcode == 2 and have_cluster_manager:
			self.meta_shard.kill_all_mysqld()
			if (dbop != None):
				dbop.execute(self.conn)
			self.meta_shard.wait_for_all_mysqld()

	def make_trouble(self, opcode, dbop):
		if opcode == 0:
			self.meta_shard.switch_master()
			if (dbop != None):
				dbop.execute(self.conn)
			self.meta_shard.wait_for_computing_node_refresh(self.conn)
		elif opcode == 1:
			self.storage_shards[0].switch_master()
			if (dbop != None):
				dbop.execute(self.conn)
			self.storage_shards[0].wait_for_computing_node_refresh(self.conn)
		elif opcode == 2:
			self.storage_shards[1].switch_master()
			if (dbop != None):
				dbop.execute(self.conn)
			self.storage_shards[1].wait_for_computing_node_refresh(self.conn)
		elif opcode == 3:
			self.storage_shards[0].switch_master()
			self.storage_shards[1].switch_master()
			if (dbop != None):
				dbop.execute(self.conn)
			self.storage_shards[0].wait_for_computing_node_refresh(self.conn)
			self.storage_shards[1].wait_for_computing_node_refresh(self.conn)
		elif opcode == 4:
			self.meta_shard.switch_master()
			self.storage_shards[0].switch_master()
			self.storage_shards[1].switch_master()
			if (dbop != None):
				dbop.execute(self.conn)
			self.meta_shard.wait_for_computing_node_refresh(self.conn)
			self.storage_shards[0].wait_for_computing_node_refresh(self.conn)
			self.storage_shards[1].wait_for_computing_node_refresh(self.conn)
		elif opcode == 5:
			self.meta_shard.switch_master()
			the_shard = self.storage_shards[int(random.random() * 10)% len(self.storage_shards)]
			the_shard.switch_master()

			if (dbop != None):
				dbop.execute(self.conn)
			self.meta_shard.wait_for_computing_node_refresh(self.conn)
			the_shard.wait_for_computing_node_refresh(self.conn)
		elif opcode == 6:
			meta_master = self.meta_shard.get_master_node()
			meta_master.kill_mysqld()
			if (dbop != None):
				dbop.execute(self.conn)
			self.meta_shard.wait_for_computing_node_refresh(self.conn)
			meta_master.pullup_mysqld()
		elif opcode == 7:
			master0 = self.storage_shards[0].get_master_node()
			master0.kill_mysqld()
			if (dbop != None):
				dbop.execute(self.conn)
			self.storage_shards[0].wait_for_computing_node_refresh(self.conn)
			master0.pullup_mysqld()
		elif opcode == 8:
			master1 = self.storage_shards[1].get_master_node()
			master1.kill_mysqld()
			if (dbop != None):
				dbop.execute(self.conn)
			self.storage_shards[1].wait_for_computing_node_refresh(self.conn)
			master1.pullup_mysqld()
		elif opcode == 9:
			master0 = self.storage_shards[0].get_master_node()
			master0.kill_mysqld()
			master1 = self.storage_shards[1].get_master_node()
			master1.kill_mysqld()
			if (dbop != None):
				dbop.execute(self.conn)
			self.storage_shards[0].wait_for_computing_node_refresh(self.conn)
			self.storage_shards[1].wait_for_computing_node_refresh(self.conn)
			master0.pullup_mysqld()
			master1.pullup_mysqld()
		elif opcode == 10:
			meta_master = self.meta_shard.get_master_node()
			meta_master.kill_mysqld()
			master0 = self.storage_shards[0].get_master_node()
			master0.kill_mysqld()
			master1 = self.storage_shards[1].get_master_node()
			master1.kill_mysqld()
			
			if (dbop != None):
				dbop.execute(self.conn)
			self.meta_shard.wait_for_computing_node_refresh(self.conn)
			self.storage_shards[0].wait_for_computing_node_refresh(self.conn)
			self.storage_shards[1].wait_for_computing_node_refresh(self.conn)

			meta_master.pullup_mysqld()
			master0.pullup_mysqld()
			master1.pullup_mysqld()
		elif opcode == 11:
			meta_master = self.meta_shard.get_master_node()
			meta_master.kill_mysqld()
			the_shard = self.storage_shards[int(random.random() * 10)% len(self.storage_shards) ]
			the_master = the_shard.get_master_node()
			the_master.kill_mysqld()
			if (dbop != None):
				dbop.execute(self.conn)

			self.meta_shard.wait_for_computing_node_refresh(self.conn)
			the_shard.wait_for_computing_node_refresh(self.conn)

			meta_master.pullup_mysqld()
			the_master.pullup_mysqld()
		elif opcode == 12 and have_cluster_manager:
			self.meta_shard.kill_all_mysqld()
			if (dbop != None):
				dbop.execute(self.conn)
			self.meta_shard.wait_for_all_mysqld()
		elif opcode == 13 and have_cluster_manager:
			self.meta_shard.kill_all_mysqld()
			the_shard = self.storage_shards[int(random.random() * 10)% len(self.storage_shards) ]
			the_shard.kill_all_mysqld()
			if (dbop != None):
				dbop.execute(self.conn)
			self.meta_shard.wait_for_all_mysqld()
			the_shard.wait_for_all_mysqld()
		elif opcode == 14 and have_cluster_manager:
			self.meta_shard.kill_all_mysqld()
			self.storage_shards[0].kill_all_mysqld()
			self.storage_shards[1].kill_all_mysqld()
			if (dbop != None):
				dbop.execute(self.conn)
			self.meta_shard.wait_for_all_mysqld()
			self.storage_shards[0].wait_for_all_mysqld()
			self.storage_shards[1].wait_for_all_mysqld()
		elif opcode == 15 and have_cluster_manager:
			self.storage_shards[0].kill_all_mysqld()
			self.storage_shards[1].kill_all_mysqld()
			if (dbop != None):
				dbop.execute(self.conn)
			self.storage_shards[0].wait_for_all_mysqld()
			self.storage_shards[1].wait_for_all_mysqld()
		elif opcode == 16 and have_cluster_manager:
			the_shard = self.storage_shards[int(random.random() * 10)% len(self.storage_shards) ]
			the_shard.kill_all_mysqld()
			if (dbop != None):
				dbop.execute(self.conn)
			the_shard.wait_for_all_mysqld()
		else:
			pass


class Trouble_maker_thread(threading.Thread, Trouble_maker):
	def __init__(self, conn_args):
		threading.Thread.__init__(self)
		Trouble_maker.__init__(self, conn_args)
	def run(self):
		self.conn.autocommit = True
		while self.keep_running:
			time.sleep(int(random.random()*100) % 3 + 1);
			opcode = int(random.random() * 100) % 20
			self.make_trouble_all(opcode, None);
			print "Made trouble {}".format(opcode);
		self.conn.close()


class Trouble_maker_thread_ddl(threading.Thread, Trouble_maker):
	def __init__(self, conn_args):
		threading.Thread.__init__(self)
		Trouble_maker.__init__(self, conn_args)
	def run(self):
		self.conn.autocommit = True
		while self.keep_running:
			time.sleep(int(random.random()*100) % 5 + 2);
			opcode = int(random.random() * 100) % 3
			self.make_trouble_meta_only(opcode);
			print "Made trouble {}".format(opcode);
		self.conn.close()

class Serial_test:
	def __init__(self, conn_args):
		self.conn_args = conn_args
	def get_next_dml(self, idx):
		if idx == 0:
			stmt = "insert into part_tab1 values(1,1)"
		elif idx == 1:
			stmt = "insert into part_tab1 values(2,1),(3,1),(4,1),(5,1),(6,1),(7,1),(8,1),(9,1),(10,1)"
		elif idx == 2:
			stmt = "select*from part_tab1"
		elif idx == 3:
			stmt = "update part_tab1 set b=b+1"
		elif idx == 4:
			stmt = "delete from part_tab1 "
		else:
			stmt = ""
		idx = idx + 1
		return stmt, idx

	def get_next_ddl(self, idx):

		if idx == 0:
			stmt = "create database dbxx"
		elif idx == 1:
			stmt = "create schema schemaxx"
		elif idx == 2:
			stmt = "create table single_tab1(a int primary key, b int)"
		elif idx == 3:
			stmt = "create index single_tab1_b on single_tab1(b);"
		elif idx == 4:
			stmt = "drop index single_tab1_b"
		elif idx == 5:
			stmt = "drop table single_tab1"
		elif idx == 6:
			stmt = "create table part_tab1(a int primary key, b int) partition by hash(a);"
		elif idx == 7:
			stmt = "create table part_tab10 partition of part_tab1 FOR VALUES WITH (MODULUS 2, REMAINDER 0)"
		elif idx == 8:
			stmt = "create table part_tab11 partition of part_tab1 FOR VALUES WITH (MODULUS 2, REMAINDER 1)"
		elif idx == 9:
			stmt = "create index part_tab1_b on part_tab1(b)"
		elif idx == 10:
			stmt = "drop index part_tab1_b"
		elif idx == 11:
			stmt = "drop table part_tab1"
		elif idx == 12:
			stmt = "drop schema schemaxx"
		elif idx == 13:
			stmt = "drop database dbxx"
		else:
			stmt = ""

		idx = idx + 1
		return stmt, idx

	def run_test(self):
		tm = Trouble_maker(self.conn_args)
		dbop = DB_Operation()
#Because MySQL's DDL is only atomic but can't be aborted, Kunlun's DDL stmt isn't
#transactional --- if a ddl's execution fails on a storage shard, the actions on
#other storage shards can't be undone. thus in this test we can only make trouble
#to metadata shard when executing DDL stmts.
#also, we need the create tables to execute DML stmts so do the 'drop' part last.
		print "Single threaded DDL tests"
		for i in range(3,3):
			idx = 0
			while idx < 14:
				stmt, idx = self.get_next_ddl(idx)
				dbop.set_stmt_cursor(stmt)
				tm.make_trouble_meta_only(i, dbop)
				print "Done opcode: {}, stmt: {}".format(i, stmt)

		cur = tm.conn.cursor()
		stmt = "create table part_tab1(a int primary key, b int) partition by hash(a);"
		cur.execute(stmt)
		stmt = "create table part_tab10 partition of part_tab1 FOR VALUES WITH (MODULUS 4, REMAINDER 0)"
		cur.execute(stmt)
		stmt = "create table part_tab11 partition of part_tab1 FOR VALUES WITH (MODULUS 4, REMAINDER 1)"
		cur.execute(stmt)
		stmt = "create table part_tab12 partition of part_tab1 FOR VALUES WITH (MODULUS 4, REMAINDER 2)"
		cur.execute(stmt)
		stmt = "create table part_tab13 partition of part_tab1 FOR VALUES WITH (MODULUS 4, REMAINDER 3)"
		cur.execute(stmt)
		cur.close()

		for i in range(8,17):
			idx = 0
			while idx < 5:
				stmt, idx = self.get_next_dml(idx)
				dbop.set_stmt_cursor(stmt)
				tm.make_trouble(i, dbop)
				print "Done opcode: {}, stmt: {}".format(i, stmt)

		print "dropping part_tab1"
		stmt = "drop table part_tab1"
		cur = tm.conn.cursor()
		cur.execute(stmt)
		cur.close()
		tm.conn.close()




if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Test storage shard and metadata shard master failover processing in computing nodes.')
	parser.add_argument('ip', type=str, help="computing node ip");
	parser.add_argument('port', type=int, help="computing node port");
	parser.add_argument('usr', type=str, help="computing node user name");
	parser.add_argument('pwd', type=str, help="computing node password");
	parser.add_argument('cluster_manager_running', type=bool, help="whether cluster_manager is running for the test to maintain MGR node status.");
	parser.add_argument('mysql_install_path', type=str, help="mysql_install_path");

	args = parser.parse_args()

	have_cluster_manager = args.cluster_manager_running;
	mysql_install_path = args.mysql_install_path
	conn = psycopg2.connect(host=args.ip, port=args.port, user=args.usr, database='postgres', password=args.pwd)
	conn.autocommit = True
	prepare_data(conn)

	serial_test = Serial_test(args)
	serial_test.run_test()

	dmlthd1 = DML_thread1(args)
	dmlthd2 = DML_thread2(args)
	trouble_maker = Trouble_maker_thread (args)
	dmlthd1.start()
	dmlthd2.start()
	trouble_maker.start()
	time.sleep(3000)
	
	trouble_maker.keep_running=False
	dmlthd1.keep_running=False
	dmlthd2.keep_running=False

	trouble_maker.join()
	dmlthd1.join()
	dmlthd2.join()

	ddlthd = DDL_thread(args)
	trouble_maker_ddl = Trouble_maker_thread_ddl (args)
	ddlthd.start()
	trouble_maker_ddl.start()
	time.sleep(3000)
	
	ddlthd.keep_running=False
	trouble_maker_ddl.keep_running=False
	trouble_maker_ddl.join()
	ddlthd.join()

	test_teardown(conn)
	conn.close()
