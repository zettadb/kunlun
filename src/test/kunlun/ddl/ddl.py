# Copyright (c) 2019 ZettaDB inc. All rights reserved.
# This source code is licensed under Apache 2.0 License,
# combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

import os
import psycopg2
import argparse
import json
import time

# test storage shard and metadata shard master failover processing in computing nodes.
# the Kunlun DDC must be well created, with 2 storage shards, at least 1 computing node
# and a metadata cluster.

# computing node config file format:
#[
#	{
#		'host':'127.0.0.1',
#		'port':6401,
#		'user':'abc',
#		'password':'abc'
#	},
#	{
#		'host':'127.0.0.1',
#		'port':6402,
#		'user':'abc',
#		'password':'abc'
#	}
#]
# test all kinds of DDLs including those supported and banned
def exec_stmt(cur, stmt):
	while True:
		try:
			cur.execute(stmt)
                        break
		except psycopg2.Error as pgerr:
			print "Autocommit statement '{}' execution failed and will be retried: {}".format(stmt, str(pgerr))
			time.sleep(1)

class DDL_test:
	def __init__(self, args):
		comp0 = args[0]
		comp1 = args[1] #verify replication on this node
		self.comp0_conn_args = comp0
		self.comp1_conn_args = comp1
		self.conn = psycopg2.connect(host=comp0['host'], port=comp0['port'], user=comp0['user'], database='postgres', password=comp0['password'])
		self.verify_conn = psycopg2.connect(host=comp1['host'], port=comp1['port'], user=comp1['user'], database='postgres', password=comp1['password'])
		self.conn.autocommit = True
		self.verify_conn.autocommit = True
		self.newdb_conn = None
		self.verify_newdb_conn = None

	def run_test(self):
		self.drop_objs(self.conn, False)

		#execute DDL stmts in one node
		try:
			self.create_test(self.conn, True)
		except psycopg2.Error as pgerr:
			self.drop_objs(self.conn, False)

		comp0 = self.comp0_conn_args
		comp1 = self.comp1_conn_args
		self.newdb_conn = psycopg2.connect(host=comp0['host'], port=comp0['port'], user=comp0['user'], database='dbxx', password=comp0['password'])
                self.newdb_conn.autocommit = True
		self.verify_newdb_conn = psycopg2.connect(host=comp1['host'], port=comp1['port'], user=comp1['user'], database='dbxx', password=comp1['password'])
                self.verify_newdb_conn.autocommit = True
		#execute DDL stmts in one node in new db
		try:
			self.create_test(self.newdb_conn, False)
		except psycopg2.Error as pgerr:
			self.drop_objs(self.conn, False)

		#verify they are effective on another node in newdb
		self.verify_create_test(self.verify_newdb_conn, False)

		#close connections to dbxx so we can drop it
		self.newdb_conn.close()
		self.verify_newdb_conn.close()
		self.newdb_conn = None
		self.verify_newdb_conn = None
		#verify they are effective on another node
		self.verify_create_test(self.verify_conn, True)

	def create_test(self, conn, creatdb):
		csr = conn.cursor()
		start = 1
		if creatdb:
			start = 0

		for i in range(start, 8):
			stmt = self.get_ddl_stmt(i)
			csr.execute(stmt)
		csr.close()

	def verify_create_test(self, conn, creatdb):
		csr = conn.cursor()
		end = 13
		if creatdb:
			end = 14

		for i in range(8, end):
			stmt = self.get_ddl_stmt(i)
			exec_stmt(csr, stmt)
		csr.close()

	def drop_objs(self, conn, dropdb):
		cur = conn.cursor()
		cur.execute("drop schema if exists schemaxx")
		cur.execute("drop table if exists single_tab1")
		cur.execute("drop table if exists part_tab1")
		if dropdb:
			cur.execute("drop database if exists dbxx")
		cur.close()

	def teardown(self):
		if self.newdb_conn:
			self.drop_objs(self.newdb_conn, False)
			self.newdb_conn.close()
			self.newdb_conn = None
		self.drop_objs(self.conn, False)
		self.conn.close()
		self.conn = None

	def get_ddl_stmt(self, idx):

		if idx == 0:
			stmt = "create database if not exists dbxx"
		elif idx == 1:
			stmt = "create schema if not exists schemaxx"
		elif idx == 2:
			stmt = "create table if not exists single_tab1(a int primary key, b int)"
		elif idx == 3:
			stmt = "create index if not exists single_tab1_b on single_tab1(b);"
		elif idx == 4:
			stmt = "create table if not exists part_tab1(a int primary key, b int) partition by hash(a);"
		elif idx == 5:
			stmt = "create table if not exists part_tab10 partition of part_tab1 FOR VALUES WITH (MODULUS 2, REMAINDER 0)"
		elif idx == 6:
			stmt = "create table if not exists part_tab11 partition of part_tab1 FOR VALUES WITH (MODULUS 2, REMAINDER 1)"
		elif idx == 7:
			stmt = "create index if not exists part_tab1_b on part_tab1(b)"
		elif idx == 8:
			stmt = "drop index if exists single_tab1_b"
		elif idx == 9:
			stmt = "drop table if exists single_tab1"
		elif idx == 10:
			stmt = "drop index if exists part_tab1_b"
		elif idx == 11:
			stmt = "drop table if exists part_tab1"
		elif idx == 12:
			stmt = "drop schema if exists schemaxx"
		elif idx == 13:
			stmt = "drop database if exists dbxx"
		else:
			stmt = ""

		return stmt
if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Tests Kunlun DDC DDL replication features.')
	parser.add_argument('nodes', type=str, help="Two computing nodes' connection arguments");
        args = parser.parse_args()

	node_jsconf = open(args.nodes);
	node_jstr = node_jsconf.read()
	node_jscfg = json.loads(node_jstr);

	ddl_test = DDL_test(node_jscfg)
	ddl_test.run_test()
	ddl_test.teardown()

	node_jsconf.close()
