# Copyright (c) 2019 ZettaDB inc. All rights reserved.
# This source code is licensed under Apache 2.0 License,
# combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

import mysql.connector
import argparse
import json
import time
import copy
from distutils.util import strtobool

def connect_mysql(node):
	try:
		mysql_conn_params = {}
		mysql_conn_params['host'] = node['ip']
		mysql_conn_params['port'] = node['port']
		mysql_conn_params['user'] = node['user']
		mysql_conn_params['password'] = node['password']
		mysql_conn = mysql.connector.connect(**mysql_conn_params)
	except mysql.connector.errors.InterfaceError as err:
		print "Unable to connect to {}, error: {}".format(str(mysql_conn_params), str(err))

	return mysql_conn



def mysql_version_check(conn, conn_params):
	csr = conn.cursor()
	csr.execute("select version()")
	row = csr.fetchone()
	if row == None or row[0].find("kunlun-storage") < 0:
		raise Exception("Version mismatch: mysql server {} version is {}, but 8.0.x-kunlun-storage is required.".format(str(conn_params), row[0]))
	print "Node {} version {} check passes.".format(str(conn_params), row[0])
	csr.close()

def mysql_node_check0(conn_params):
    return conn_params['ip'], conn_params['port']

# check target mysql node has right version and is in its shard cluster.
# return its known master's ip&port
def mysql_node_check(conn_params):
	conn = connect_mysql(conn_params)
	mysql_version_check(conn, conn_params)

	csr = conn.cursor()
	csr.execute("select MEMBER_HOST, MEMBER_PORT from performance_schema.replication_group_members where member_state='ONLINE' and MEMBER_ROLE='PRIMARY'".format(conn_params['ip'], conn_params['port']))
	row = csr.fetchone()
	if row == None or row[0] == None or row[0] == '' or row[1] == None:
		raise Exception("ERROR: Node {} disconnected from its shard cluster.".format(str(conn_params)))
	csr.close()
	conn.close()
	return row[0], row[1]

# make sure all nodes are in the same shard and has the same master node
# return the master node params suitable for mysql connection
def mysql_shard_check(shard_conn_params, do_check):
	cur_prim_ip = ''
	cur_prim_port = 0
	prim_node = None

	for node in shard_conn_params:
		if do_check:
			node_mip, node_mport = mysql_node_check(node)
                else:
			node_mip, node_mport = mysql_node_check0(node)
		if cur_prim_port == 0:
			cur_prim_ip = node_mip
			cur_prim_port = node_mport
		elif cur_prim_ip != node_mip or cur_prim_port != node_mport:
			raise Exception("Node {} has different primary node ({}:{}) than currently found primary node({}:{})".format(str(node), node_mip, node_mport, cur_prim_ip, cur_prim_port))

		if node['ip'] == cur_prim_ip and node['port'] == cur_prim_port:
			prim_node = node

	if prim_node == None:
		raise Exception("Shard primary node ({}:{}) is not found in the shard {}".format(cur_prim_ip, cur_prim_port, str(shard_conn_params)))
	prim_node = copy.deepcopy(prim_node)
	prim_node['host'] = prim_node['ip']
	del prim_node['ip']
	return prim_node



if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='test functions in this file')
	parser.add_argument('--config', help="shard config file path")
	parser.add_argument('--meta_config', type=str, help="metadata cluster config file path")
	parser.add_argument('--usemgr', type=str, default='True'); # used for internal testing, --usemgr=True|False

	args = parser.parse_args()
	args.usemgr=strtobool(args.usemgr)
	
	meta_jsconf = open(args.meta_config)
	meta_jstr = meta_jsconf.read()
	meta_jscfg = json.loads(meta_jstr)
	mysql_conn_params = mysql_shard_check(meta_jscfg, True)
	print "Meta shard primary node: {}".format(str(mysql_conn_params))

	jsconf = open(args.config)
	jstr = jsconf.read()
	jscfg = json.loads(jstr)

	for shardcfg in jscfg:
		mysql_conn_params = mysql_shard_check(shardcfg['shard_nodes'], args.usemgr)
		print "Shard {} primary node: {}".format(shardcfg['shard_name'], str(mysql_conn_params))

