# Copyright (c) 2019 ZettaDB inc. All rights reserved.
# This source code is licensed under Apache 2.0 License,
# combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

# create meta tables and initialize them with meta data
import mysql.connector
import argparse
import json
import subprocess
import common

parser = argparse.ArgumentParser(description='create meta tables and initialize them with meta data.')
parser.add_argument('--config', type=str, help="meta-data shard config file path")
parser.add_argument('--bootstrap_sql', type=str, help="path of sql file to create meta-data tables")
parser.add_argument('--ha_mode', type=str, default='mgr', choices=['mgr','no_rep', 'rbr'])

args = parser.parse_args()
mysql_conn_params = {
    'database':'Kunlun_Metadata_DB'
}

# config file format:
#[
#   {
#      "ip": "127.0.0.1",
#      "port": 3602,
#      "user": "abc",
#      "password":"abc",
#   },
#   { more nodes of the metadata cluster }
#]

jsconf = open(args.config)
jstr = jsconf.read()
jscfg = json.loads(jstr)
mysql_conn_params = common.mysql_shard_check(jscfg, args.ha_mode)

mysql_conn_params['database'] = 'mysql'
fbootstrap_sql = open(args.bootstrap_sql)
bootsql = fbootstrap_sql.read()
# load bootstrap sql file to create metadata tables
if mysql_conn_params.has_key('is_primary'):
    del mysql_conn_params['is_primary']
meta_conn = mysql.connector.connect(**mysql_conn_params)
meta_conn.autocommit = True
for result in meta_conn.cmd_query_iter(bootsql):
    pass
meta_conn.close()
fbootstrap_sql.close()

mysql_conn_params['database'] = 'Kunlun_Metadata_DB'
meta_conn = mysql.connector.connect(**mysql_conn_params)
meta_cursor = meta_conn.cursor(prepared=True)
meta_cursor0 = meta_conn.cursor()
# insert meta-cluster node info
meta_cursor0.execute("start transaction")
meta_cursor.execute("insert into global_configuration(name, value) values(%s, %s)", ("meta_ha_mode", args.ha_mode))
stmt = "insert into meta_db_nodes(hostaddr, port, user_name, passwd, member_state, sync_state, \
        degrade_conf_state, degrade_run_state, replica_delay, degrade_conf_time, sync_num) \
        values(%s, %s, %s, %s, %s, %s, %s, %s, 0, 0, 1)"
mysql_conn_params['is_primary'] = True
for node in jscfg:
    role = 'replica'
    if node.get('is_primary', False):
        role = 'source'
    meta_cursor.execute(stmt, (node['ip'], node['port'], node['user'], node['password'], role, 'fsync', 'OFF', 'OFF'))
meta_cursor0.execute("commit")
meta_cursor.close()
meta_cursor0.close()
meta_conn.close()
jsconf.close()
print "Bootstrapping completed!"
