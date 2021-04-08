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
mysql_conn_params = common.mysql_shard_check(jscfg, True)
mysql_conn_params['database'] = 'Kunlun_Metadata_DB'

fbootstrap_sql = open(args.bootstrap_sql)
# load bootstrap sql file to create metadata tables
subp = subprocess.Popen(['mysql', '-h', mysql_conn_params['host'], '-P', str(mysql_conn_params['port']), '-u', mysql_conn_params['user'], '-p'+mysql_conn_params['password']], stdin=fbootstrap_sql, stdout=subprocess.PIPE)
subp.wait()
fbootstrap_sql.close()

meta_conn = mysql.connector.connect(**mysql_conn_params)
meta_cursor = meta_conn.cursor(prepared=True)
meta_cursor0 = meta_conn.cursor()
stmt = "insert into meta_db_nodes(ip, port, user_name, passwd) values(%s, %s, %s, %s)"
# insert meta-cluster node info
meta_cursor0.execute("start transaction")
for node in jscfg:
    meta_cursor.execute(stmt, (node['ip'], node['port'], node['user'], node['password']))
meta_cursor0.execute("commit")
meta_cursor.close()
meta_cursor0.close()
meta_conn.close()
fbootstrap_sql.close()
jsconf.close()
print "Bootstrapping completed!"
