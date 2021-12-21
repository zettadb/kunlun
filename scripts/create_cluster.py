# Copyright (c) 2019 ZettaDB inc. All rights reserved.
# This source code is licensed under Apache 2.0 License,
# combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

# create one cluster, using shard config file and computing node config file.
# metadata in meta-cluster and all computing nodes are updated and well maintained.

import mysql.connector
import argparse
import json
import add_comp_nodes
import add_shards
import common
from distutils.util import strtobool

parser = argparse.ArgumentParser(description='Create one cluster')
parser.add_argument('--shards_config', type=str, help="shard config file path")
parser.add_argument('--comps_config', type=str, help="computing nodes config file path")
parser.add_argument('--meta_config', type=str, help="meta-shard config file path")
parser.add_argument('--cluster_name', type=str)
parser.add_argument('--cluster_owner', type=str); # owner name, e.g. department/group name, or employee name
parser.add_argument('--cluster_biz', type=str); # used in which business ?
parser.add_argument('--ha_mode', type=str, default='mgr', choices=['mgr','no_rep', 'rbr'])

args = parser.parse_args()

meta_jsconf = open(args.meta_config)
meta_jstr = meta_jsconf.read()
meta_jscfg = json.loads(meta_jstr)
mysql_conn_params = common.mysql_shard_check(meta_jscfg, args.ha_mode == 'mgr')
mysql_conn_params['database'] = 'Kunlun_Metadata_DB'

meta_conn = mysql.connector.connect(**mysql_conn_params)

meta_cursor = meta_conn.cursor(prepared=True)
meta_cursor0 = meta_conn.cursor()

# insert cluster info into db_clusters
stmt = "insert into db_clusters(name, owner, ddl_log_tblname, business, ha_mode) values(%s, %s, %s, %s, %s)"
ddl_log_tblname='ddl_ops_log_'+args.cluster_name  # must be like this, don't change the format

print "Creating database cluster " + args.cluster_name
print "Step 1. Inserting meta-data row for database cluster " + args.cluster_name
meta_cursor0.execute("start transaction")

meta_cursor.execute(stmt, (args.cluster_name, args.cluster_owner, ddl_log_tblname, args.cluster_biz, args.ha_mode))

meta_cursor0.execute("commit")

print "Step 2. Creating DDL log for " + args.cluster_name
meta_cursor0.execute("create table ddl_ops_log_" + args.cluster_name + " like ddl_ops_log_template_table")
print "Step 3. Creating commit log for " + args.cluster_name
meta_cursor0.execute("create table commit_log_" + args.cluster_name + " like commit_log_template_table")
meta_cursor.close()
meta_cursor0.close()
meta_conn.close()

print "Step 4. Adding computing nodes into cluster " + args.cluster_name
install_ids=[-1] # add all computing nodes
add_comp_nodes.add_computing_nodes(mysql_conn_params, args, args.comps_config, install_ids)

print "Step 5. Adding storage shards into cluster " + args.cluster_name
install_names=[''] # add all shards
add_shards.add_shards_to_cluster(mysql_conn_params, args.cluster_name, args.shards_config, install_names, args.ha_mode == 'mgr')
print "Installation complete for cluster " + args.cluster_name
