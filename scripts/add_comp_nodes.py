# Copyright (c) 2019 ZettaDB inc. All rights reserved.
# This source code is licensed under Apache 2.0 License,
# combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

# add one or more computing nodes

import psycopg2
import mysql.connector
import argparse
import json
import common



# config file format:
#
#[
#   {
#      "id":1,
#      "name":"comp1",
#      "ip":"127.0.0.1",
#      "port":5431,
#      "user":"abc",
#      "password":"abc"
#      "datadir":"/data/pg_data_dir1"
#   },    
#   {
#      "id":2,
#      "name":"comp2",
#      "ip":"127.0.0.1",
#      "port":5432,
#      "user":"abc",
#      "password":"abc"
#      "datadir":"/data/pg_data_dir2"
#   }
#   , more computing node config objects can follow.
#]


def add_computing_nodes(mysql_conn_params, args, config_path, install_ids, intoSeqTable=True) :
    meta_conn = mysql.connector.connect(**mysql_conn_params)
    jsconf = open(config_path)
    jstr = jsconf.read()
    jscfg0 = json.loads(jstr)
    jscfg = []

    # fetch the list of target computing nodes to jscfg. may be all nodes or specified part of the nodes in the config file.
    if install_ids[0] == -1:
        jscfg = jscfg0
    else:
        for cfg in jscfg0:
            if install_ids.count(cfg['id']) > 0:
                jscfg.append(cfg)

    meta_cursor = meta_conn.cursor(prepared=True)
    get_cluster_id_stmt = "select id, @@server_id as svrid, ha_mode from db_clusters where name=%s"
    meta_cursor.execute(get_cluster_id_stmt, (args.cluster_name,))
    row = meta_cursor.fetchone()
    cluster_id = row[0]
    cluster_master_svrid = row[1]

    ha_mode = -1
    if row[2] == 'no_rep':
        ha_mode = 0
    elif row[2] == 'mgr':
        ha_mode = 1
    elif row[2] == 'rbr':
        ha_mode = 2

    meta_cursor0 = meta_conn.cursor(buffered=True, dictionary=True)

    # insert computing nodes info into meta-tables.
    stmt = "insert into comp_nodes(id, name, hostaddr, port, db_cluster_id,user_name,passwd) values(%s, %s, %s, %s, %s, %s, %s)"
    idstmt = "insert into comp_nodes_id_seq(id) values(%s)"
    meta_cursor0.execute("start transaction")

    for compcfg in jscfg:
        meta_cursor.execute(stmt, (compcfg['id'], compcfg['name'],compcfg['ip'], compcfg['port'], cluster_id, compcfg['user'], compcfg['password']))
        if intoSeqTable:
            meta_cursor.execute(idstmt % str(compcfg['id']))
    meta_cursor.close()
    meta_cursor0.execute("select*from meta_db_nodes")
    meta_dbnodes = meta_cursor0.fetchall()
    meta_master_id = 0

    meta_cursor0.execute("select * from shards where db_cluster_id=" + str(cluster_id))
    shard_rows = meta_cursor0.fetchall()

    meta_cursor0.execute("select * from shard_nodes where db_cluster_id= " + str(cluster_id))
    shard_node_rows = meta_cursor0.fetchall()

    meta_cursor0.execute("commit")

    # create a partition for each computing node in commit log table. DDLs can't be prepared so we have to connect strings.
    for compcfg in jscfg:
        meta_cursor0.execute("alter table commit_log_" + args.cluster_name + " add partition(partition p" + str(compcfg['id']) + " values in (" + str(compcfg['id']) + "))")

    # insert meta data into each computing node's catalog tables.
    for compcfg in jscfg:
        conn = psycopg2.connect(host=compcfg['ip'], port=compcfg['port'], user=compcfg['user'], database='postgres', password=compcfg['password'])
        cur = conn.cursor()
        cur.execute("set skip_tidsync = true; start transaction")
        cur.execute("insert into pg_cluster_meta values(%s, %s, %s, %s, %s, %s)",
                (compcfg['id'], cluster_id, 0, ha_mode, args.cluster_name, compcfg['name']))
        for meta_node in meta_dbnodes:
            is_master = False
            
            if meta_node['hostaddr'] == mysql_conn_params['host'] and meta_node['port'] == mysql_conn_params['port']:
                is_master = True
                meta_master_id = meta_node['id']

            cur.execute("insert into pg_cluster_meta_nodes values(%s, %s, %s, %s, %s, %s, %s)",
                    (meta_node['id'], cluster_id, is_master, meta_node['port'], meta_node['user_name'], meta_node['hostaddr'], meta_node['passwd']))
        # if this is from a backup then it may already have some or all shard info, proceed anyway.
        cur1 = conn.cursor()
        cur1.execute("select id from pg_shard")
        shardids = cur1.fetchall()
        shard_nrows = {} # how many nodes are there in each shard? shard_id->count mapping.

        for shard_row in shard_rows:
            if shard_row['id'] in shardids:
                continue
            shard_nrows[shard_row['id']] = 0;
            cur.execute("insert into pg_shard (name, id, master_node_id, num_nodes, space_volumn, num_tablets, db_cluster_id, when_created) values(%s, %s, %s, %s, %s,%s,%s,%s)",
                    (shard_row['name'], shard_row['id'], 0, 0, shard_row['space_volumn'],
                     shard_row['num_tablets'], shard_row['db_cluster_id'], shard_row['when_created']))

        cur1.execute("select id from pg_shard_node")
        shardnodeids = cur1.fetchall()
        for shard_node_row in shard_node_rows:
            if shard_node_row['id'] in shardnodeids:
                continue
            shard_nrows[shard_node_row['shard_id']] += 1;
            cur.execute("insert into pg_shard_node values(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (shard_node_row['id'], shard_node_row['port'], shard_node_row['shard_id'], 0, 0,
                     shard_node_row['user_name'], shard_node_row['hostaddr'], shard_node_row['passwd'], shard_node_row['when_created']))
            # update master_node_id to any node's id of the shard, it can't be 0 otherwise computing node won't be able to work.
            cur.execute("update pg_shard set master_node_id = %s where master_node_id = 0 and id=%s", (shard_node_row['id'], shard_node_row['shard_id']))
        # update 'num_rows' for each pg_shard row.
        for k,v in shard_nrows.iteritems():
            cur.execute("update pg_shard set num_nodes = %s where id=%s", (v, k))

        cur.execute("update pg_cluster_meta set cluster_master_id=%s where cluster_name=%s",(meta_master_id, args.cluster_name))

        cur.execute("select oid from pg_database where datname='postgres'")
        dbid = cur.fetchone()
        cur.execute("insert into pg_ddl_log_progress values(%s, 0, 0)",(dbid,))
        cur.execute("commit")
        cur.close()
        cur1.close()
        conn.close()

    meta_cursor.close()
    meta_cursor0.close()
    meta_conn.close()
    jsconf.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Add one or more computing node(s) to the cluster.')
    parser.add_argument('--config', type=str, help="computing nodes config file path")
    parser.add_argument('--meta_config', type=str, help="metadata cluster config file path")
    parser.add_argument('--cluster_name', type=str)
    parser.add_argument('--targets', type=str, help="target computing nodes to install. e.g. all, or 1,2,3")

    args = parser.parse_args()
    install_ids = []

    if args.targets and args.targets != "all":
        idstr = args.targets.split(',')
        for id in idstr:
            install_ids.append(int(id))
    else:
        install_ids = [-1] # install all nodes

    meta_jsconf = open(args.meta_config)
    meta_jstr = meta_jsconf.read()
    meta_jscfg = json.loads(meta_jstr)

    mysql_conn_params = {}
    mysql_conn_params = common.mysql_shard_check(meta_jscfg, len(meta_jscfg) > 1)
    mysql_conn_params['database'] = 'Kunlun_Metadata_DB'
            
    add_computing_nodes(mysql_conn_params, args, args.config, install_ids)
    print "Computing nodes successfully added to cluster " + args.cluster_name
