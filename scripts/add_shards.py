# Copyright (c) 2019 ZettaDB inc. All rights reserved.
# This source code is licensed under Apache 2.0 License,
# combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

import psycopg2
import mysql.connector
import argparse
import json
import time
import common
from distutils.util import strtobool

# shards config file format:
#
# [
#   {
#   "shard_name": "shard-name",
#   "shard_nodes":
#   [
#       {
#          "ip": "127.0.0.1",
#          "port": 5431,
#          "user": "abc",
#          "password":"abc"
#       },
#   
#       {
#          "ip": "127.0.0.1",
#          "port": 5432,
#          "user": "abc",
#          "password":"abc"
#       },
#       { more slaves as needed }
#   ]
# ,
# { more shard configs like above}
# ]

def add_shards_to_cluster(mysql_conn_params, cluster_name, config_path, install_names, usemgr):
    meta_conn = mysql.connector.connect(**mysql_conn_params)

    jsconf = open(config_path)
    jstr = jsconf.read()
    jscfg0 = json.loads(jstr)
    meta_cursor = meta_conn.cursor(prepared=True)
    get_cluster_id_stmt = "select id from db_clusters where name=%s"
    meta_cursor.execute(get_cluster_id_stmt, (cluster_name,))
    row = meta_cursor.fetchone()
    cluster_id = row[0]

    num_nodes = 0

    add_shard_stmt = "insert into shards(name, when_created, num_nodes, db_cluster_id) values(%s, now(), 0, %s)"
    add_shard_node_stmt = "insert into shard_nodes(hostaddr, port, user_name, passwd, shard_id, db_cluster_id, svr_node_id, master_priority) values(%s, %s, %s, %s, %s, %s, 1,0)"

    jscfg = []
    nshards = 0

    # fetch the list of target shards to jscfg. may be all shards or specified ones in the config file.
    if install_names[0] == '':
        jscfg = jscfg0
        nshards = len(jscfg)
    else:
        shard_names2add = ''
        sepstr = ''
        for cfg in jscfg0:
            if install_names.count(cfg['shard_name']) > 0:
                if nshards == 1:
                    sepstr = ', '
                shard_names2add = shard_names2add + sepstr + cfg['shard_name']
                jscfg.append(cfg)
                nshards = nshards+1

        print "Shards to add: {}".format(shard_names2add)

    masters = []

    # check storage shard topology and version first.
    for shardcfg in jscfg:
		# set to False to disable checking master of storage shards if MGR isn't used
        shard_master = common.mysql_shard_check(shardcfg['shard_nodes'], usemgr)
        masters.append(shard_master)

    # add shards info to metadata-cluster tables.
    meta_cursor0 = meta_conn.cursor(buffered=True, dictionary=True)
    meta_cursor0.execute("start transaction")
    for shardcfg in jscfg:
        meta_cursor.execute(add_shard_stmt, (shardcfg['shard_name'], cluster_id))
        meta_cursor0.execute("select last_insert_id() as id")
        row = meta_cursor0.fetchone()
        shard_id = row['id']
        shardcfg['shard_id'] = shard_id
        num_nodes = 0
        for val in shardcfg['shard_nodes']:
            meta_cursor.execute(add_shard_node_stmt, (val['ip'], val['port'], val['user'], val['password'], shard_id, cluster_id))
            meta_cursor0.execute("select last_insert_id() as id")
            row0 = meta_cursor0.fetchone()
            val['shard_node_id'] = row0['id']
            num_nodes=num_nodes+1

        shardcfg['num_nodes'] = num_nodes
        meta_cursor.execute("update shards set num_nodes=? where id=?", (num_nodes, shard_id))

    meta_cursor0.execute("commit")
    meta_cursor.close()
    meta_cursor0.close()

    # create the default database in each new shard.
    for master in masters:
        master_conn = mysql.connector.connect(**master)
        master_cursor = master_conn.cursor()
        master_cursor.execute("create database postgres_$$_public CHARACTER set=utf8")
        master_cursor.close()
        master_conn.close()

    add_new_shards_to_all_computing_nodes(cluster_id, meta_conn, jscfg)
    meta_conn.close()
    jsconf.close()
    return nshards

def add_new_shards_to_all_computing_nodes(cluster_id, meta_conn, jscfg):

    meta_cursor0 = meta_conn.cursor(buffered=True, dictionary=True)
    meta_cursor0.execute("select * from comp_nodes where db_cluster_id={}".format(cluster_id))

    for row in meta_cursor0:
        conn = psycopg2.connect(host=row['hostaddr'], port=row['port'], user=row['user_name'], database='postgres', password=row['passwd'])
        cur = conn.cursor()
        nretries = 0
        while nretries < 10:
            try:
                cur.execute("start transaction")
                for shardcfg in jscfg:
                    cur.execute("insert into pg_shard (name, id, num_nodes, master_node_id, space_volumn, num_tablets, db_cluster_id, when_created) values(%s, %s, %s, %s, %s,%s,%s, now())",
                            (shardcfg['shard_name'], shardcfg['shard_id'], shardcfg['num_nodes'], shardcfg['shard_nodes'][0]['shard_node_id'], 0, 0, cluster_id))
                    for v in shardcfg['shard_nodes']:
                        cur.execute("insert into pg_shard_node values(%s, %s, %s, %s, %s, %s, %s, %s, now())",
                                (v['shard_node_id'], v['port'], shardcfg['shard_id'], 0, 0, v['user'], v['ip'], v['password']))
                cur.execute("commit")
                break
            except psycopg2.Error as pgerr:
                nretries = nretries + 1
                print "Got error: " + str(pgerr) + ". Shard config: {" + str(shardcfg) + "}. Will retry in 2 seconds, " + str(nretries) +" of 10 retries."
                cur.execute("rollback")
                time.sleep(2)

        cur.close()
        conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Add one or more shard(s) to the cluster.')
    parser.add_argument('--config', help="shard config file path")
    parser.add_argument('--meta_config', type=str, help="metadata cluster config file path")
    parser.add_argument('--cluster_name', type=str)
    parser.add_argument('--targets', type=str, help="target shards to install, specified by shard names. If none, add all shards.")

    args = parser.parse_args()
    install_names = []

    if args.targets:
        idstr = args.targets.split(',')
        for id in idstr:
            id = id.strip()
            if id == '':
                raise Exception("Must specifiy valid shard names.")
            install_names.append(id)
    else:
        install_names = [''] # install all shards

    meta_jsconf = open(args.meta_config)
    meta_jstr = meta_jsconf.read()
    meta_jscfg = json.loads(meta_jstr)
    mysql_conn_params = {}

    usemgr = len(meta_jscfg) > 1
    mysql_conn_params = common.mysql_shard_check(meta_jscfg, usemgr)
    mysql_conn_params['database'] = 'Kunlun_Metadata_DB'
    num_done = add_shards_to_cluster(mysql_conn_params, args.cluster_name, args.config, install_names, usemgr)
    print "Shard nodes successfully added to cluster {} : {}".format(args.cluster_name, num_done)
