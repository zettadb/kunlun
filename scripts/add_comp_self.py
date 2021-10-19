# Copyright (c) 2019 ZettaDB inc. All rights reserved.
# This source code is licensed under Apache 2.0 License,
# combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

# add one or more computing nodes

import os
import os.path
import mysql.connector
import argparse
import json
import common
import socket
import add_comp_nodes
import install_pg
import sys
import psycopg2

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
#   }
#]

def gethostip():
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)
    return ip

def checkserver(sip, sport, suser, spass, sdb):
    conn = psycopg2.connect(host=sip, port=sport, user=suser, database=sdb, password=spass)
    conn.close()
    return conn

def add_comp_self(install_path, config_template_file, mysql_conn_params, config_path, args):
    selfip = args.hostname

    meta_conn = mysql.connector.connect(**mysql_conn_params)
    meta_cursor = meta_conn.cursor()
    meta_cursor.execute("start transaction")
    stmt = "insert into comp_nodes_id_seq values();"
    meta_cursor.execute(stmt)
    stmt = "select last_insert_id();"
    meta_cursor.execute(stmt)
    row = meta_cursor.fetchone()
    meta_cursor.execute("commit")
    maxid = 1 
    if row is not None and row[0] is not None:
	maxid = int(row[0])
    meta_cursor.close()
    meta_conn.close()

    selfobj = {"id" : maxid,
	       "name" : "comp" + str(maxid),
	       "ip" : selfip,
	       "port" : args.port,
	       "user": args.user,
	       "password": args.password,
	       "datadir" : args.datadir
	       }
    selfarr = [selfobj]
    outf = open(config_path, "w")
    json.dump(selfarr, outf, indent=4)
    outf.close()
    if args.install:
        if args.docker:
            # install is not performed here currently, since the meta_config file needs to
            os.system("chmod a+rwx /kunlun/env.sh")
            os.system("chown -R postgres:postgres /pgdatadir")
            os.system("su postgres -c 'cd /kunlun && . ./env.sh; cd postgresql-11.5-rel/scripts; python2 install_pg.py config=./%s install_ids=%d' " % (config_path, maxid))
        else:
            install_pg.install_pg(config_template_file, install_path, selfobj)
    conn = checkserver(selfip, args.port, args.user, args.password, 'postgres')
    if conn is None:
        raise Exception("Computing server is not installed correctly, please check the installation!")
    add_comp_nodes.add_computing_nodes(mysql_conn_params, args, config_path, [maxid])
    sys.stdout.flush()
    sys.stderr.flush()

    # Reset the comp_node_id. It should be removed when comp_node_id is removed from postgresql.conf
    if not args.install:
        cmd0 = "export PATH=" + install_path + "/bin:$PATH;"
        cmd1 = "export LD_LIBRARY_PATH=" + install_path + "/lib:$LD_LIBRARY_PATH;"
        if args.docker:
            os.system("sed -i 's/comp_node_id.*=.*/comp_node_id=%d/g' %s/postgresql.conf" % (maxid, args.datadir))
            os.system("su postgres -c 'cd /kunlun && . ./env.sh && pg_ctl -D %s stop -m immediate' " % args.datadir)
            os.system("su postgres -c 'cd /kunlun && . ./env.sh && cd postgresql-11.5-rel/scripts && python2 start_pg.py port=%d' " % args.port)
        else:
            os.system("sed -i 's/comp_node_id.*=.*/comp_node_id=%d/g' %s/postgresql.conf" % (maxid, args.datadir))
            os.system(cmd0 + cmd1 + "pg_ctl -D %s stop -m immediate " % args.datadir)
            # start_pg.py set the env well.
            os.system("python2 start_pg.py port=%d " % args.port)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Add current computing node to the cluster.')
    parser.add_argument('--meta_config', type=str, help="metadata cluster config file path")
    parser.add_argument('--cluster_name', type=str, help = "The cluster name")
    parser.add_argument('--user', type=str, help="The user name")
    parser.add_argument('--password', type=str, help="The password")
    parser.add_argument('--hostname', type=str, help="The hostname", default=gethostip())
    parser.add_argument('--port', type=int, help="The port", default=5432)
    parser.add_argument('--datadir', type=str, help="The data directory", default='/pgdatadir')
    parser.add_argument('--install', help="install it first", default=False, action='store_true')
    parser.add_argument('--docker', help="process is in docker container", default=False, action='store_true')

    args = parser.parse_args()

    meta_jsconf = open(args.meta_config)
    meta_jstr = meta_jsconf.read()
    meta_jscfg = json.loads(meta_jstr)

    install_path = os.path.dirname(os.getcwd())
    config_template_file = install_path + "/resources/postgresql.conf"

    mysql_conn_params = {}
    mysql_conn_params = common.mysql_shard_check(meta_jscfg, True)
    mysql_conn_params['database'] = 'Kunlun_Metadata_DB'
            
    add_comp_self(install_path, config_template_file, mysql_conn_params, "self.json", args)
    print "Current computing node successfully added to cluster " + args.cluster_name
