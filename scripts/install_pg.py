# Copyright (c) 2019 ZettaDB inc. All rights reserved.
# This source code is licensed under Apache 2.0 License,
# combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

import os
import os.path
import getpass
import sys
import re
import time
import random
import fcntl
import struct
import socket
import subprocess
import json
import shlex
from distutils.util import strtobool
import argparse

# This script installs one or more specified computing node instances on current server.
# the configs of these computing nodes are all stored in the supplied config file.
# use comma seperated list of computing node ids to specify the computing nodes to
# install on this server, as you may want to install all the computing nodes into multiple servers.

def param_replace(string, rep_dict):
    pattern = re.compile("|".join([re.escape(k) for k in rep_dict.keys()]), re.M)
    return pattern.sub(lambda x: rep_dict[x.group(0)], string)

def install_pg(config_template_file, install_path, compcfg):
    #check install datadir is empty, make it if not exist
    datadir = compcfg['datadir'].strip()
    if datadir[0] != '/':
        raise Exception("Invalid data dir: data dir must use full path.")
    if os.path.exists(datadir) and len(os.listdir(datadir)) != 0:
        raise Exception("Invalid data dir: data dir " + datadir + " already exists and is not empty!")
    if not os.path.exists(datadir):
        os.makedirs(datadir)

    portstr = str(compcfg['port'])
    etc_path = install_path + "/etc"
    conf_list_file = etc_path+"/instances_list.txt"
    ret = os.system("grep '^" + portstr + "==>' " + conf_list_file + " >/dev/null 2>/dev/null")
    if ret == 0:
        raise Exception("Invalid port:" + portstr + ", The port is in use!")

    # do initdb
    cmd0 = "export LD_LIBRARY_PATH=" + install_path + "/lib:$LD_LIBRARY_PATH;"
    cmd1 = 'export LD_PRELOAD="' + install_path + '/resources/libjemalloc.so.3.6.0"; ulimit -c unlimited; '
    initcmd = cmd0 + install_path + '/bin/initdb -D ' + datadir
    os.system(initcmd)

    # copy pg config template to datadir
    cp_conf = 'cp ' + config_template_file + ' ' + datadir
    os.system(cp_conf)
    conf_path = datadir + '/postgresql.conf'

    # replace place holder params and then write back to the instance's conf file
    config_template = open(conf_path, 'r').read()
    replace_items = {}
    replace_items['port_placeholder'] = portstr
    replace_items['comp_node_id_placeholder'] = str(compcfg['id'])
    replace_items['unix_socket_dir_place_holder'] = datadir

    conf_str = param_replace(config_template, replace_items)
    cnf_file = open(conf_path, 'w')
    cnf_file.write(conf_str)
    cnf_file.close()
    os.system('echo "host all all ' + compcfg['ip'].strip() + '/32 trust" >> ' + datadir + '/pg_hba.conf')
    os.system('echo "host all all 127.0.0.1/32  trust" >> ' + datadir + '/pg_hba.conf')
    os.system('echo "host all agent 0.0.0.0/0 reject" >> ' + datadir + '/pg_hba.conf')
    os.system('echo "host all all 0.0.0.0/0 md5" >> ' + datadir + '/pg_hba.conf')

    # startup postgres, put log file in datadir too
    pg_logfp = datadir + "/logfile-" + str(compcfg['port'])
    startup_cmd = cmd0 + cmd1 + install_path + '/bin/postgres -D ' + datadir + " > " + pg_logfp + " 2>&1 &"
    os.system(startup_cmd)
    os.system('sleep 5'); # wait for postgres to startup

    # add initial user for clients to use later.
    # TODO: use more restricted privs than superuser
    sql = "set skip_tidsync = true; CREATE USER agent PASSWORD 'agent_pwd' superuser; CREATE USER " + compcfg['user'].strip() + " PASSWORD '" + compcfg['password'] + '\' superuser;'
    psql_cmd = cmd0 + install_path + "/bin/psql -h localhost -p" + str(compcfg['port']) + " -U " + getpass.getuser() + " -d postgres -c \"" + sql + "\""
    os.system(psql_cmd)

    # append the new instance's port to datadir mapping into instance_list.txt
    if not os.path.exists(etc_path):
        os.mkdir(etc_path)
    os.system("echo \"" + str(compcfg['port']) + "==>" + datadir + "\" >> " + conf_list_file)

# install_pg.py --config /path/of/config/file --install_ids comma-seperated-comp_id-list|all
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Install the computing node.')
    parser.add_argument('--config', type=str, help="The config path", required=True)
    parser.add_argument('--install_ids', type=str, help = "The ids for the nodes to install", default='all')
    args = parser.parse_args()
    try:
        if args.install_ids != "all":
            idstr = args.install_ids.split(',')
            install_ids = []
            for id in idstr:
                install_ids.append(int(id))
        else:
            install_ids = [-1]

        install_path = os.path.dirname(os.getcwd())
        config_template_file = install_path + "/resources/postgresql.conf"
        print "Installing computing nodes, please wait..."
        jsconf = open(args.config)
        jstr = jsconf.read()
        jscfg = json.loads(jstr)
        idx = 0
        for compcfg in jscfg:
            if install_ids[0] == -1 or install_ids.count(compcfg['id']) > 0:
                install_pg(config_template_file, install_path, compcfg)
                print "Installation completed for instance in " + compcfg['datadir'].strip()
        print "Installation of computing nodes with ID(s) " + str(install_ids) + " in file " + args.config + " completed."
    except KeyError, e:
        print 'install_pg.py --config /path/of/config/file --install_ids comma-seperated-comp_id-list|all'
        print e
