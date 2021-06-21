#!/bin/python2
# Copyright (c) 2019 ZettaDB inc. All rights reserved.
# This source code is licensed under Apache 2.0 License,
# combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

import sys
import json
import getpass
import re
import time
import uuid
import os
import os.path

defuser=getpass.getuser()
defbase='/kunlun'

def addIpToMachineMap(map, ip):
    global defuser
    global defbase
    if not map.has_key(ip):
	mac={"ip":ip, "user":defuser, "basedir":defbase}
	map[ip] = mac

def addMachineToMap(map, ip, user, basedir):
    mac={"ip":ip, "user":user, "basedir":basedir}
    map[ip] = mac

def addIpToFilesMap(map, ip, fname, targetdir):
    if not map.has_key(ip):
	map[ip] = {}
    tmap = map[ip]
    if not tmap.has_key(fname):
	tmap[fname] = targetdir

def addNodeToFilesMap(map, node, fname, targetdir):
    ip = node['ip']
    addIpToFilesMap(map, ip, fname, targetdir)

def addNodeToIpset(set, node):
    ip = node['ip']
    set.add(ip)

# Not used currently.
def addToCommandsMap(map, ip, targetdir, command):
    if not map.has_key(ip):
	map[ip] = []
    cmds = map[ip]
    cmds.append([targetdir, command])

def addToCommandsList(cmds, ip, targetdir, command):
    lst = [ip, targetdir, command]
    cmds.append(lst)

def addToDirMap(map, ip, newdir):
    if not map.has_key(ip):
	map[ip] = []
    dirs = map[ip]
    dirs.append(newdir)

def getuuid():
    return str(uuid.uuid1())

def getuuid_from_cnf(cnfpath):
    cnf = open(cnfpath)
    for line in cnf.readlines():
	if re.match('group_replication_group_name', line):
	    line=re.sub(r'\n','',line)
	    linea=re.sub(r'[ #].*', '',line)
	    lineb=re.sub('group_replication_group_name=','', linea)
	    linec=re.sub(r'[\'\"]','',lineb)
	    return linec
    return None

def generate_install_scripts(jscfg):
    global defuser
    global defbase
    localip = '127.0.0.1'

    machines = {}
    for mach in jscfg['machines']:
	ip=mach['ip']
	user=mach.get('user', defuser)
	base=mach.get('basedir', defbase)
	addMachineToMap(machines, ip, user, base)

    filesmap = {}
    commandslist = []
    dirmap = {}
    usemgr=True

    cluster = jscfg['cluster']
    cluster_name = cluster['name']
    meta = cluster['meta']
    if not meta.has_key('group_uuid'):
	    meta['group_uuid'] = getuuid()
    my_metaname = 'mysql_meta.json'
    metaf = open(r'install/%s' % my_metaname,'w')
    json.dump(meta, metaf, indent=4)
    metaf.close()

    # commands like:
    # sudo python2 install-mysql.py dbcfg=./template.cnf mgr_config=./mysql_meta.json target_node_index=0
    targetdir='percona-8.0.18-bin-rel/dba_tools'
    i=0
    secmdlist=[]
    for node in meta['nodes']:
	addNodeToFilesMap(filesmap, node, my_metaname, targetdir)
	addIpToMachineMap(machines, node['ip'])
	cmdpat = r'sudo python2 install-mysql.py dbcfg=./template.cnf mgr_config=./%s target_node_index=%d'
	if node.get('is_primary', False):
		addToCommandsList(commandslist, node['ip'], targetdir, cmdpat % (my_metaname, i))
	else:
		addToCommandsList(secmdlist, node['ip'], targetdir, cmdpat % (my_metaname, i))
	addToDirMap(dirmap, node['ip'], node['data_dir_path'])
	addToDirMap(dirmap, node['ip'], node['log_dir_path'])
	i+=1

    targetdir='percona-8.0.18-bin-rel/dba_tools'
    datas = cluster['data']
    i=1
    pries = []
    secs = []
    for shard in datas:
	    if not shard.has_key('group_uuid'):
		    shard['group_uuid'] = getuuid()
	    my_shardname = "mysql_shard%d.json" % i
	    shardf = open(r'install/%s' % my_shardname, 'w')
	    json.dump(shard, shardf, indent=4)
	    shardf.close()
	    j = 0
	    for node in shard['nodes']:
		addNodeToFilesMap(filesmap, node, my_shardname, targetdir)
		addIpToMachineMap(machines, node['ip'])
		cmdpat = r'sudo python2 install-mysql.py dbcfg=./template.cnf mgr_config=./%s target_node_index=%d'
		if node.get('is_primary', False):
			pries.append([node['ip'], targetdir, cmdpat % (my_shardname, j)])
		else:
			secs.append([node['ip'], targetdir, cmdpat % (my_shardname, j)])
		addToDirMap(dirmap, node['ip'], node['data_dir_path'])
		addToDirMap(dirmap, node['ip'], node['log_dir_path'])
		j += 1
	    if j == 1:
		usemgr=False
	    i+=1
    extraopt = " "
    if not usemgr:
        extraopt = " usemgr=False"
    for item in pries:
        addToCommandsList(commandslist, item[0], item[1], item[2] + extraopt)
    for item in secs:
        addToCommandsList(secmdlist, item[0], item[1], item[2] + extraopt)
    commandslist.extend(secmdlist)
    # This only needs to transfered to machine creating the cluster.
    pg_metaname = 'postgres_meta.json'
    metaf = open(r'install/%s' % pg_metaname, 'w')
    objs = []
    for node in meta['nodes']:
	obj = {}
	obj['ip'] = node['ip']
	obj['port'] = node['port']
	obj['user'] = "pgx"
	obj['password'] = "pgx_pwd"
	objs.append(obj)
    json.dump(objs, metaf, indent=4)
    metaf.close()

    # This only needs to transfered to machine creating the cluster.
    pg_shardname = 'postgres_shards.json'
    shardf = open(r'install/%s' % pg_shardname, 'w')
    shards = []
    i=1
    for shard in datas:
	obj={'shard_name': "shard%d" % i}
	i+=1
	nodes=[]
	for node in shard['nodes']:
	    n={'user':'pgx', 'password':'pgx_pwd'}
	    n['ip'] = node['ip']
	    n['port'] = node['port']
	    nodes.append(n)
	obj['shard_nodes'] = nodes
	shards.append(obj)
    json.dump(shards, shardf, indent=4)
    shardf.close()

    comps = cluster['comp']['nodes']
    pg_compname = 'postgres_comp.json'
    compf = open(r'install/%s' % pg_compname, 'w')
    json.dump(comps, compf, indent=4)
    compf.close()

    # python2 install_pg.py config=docker-comp.json install_ids=1,2,3
    targetdir="postgresql-11.5-rel/scripts"
    for node in comps:
	addNodeToFilesMap(filesmap, node, pg_compname, targetdir)
	addIpToMachineMap(machines, node['ip'])
	cmdpat = r'python2 install_pg.py  config=./%s install_ids=%d'
	if not usemgr:
	    cmdpat = cmdpat + " usemgr=False"
	addToCommandsList(commandslist, node['ip'], targetdir, cmdpat % (pg_compname, node['id']))
	addToDirMap(dirmap, node['ip'], node['datadir'])
    comp1 = comps[0]
    addNodeToFilesMap(filesmap, comp1, pg_metaname, targetdir)
    addNodeToFilesMap(filesmap, comp1, pg_shardname, targetdir)
    resourcedir = "postgresql-11.5-rel/resources"
    cmdpat=r'/bin/bash build_driver.sh'
    addToCommandsList(commandslist, comp1['ip'], resourcedir, cmdpat)
    cmdpat=r'python2 bootstrap.py --config=./%s --bootstrap_sql=./meta_inuse.sql'
    addToCommandsList(commandslist, comp1['ip'], targetdir, cmdpat % pg_metaname)
    cmdpat='python2 create_cluster.py --shards_config ./%s \
--comps_config ./%s  --meta_config ./%s --cluster_name %s --cluster_owner abc --cluster_biz test'
    if not usemgr:
        cmdpat = cmdpat + " --usemgr False"
    addToCommandsList(commandslist, comp1['ip'], targetdir,
    	cmdpat % (pg_shardname, pg_compname, pg_metaname, cluster_name))

    # bin/cluster_mgr docker_mgr.cnf >/dev/null 2>/dev/null </dev/null &
    mgr_name = 'clustermgr.cnf'
    mgrf = open(r'install/%s' % mgr_name, 'w')
    mgrtempf = open(r'clustermgr.cnf.template','r')
    firstmeta = meta['nodes'][0]
    for line in mgrtempf:
	newline = re.sub('META_HOST', firstmeta['ip'], line)
	newline = re.sub('META_PORT', str(firstmeta['port']), newline)
	mgrf.write(newline)
    mgrtempf.close()
    mgrf.close()
    targetdir="cluster_mgr_rel"
    addIpToMachineMap(machines, cluster['clustermgr']['ip'])
    addIpToFilesMap(filesmap, cluster['clustermgr']['ip'], mgr_name, targetdir)
    cmdpat = r'bin/cluster_mgr %s >/dev/null 2>/dev/null </dev/null &'
    addToCommandsList(commandslist, cluster['clustermgr']['ip'], targetdir, cmdpat % mgr_name)

    com_name = 'commands.sh'
    comf = open(r'install/%s' % com_name, 'w')
    comf.write('#! /bin/bash\n')

    # dir making
    for ip in dirmap:
	mach = machines.get(ip)
	dirs=dirmap[ip]
	dirs.append(mach['basedir'])
	for d in dirs:
	    mkstr = "bash remote_run.sh --user=%s %s 'sudo mkdir -p %s; sudo chown -R %s:%s %s'\n"
	    tup= (mach['user'], ip, d, mach['user'], mach['user'], d)
	    comf.write(mkstr % tup)

    # files copy and extract
    for ip in filesmap:
	mach = machines.get(ip)

	# Set up the files
	comstr = "bash dist.sh --hosts=%s --user=%s %s %s\n"
	comf.write(comstr % (ip, mach['user'], 'percona-8.0.18-bin-rel.tgz', mach['basedir']))
	comf.write(comstr % (ip, mach['user'], 'postgresql-11.5-rel.tgz', mach['basedir']))
	comf.write(comstr % (ip, mach['user'], 'cluster_mgr_rel.tgz', mach['basedir']))
	extstr = "bash remote_run.sh --user=%s %s 'cd %s; tar -xzf %s'\n"
	comf.write(extstr % (mach['user'], ip, mach['basedir'], 'percona-8.0.18-bin-rel.tgz'))
	comf.write(extstr % (mach['user'], ip, mach['basedir'], 'postgresql-11.5-rel.tgz'))
	comf.write(extstr % (mach['user'], ip, mach['basedir'], 'cluster_mgr_rel.tgz'))

	# files
	filesmap[ip]['build_driver.sh'] = 'postgresql-11.5-rel/resources'
	filesmap[ip]['process_deps.sh'] = '.'
	fmap = filesmap[ip]
	for fname in fmap:
	    comstr = "bash dist.sh --hosts=%s --user=%s install/%s %s/%s\n"
	    tup=(ip, mach['user'], fname, mach['basedir'], fmap[fname])
	    comf.write(comstr % tup)

	comstr = "bash remote_run.sh --user=%s %s 'cd %s; echo > postgresql-11.5-rel/etc/instances_list.txt 2>/dev/null'\n"
	comf.write(comstr % (mach['user'], ip, mach['basedir']))
	comstr = "bash remote_run.sh --user=%s %s 'cd %s; echo > percona-8.0.18-bin-rel/etc/instances_list.txt 2>/dev/null'\n"
	comf.write(comstr % (mach['user'], ip, mach['basedir']))

	# Set up the env.sh
	comstr = "bash dist.sh --hosts=%s --user=%s env.sh.template %s\n"
	extstr = ''' bash remote_run.sh --user=%s %s "cd %s; sed -s 's#KUNLUN_BASEDIR#%s#g' env.sh.template > env.sh" '''
	tup=(ip, mach['user'], mach['basedir'])
	exttup=(mach['user'], ip, mach['basedir'], mach['basedir'])
	comf.write(comstr % tup)
	comf.write(extstr % exttup)
	comf.write("\n")

	comstr = "bash remote_run.sh --user=%s %s 'cd %s; source ./env.sh; cd percona-8.0.18-bin-rel/lib; bash ../../process_deps.sh'\n"
	comf.write(comstr % (mach['user'], ip, mach['basedir']))
	comstr = "bash remote_run.sh --user=%s %s 'cd %s; source ./env.sh; cd postgresql-11.5-rel/lib; bash ../../process_deps.sh'\n"
	comf.write(comstr % (mach['user'], ip, mach['basedir']))

    # The reason for not using commands map is that,
    # we need to keep the order for the commands.
    for cmd in commandslist:
	ip=cmd[0]
	mach = machines[ip]
	mkstr = "bash remote_run.sh --user=%s %s 'cd %s; source ./env.sh; cd %s; %s'\n"
	tup= (mach['user'], ip, mach['basedir'], cmd[1], cmd[2])
	comf.write(mkstr % tup)

    comf.close()

# The order is meta shard -> data shards -> cluster_mgr -> comp nodes
def generate_start_scripts(jscfg):
    global defuser
    global defbase
    localip = '127.0.0.1'

    machines = {}
    for mach in jscfg['machines']:
	ip=mach['ip']
	user=mach.get('user', defuser)
	base=mach.get('basedir', defbase)
	addMachineToMap(machines, ip, user, base)

    filesmap = {}
    commandslist = []
    
    cluster = jscfg['cluster']
    meta = cluster['meta']
    # commands like:
    # bash startmysql.sh [port]
    targetdir='percona-8.0.18-bin-rel/dba_tools'
    for node in meta['nodes']:
	addIpToMachineMap(machines, node['ip'])
	cmdpat = r'bash startmysql.sh %s'
	addToCommandsList(commandslist, node['ip'], targetdir, cmdpat % node['port'])

    # bash startmysql.sh [port]
    targetdir='percona-8.0.18-bin-rel/dba_tools'
    datas = cluster['data']
    for shard in datas:
	    for node in shard['nodes']:
		addIpToMachineMap(machines, node['ip'])
		cmdpat = r'bash startmysql.sh %s'
		addToCommandsList(commandslist, node['ip'], targetdir, cmdpat % node['port'])
    
    # bin/cluster_mgr docker_mgr.cnf >/dev/null 2>/dev/null </dev/null &
    mgr_name = 'clustermgr.cnf'
    targetdir="cluster_mgr_rel"
    cmdpat = r'bin/cluster_mgr %s >/dev/null 2>/dev/null </dev/null &'
    addToCommandsList(commandslist, cluster['clustermgr']['ip'], targetdir, cmdpat % mgr_name)

    # su postgres -c "python2 start_pg.py port=5401"
    comps = cluster['comp']['nodes']
    targetdir="postgresql-11.5-rel/scripts"
    for node in comps:
	addIpToMachineMap(machines, node['ip'])
	cmdpat = r'python2 start_pg.py port=%d'
	addToCommandsList(commandslist, node['ip'], targetdir, cmdpat % node['port'])

    com_name = 'commands.sh'
    comf = open(r'start/%s' % com_name, 'w')
    comf.write('#! /bin/bash\n')

    for cmd in commandslist:
	ip=cmd[0]
	mach = machines[ip]
	mkstr = "bash remote_run.sh --user=%s %s 'cd %s; source ./env.sh; cd %s; %s'\n"
	tup= (mach['user'], ip, mach['basedir'], cmd[1], cmd[2])
	comf.write(mkstr % tup)

    comf.close()

# The order is: comp-nodes -> cluster_mgr -> data shards -> meta shard
def generate_stop_scripts(jscfg):
    global defuser
    global defbase
    localip = '127.0.0.1'

    machines = {}
    for mach in jscfg['machines']:
	ip=mach['ip']
	user=mach.get('user', defuser)
	base=mach.get('basedir', defbase)
	addMachineToMap(machines, ip, user, base)

    commandslist = []
    cluster = jscfg['cluster']

    # pg_ctl -D %s stop"
    comps = cluster['comp']['nodes']
    targetdir="postgresql-11.5-rel/scripts"
    for node in comps:
	addIpToMachineMap(machines, node['ip'])
	cmdpat = r'pg_ctl -D %s stop -m immediate'
	addToCommandsList(commandslist, node['ip'], targetdir, cmdpat % node['datadir'])

    # ps -fe | grep cluster_mgr | grep -v grep | awk '{print \$2}' | xargs kill -9
    targetdir="cluster_mgr_rel"
    cmdpat = r"ps -fe | grep cluster_mgr | grep -v grep | awk '{print \$2}' | xargs kill -9"
    addToCommandsList(commandslist, cluster['clustermgr']['ip'], targetdir, cmdpat)

    # bash startmysql.sh [port]
    targetdir='percona-8.0.18-bin-rel/dba_tools'
    datas = cluster['data']
    for shard in datas:
	    for node in shard['nodes']:
		addIpToMachineMap(machines, node['ip'])
		cmdpat = r'bash stopmysql.sh %d'
		addToCommandsList(commandslist, node['ip'], targetdir, cmdpat % node['port'])

    meta = cluster['meta']
    # commands like:
    # mysqladmin --defaults-file=/kunlun/percona-8.0.18-bin-rel/etc/my_6001.cnf -uroot -proot shutdown
    targetdir='percona-8.0.18-bin-rel/dba_tools'
    for node in meta['nodes']:
	addIpToMachineMap(machines, node['ip'])
	cmdpat = r'bash stopmysql.sh %d'
	addToCommandsList(commandslist, node['ip'], targetdir, cmdpat % node['port'])
    
    com_name = 'commands.sh'
    comf = open(r'stop/%s' % com_name, 'w')
    comf.write('#! /bin/bash\n')

    for cmd in commandslist:
	ip=cmd[0]
	mach = machines[ip]
	mkstr = 'bash remote_run.sh --user=%s %s "cd %s; source ./env.sh; cd %s; %s"\n'
	tup= (mach['user'], ip, mach['basedir'], cmd[1], cmd[2])
	comf.write(mkstr % tup)

    comf.close()

# The order is: comp-nodes -> cluster_mgr -> data shards -> meta shard
def generate_clean_scripts(jscfg):
    global defuser
    global defbase
    localip = '127.0.0.1'

    machines = {}
    for mach in jscfg['machines']:
	ip=mach['ip']
	user=mach.get('user', defuser)
	base=mach.get('basedir', defbase)
	addMachineToMap(machines, ip, user, base)

    commandslist = []
    cluster = jscfg['cluster']

    # pg_ctl -D %s stop"
    comps = cluster['comp']['nodes']
    targetdir="postgresql-11.5-rel/scripts"
    for node in comps:
	addIpToMachineMap(machines, node['ip'])
	cmdpat = r'pg_ctl -D %s stop -m immediate'
	addToCommandsList(commandslist, node['ip'], targetdir, cmdpat % node['datadir'])
	cmdpat = r'sudo rm -fr %s/*'
	addToCommandsList(commandslist, node['ip'], targetdir, cmdpat % node['datadir'])

    # ps -fe | grep cluster_mgr | grep -v grep | awk '{print \$2}' | xargs kill -9
    targetdir="cluster_mgr_rel"
    cmdpat = r"ps -fe | grep cluster_mgr | grep -v grep | awk '{print \$2}' | xargs kill -9"
    addToCommandsList(commandslist, cluster['clustermgr']['ip'], targetdir, cmdpat)

    # bash startmysql.sh [port]
    targetdir='percona-8.0.18-bin-rel/dba_tools'
    datas = cluster['data']
    for shard in datas:
	    for node in shard['nodes']:
		addIpToMachineMap(machines, node['ip'])
		cmdpat = r'bash stopmysql.sh %d'
		addToCommandsList(commandslist, node['ip'], targetdir, cmdpat % node['port'])
		cmdpat = r'sudo rm -fr %s/*'
		addToCommandsList(commandslist, node['ip'], targetdir, cmdpat % node['log_dir_path'])
		addToCommandsList(commandslist, node['ip'], targetdir, cmdpat % node['data_dir_path'])

    meta = cluster['meta']
    # commands like:
    # mysqladmin --defaults-file=/kunlun/percona-8.0.18-bin-rel/etc/my_6001.cnf -uroot -proot shutdown
    targetdir='percona-8.0.18-bin-rel/dba_tools'
    for node in meta['nodes']:
	addIpToMachineMap(machines, node['ip'])
	cmdpat = r'bash stopmysql.sh %d'
	addToCommandsList(commandslist, node['ip'], targetdir, cmdpat % node['port'])
	cmdpat = r'sudo rm -fr %s/*'
	addToCommandsList(commandslist, node['ip'], targetdir, cmdpat % node['log_dir_path'])
	addToCommandsList(commandslist, node['ip'], targetdir, cmdpat % node['data_dir_path'])

    for ip in machines:
	    mach =machines[ip]
	    cmdpat = 'sudo rm -fr %s/*'
	    addToCommandsList(commandslist, ip, "/", cmdpat % mach['basedir'])
    com_name = 'commands.sh'
    comf = open(r'clean/%s' % com_name, 'w')
    comf.write('#! /bin/bash\n')

    for cmd in commandslist:
	ip=cmd[0]
	mach = machines[ip]
	mkstr = 'bash remote_run.sh --user=%s %s "cd %s; source ./env.sh; cd %s; %s"\n'
	tup= (mach['user'], ip, mach['basedir'], cmd[1], cmd[2])
	comf.write(mkstr % tup)

    comf.close()

def addDefFilesForBackup(filesmap, ip):
    targetdir='backup'
    files=['backupmysql.sh', 'backuppostgresql.sh', 'check_gtid.py']
    for f in files:
	addIpToFilesMap(filesmap, ip, f, targetdir)

def generate_backup_init(jscfg, fulldir, incrdir):
    global defuser
    global defbase
    localip = '127.0.0.1'

    machines = {}
    for mach in jscfg['machines']:
	ip=mach['ip']
	user=mach.get('user', defuser)
	base=mach.get('basedir', defbase)
	addMachineToMap(machines, ip, user, base)

    filesmap = {}
    commandslist = []
    dirmap = {}
    
    cluster = jscfg['cluster']
    meta = cluster['meta']

    com_name = 'commands.sh'
    comf = open(r'backup/%s' % com_name, 'w')
    comf.write('#! /bin/bash\n')

    metafulldir="%s/meta" % fulldir
    metaincrdir="%s/meta" % incrdir
    comf.write('mkdir -p %s/meta\n' % metafulldir)
    comf.write('mkdir -p %s/meta\n' % metaincrdir)
    for node in meta['nodes']:
	# All the log/dir should exists and has data, so no need to create.
	addIpToMachineMap(machines, node['ip'])
	addToDirMap(dirmap, node['ip'], node['backupdir'])
	addDefFilesForBackup(filesmap, node['ip'])

    datas = cluster['data']
    i=1
    for shard in datas:
	shardname="shard%d" % i
	shardfulldir="%s/%s" % (fulldir, shardname)
	shardincrdir="%s/%s" % (incrdir, shardname)
	comf.write('mkdir -p %s\n' % shardfulldir)
	comf.write('mkdir -p %s\n' % shardincrdir)
	for node in shard['nodes']:
	    # All the log/dir should exists and has data, so no need to create.
	    addIpToMachineMap(machines, node['ip'])
	    addToDirMap(dirmap, node['ip'], node['backupdir'])
	    addDefFilesForBackup(filesmap, node['ip'])
	i+=1
    
    # For comp nodes, we always do full backup using pg_dump
    comps = cluster['comp']['nodes']
    compfulldir="%s/comp" % fulldir
    comf.write('mkdir -p %s\n' % compfulldir)
    for node in comps:
	addIpToMachineMap(machines, node['ip'])
	addToDirMap(dirmap, node['ip'], node['backupdir'])
	addDefFilesForBackup(filesmap, node['ip'])

    # dir making
    for ip in dirmap:
	mach = machines.get(ip)
	dirs=dirmap[ip]
	backupdir = mach['basedir']+"/backup"
	dirs.append(backupdir)
	for d in dirs:
	    mkstr = "bash remote_run.sh --user=%s %s 'sudo mkdir -p %s; sudo chown -R %s:%s %s'\n"
	    tup= (mach['user'], ip, d, mach['user'], mach['user'], d)
	    comf.write(mkstr % tup)

    # copy files to remote machines.
    for ip in filesmap:
	mach = machines.get(ip)
	fmap = filesmap[ip]
	for fname in fmap:
	    comstr = "bash dist.sh --hosts=%s --user=%s backup/%s %s/%s\n"
	    tup=(ip, mach['user'], fname, mach['basedir'], fmap[fname])
	    comf.write(comstr % tup)

    comf.close()

def generate_backup_scripts(jscfg, backuptype, fulldir, incrdir):
    global defuser
    global defbase
    localip = '127.0.0.1'

    curtime = str(long(time.time()))
    machines = {}
    for mach in jscfg['machines']:
	ip=mach['ip']
	user=mach.get('user', defuser)
	base=mach.get('basedir', defbase)
	addMachineToMap(machines, ip, user, base)

    filesmap = {}
    commandslist = []
    mysqlbackups=[]
    
    cluster = jscfg['cluster']
    meta = cluster['meta']

    com_name = 'commands.sh'
    comf = open(r'backup/%s' % com_name, 'w')
    comf.write('#! /bin/bash\n')

    metafulldir="%s/meta" % fulldir
    metaincrdir="%s/meta" % incrdir
    metagtid = "0"
    gtidfile ="%s/gtid" % metaincrdir;
    if backuptype == 'incremental' and os.access(gtidfile, os.R_OK):
	with open(gtidfile, 'r') as f:
	    metagtid=f.read()
    if metagtid == '':
	metagtid='0'
    isfull=False
    if metagtid == '0':
	isfull=True
    targetdir=metafulldir
    if not isfull:
        targetdir="%s/%s" % (metaincrdir, curtime)
        comf.write("mkdir -p %s\n" % targetdir)
    mysqlbackups.append([targetdir, gtidfile])
    for node in meta['nodes']:
	# All the log/dir should exists and has data, so no need to create.
	addIpToMachineMap(machines, node['ip'])
	mach = machines.get(node['ip'])
	backupdir = mach['basedir']+"/backup"
	# data_dir_path and user is not used currently.
	cmdpat = r'bash backupmysql.sh %d %s %s %s'
	addToCommandsList(commandslist, node['ip'], backupdir, cmdpat % (node['port'],
	    node['log_dir_path'], node['backupdir'], metagtid))
	fname = "%s/backup.tar.gz" % node['backupdir']
	addIpToFilesMap(filesmap, node['ip'], fname, targetdir)

    i = 1
    datas = cluster['data']
    for shard in datas:
	shardname="shard%d" % i
	shardfulldir="%s/%s" % (fulldir, shardname)
	shardincrdir="%s/%s" % (incrdir, shardname)
	gtid="0"
	gtidfile ="%s/gtid" % shardincrdir;
	if backuptype == 'incremental' and os.access(gtidfile, os.R_OK):
	    with open(gtidfile, 'r') as f:
		gtid=f.read()
	if gtid == "":
	    gtid="0"
	isfull = False
	if gtid == '0':
	    isfull=True
	targetdir=shardfulldir
	if not isfull:
	    targetdir="%s/%s" % (shardincrdir, curtime)
	    comf.write("mkdir -p %s\n" % targetdir)
	mysqlbackups.append([targetdir, gtidfile])
	for node in shard['nodes']:
	    # All the log/dir should exists and has data, so no need to create.
	    addIpToMachineMap(machines, node['ip'])
	    mach = machines.get(node['ip'])
	    backupdir = mach['basedir']+"/backup"
	    cmdpat = r'bash backupmysql.sh %d %s %s %s'
	    addToCommandsList(commandslist, node['ip'], backupdir, cmdpat % (node['port'],
		node['log_dir_path'], node['backupdir'], gtid))
	    fname = "%s/backup.tar.gz" % node['backupdir']
	    addIpToFilesMap(filesmap, node['ip'], fname, targetdir)
    	i+=1

    compfulldir="%s/comp" % fulldir
    targetdir="%s/%s" % (compfulldir, curtime)
    comf.write("mkdir -p %s\n" % targetdir)
    comps = cluster['comp']['nodes']
    for node in comps:
	addIpToMachineMap(machines, node['ip'])
	mach = machines.get(node['ip'])
	backupdir = mach['basedir']+"/backup"
	cmdpat = r'bash backuppostgresql.sh %d %s'
	addToCommandsList(commandslist, node['ip'], backupdir, cmdpat % (node['port'], node['backupdir']))
	fname = "%s/backup.tar.gz" % node['backupdir']
	addIpToFilesMap(filesmap, node['ip'], fname, targetdir)

    for cmd in commandslist:
	ip=cmd[0]
	mach = machines[ip]
	mkstr = "bash remote_run.sh --user=%s %s 'cd %s; source ./env.sh; cd %s; %s'\n"
	tup= (mach['user'], ip, mach['basedir'], cmd[1], cmd[2])
	comf.write(mkstr % tup)

    # copy files back.
    for ip in filesmap:
	mach = machines.get(ip)
	fmap = filesmap[ip]
	for fname in fmap:
	    name = os.path.basename(fname)
	    comstr = "test -f %s/%s || bash getback.sh --hosts=%s --user=%s %s %s\n"
	    tup=(fmap[fname], name, ip, mach['user'], fname, fmap[fname])
	    comf.write(comstr % tup)

    for ba in mysqlbackups:
	tdir=ba[0]
	gtidpath=ba[1]
	comf.write("cd %s; tar -xzf backup.tar.gz; cd -; cp -fr %s/gtid %s\n" % (tdir, tdir, gtidpath))

    comf.close()

def generate_restore_scripts(jscfg):
    global defuser
    global defbase
    localip = '127.0.0.1'

    machines = {}
    for mach in jscfg['machines']:
	ip=mach['ip']
	user=mach.get('user', defuser)
	base=mach.get('basedir', defbase)
	addMachineToMap(machines, ip, user, base)

    filesmap = {}
    commandslist = []
    dirmap = {}
    usemgr=True

    cluster = jscfg['cluster']
    cluster_name = cluster['name']
    meta = cluster['meta']
    metafulldir = meta['fullbackupdir']
    metaincrdir = meta['incrbackupdir']
    del meta['fullbackupdir']
    del meta['incrbackupdir']
    metauuid = getuuid_from_cnf("%s/my.cnf" % metafulldir)
    meta['group_uuid'] = metauuid;
    my_metaname = 'mysql_meta.json'
    metaf = open(r'restore/%s' % my_metaname,'w')
    json.dump(meta, metaf, indent=4)
    metaf.close()

    # commands like:
    # sudo python2 install-mysql.py dbcfg=./template.cnf mgr_config=./mysql_meta.json target_node_index=0
    targetdir='percona-8.0.18-bin-rel/dba_tools'
    i=0
    pricmdlist=[]
    secmdlist=[]
    cleanlist=[]
    for node in meta['nodes']:
	name = 'meta';
	addNodeToFilesMap(filesmap, node, "restore/%s" % my_metaname, targetdir)
	addIpToMachineMap(machines, node['ip'])
	addToDirMap(dirmap, node['ip'], node['data_dir_path'])
	addToDirMap(dirmap, node['ip'], node['log_dir_path'])
	mach = machines.get(node['ip'])
	backuptargetd = "restore/meta"
	addToDirMap(dirmap, node['ip'], "%s/%s" % (mach['basedir'], backuptargetd))
	addNodeToFilesMap(filesmap, node, "%s/backup.tar.gz" % metafulldir, backuptargetd)
	cmdpat = r'tar -xzf backup.tar.gz'
	addToCommandsList(commandslist, node['ip'], backuptargetd, cmdpat)
	cmdpat = r'xtrabackup --prepare --target-dir=base > prepare.out'
	addToCommandsList(commandslist, node['ip'], backuptargetd, cmdpat)
	cmdpat = r'python2 restore-mysql.py dbcfg=./template.cnf config=%s target_node_index=%d'
	addToCommandsList(commandslist, node['ip'], targetdir, cmdpat % (my_metaname, i))
	cnfpath='%s/percona-8.0.18-bin-rel/etc/my_%d.cnf' % (mach['basedir'], node['port'])
	cmdpat = r'xtrabackup --defaults-file=%s --copy-back --target-dir=base > copyback.out' % cnfpath
	addToCommandsList(commandslist, node['ip'], backuptargetd, cmdpat)
	cmdpat = r'bash startmysql.sh %d'
	addToCommandsList(commandslist, node['ip'], targetdir, cmdpat % node['port'])
	cmdpat = r'bash wait_mysqlup.sh %d'
	addToCommandsList(commandslist, node['ip'], ".", cmdpat % node['port'])
	cmdpat = r'bash start_mgr.sh %d %s'
	if node.get('is_primary', False):
	    addToCommandsList(pricmdlist, node['ip'], ".", cmdpat % (node['port'], 'true'))
	    cleanpat = "mysql --defaults-file=%s -uroot -proot -e 'use Kunlun_Metadata_DB; drop table if exists commit_log_%s;'"
	    addToCommandsList(cleanlist, node['ip'], ".", cleanpat % (cnfpath, cluster_name))
	else:
	    addToCommandsList(secmdlist, node['ip'], ".", cmdpat % (node['port'], 'false'))
	i+=1

    targetdir='percona-8.0.18-bin-rel/dba_tools'
    datas = cluster['data']
    i=1
    for shard in datas:
	    fulldir = shard['fullbackupdir']
	    incrdir = shard['incrbackupdir']
	    del shard['fullbackupdir']
	    del shard['incrbackupdir']
	    shard['group_uuid'] = getuuid_from_cnf("%s/my.cnf" % fulldir)
	    my_shardname = "mysql_shard%d.json" % i
	    shardf = open(r'restore/%s' % my_shardname, 'w')
	    json.dump(shard, shardf, indent=4)
	    shardf.close()
	    shardname="shard%d" % i
	    j = 0
	    for node in shard['nodes']:
		addNodeToFilesMap(filesmap, node, "restore/%s" % my_shardname, targetdir)
		addIpToMachineMap(machines, node['ip'])
		addToDirMap(dirmap, node['ip'], node['data_dir_path'])
		addToDirMap(dirmap, node['ip'], node['log_dir_path'])
		mach = machines.get(node['ip'])
		backuptargetd = "restore/%s" % shardname
		addToDirMap(dirmap, node['ip'], "%s/%s" % (mach['basedir'], backuptargetd))
		addNodeToFilesMap(filesmap, node, "%s/backup.tar.gz" % fulldir, backuptargetd)
		cmdpat = r'tar -xzf backup.tar.gz'
		addToCommandsList(commandslist, node['ip'], backuptargetd, cmdpat)
		cmdpat = r'xtrabackup --prepare --target-dir=base > prepare.out'
		addToCommandsList(commandslist, node['ip'], backuptargetd, cmdpat)
		cmdpat = r'python2 restore-mysql.py dbcfg=./template.cnf config=%s target_node_index=%d'
		addToCommandsList(commandslist, node['ip'], targetdir, cmdpat % (my_shardname, j))
		cnfpath='%s/percona-8.0.18-bin-rel/etc/my_%d.cnf' % (mach['basedir'], node['port'])
		cmdpat = r'xtrabackup --defaults-file=%s --copy-back --target-dir=base > copyback.out' % cnfpath
		addToCommandsList(commandslist, node['ip'], backuptargetd, cmdpat)
		cmdpat = r'bash startmysql.sh %d'
		addToCommandsList(commandslist, node['ip'], targetdir, cmdpat % node['port'])
		cmdpat = r'bash wait_mysqlup.sh %d'
		addToCommandsList(commandslist, node['ip'], ".", cmdpat % node['port'])
		cmdpat = r'bash start_mgr.sh %d %s'
		if node.get('is_primary', False):
		    addToCommandsList(pricmdlist, node['ip'], ".", cmdpat % (node['port'], 'true'))
		else:
		    addToCommandsList(secmdlist, node['ip'], ".", cmdpat % (node['port'], 'false'))
		j += 1
	    if j == 1:
		usemgr=False
	    i+=1

    commandslist.extend(pricmdlist)
    commandslist.extend(secmdlist)
    commandslist.extend(cleanlist)
    # This only needs to transfered to machine creating the cluster.
    pg_metaname = 'postgres_meta.json'
    metaf = open(r'restore/%s' % pg_metaname, 'w')
    objs = []
    for node in meta['nodes']:
	obj = {}
	obj['ip'] = node['ip']
	obj['port'] = node['port']
	obj['user'] = "pgx"
	obj['password'] = "pgx_pwd"
	objs.append(obj)
    json.dump(objs, metaf, indent=4)
    metaf.close()

    # This only needs to transfered to machine creating the cluster.
    pg_shardname = 'postgres_shards.json'
    shardf = open(r'restore/%s' % pg_shardname, 'w')
    shards = []
    i=1
    for shard in datas:
	obj={'shard_name': "shard%d" % i}
	i+=1
	nodes=[]
	for node in shard['nodes']:
	    n={'user':'pgx', 'password':'pgx_pwd'}
	    n['ip'] = node['ip']
	    n['port'] = node['port']
	    nodes.append(n)
	obj['shard_nodes'] = nodes
	shards.append(obj)
    json.dump(shards, shardf, indent=4)
    shardf.close()

    comps = cluster['comp']['nodes']
    pg_compname = 'postgres_comp.json'
    compf = open(r'restore/%s' % pg_compname, 'w')
    json.dump(comps, compf, indent=4)
    compf.close()

    # python2 install_pg.py config=docker-comp.json install_ids=1,2,3
    compfulldir=cluster['comp']['fullbackupdir']
    targetdir="postgresql-11.5-rel/scripts"
    for node in comps:
	addNodeToFilesMap(filesmap, node, "restore/%s" % pg_compname, targetdir)
	addIpToMachineMap(machines, node['ip'])
	cmdpat = r'python2 install_pg.py  config=./%s install_ids=%d'
	addToCommandsList(commandslist, node['ip'], targetdir, cmdpat % (pg_compname, node['id']))
	addToDirMap(dirmap, node['ip'], node['datadir'])
	mach = machines.get(node['ip'])
	addToDirMap(dirmap, node['ip'], "%s/restore/comp" % mach['basedir'])

    comp1 = comps[0]
    addNodeToFilesMap(filesmap, comp1, "restore/%s" % pg_metaname, targetdir)
    addNodeToFilesMap(filesmap, comp1, "restore/%s" % pg_shardname, targetdir)
    resourcedir = "postgresql-11.5-rel/resources"
    cmdpat=r'/bin/bash build_driver.sh'
    addToCommandsList(commandslist, comp1['ip'], resourcedir, cmdpat)
    cmdpat=r'python2 bootstrap.py --config=./%s --bootstrap_sql=./clean_meta.sql'
    addToCommandsList(commandslist, comp1['ip'], targetdir, cmdpat % pg_metaname)
    cmdpat='python2 create_cluster.py --shards_config ./%s \
--comps_config ./%s  --meta_config ./%s --cluster_name %s --cluster_owner abc --cluster_biz test'
    addToCommandsList(commandslist, comp1['ip'], targetdir,
    	cmdpat % (pg_shardname, pg_compname, pg_metaname, cluster_name))

    # bin/cluster_mgr docker_mgr.cnf >/dev/null 2>/dev/null </dev/null &
    mgr_name = 'clustermgr.cnf'
    mgrf = open(r'install/%s' % mgr_name, 'w')
    mgrtempf = open(r'clustermgr.cnf.template','r')
    firstmeta = meta['nodes'][0]
    for line in mgrtempf:
	newline = re.sub('META_HOST', firstmeta['ip'], line)
	newline = re.sub('META_PORT', str(firstmeta['port']), newline)
	mgrf.write(newline)
    mgrtempf.close()
    mgrf.close()
    targetdir="cluster_mgr_rel"
    addIpToMachineMap(machines, cluster['clustermgr']['ip'])
    addIpToFilesMap(filesmap, cluster['clustermgr']['ip'], mgr_name, targetdir)
    cmdpat = r'bin/cluster_mgr %s >/dev/null 2>/dev/null </dev/null &'
    addToCommandsList(commandslist, cluster['clustermgr']['ip'], targetdir, cmdpat % mgr_name)

    com_name = 'commands.sh'
    comf = open(r'restore/%s' % com_name, 'w')
    comf.write('#! /bin/bash\n')

    # dir making
    for ip in dirmap:
	mach = machines.get(ip)
	dirs=dirmap[ip]
	dirs.append(mach['basedir'])
	for d in dirs:
	    mkstr = "bash remote_run.sh --user=%s %s 'sudo mkdir -p %s; sudo chown -R %s:%s %s'\n"
	    tup= (mach['user'], ip, d, mach['user'], mach['user'], d)
	    comf.write(mkstr % tup)


    # files copy and extract
    for ip in filesmap:
	mach = machines.get(ip)

	# Set up the files
	comstr = "bash dist.sh --hosts=%s --user=%s %s %s\n"
	comf.write(comstr % (ip, mach['user'], 'percona-8.0.18-bin-rel.tgz', mach['basedir']))
	comf.write(comstr % (ip, mach['user'], 'postgresql-11.5-rel.tgz', mach['basedir']))
	comf.write(comstr % (ip, mach['user'], 'cluster_mgr_rel.tgz', mach['basedir']))
	extstr = "bash remote_run.sh --user=%s %s 'cd %s; tar -xzf %s'\n"
	comf.write(extstr % (mach['user'], ip, mach['basedir'], 'percona-8.0.18-bin-rel.tgz'))
	comf.write(extstr % (mach['user'], ip, mach['basedir'], 'postgresql-11.5-rel.tgz'))
	comf.write(extstr % (mach['user'], ip, mach['basedir'], 'cluster_mgr_rel.tgz'))

	# files
	fmap = filesmap[ip]
	fmap['restore/restore-mysql.py'] = 'percona-8.0.18-bin-rel/dba_tools'
	fmap['restore/build_driver.sh'] = 'postgresql-11.5-rel/resources'
	fmap['restore/clean_meta.sql'] = 'postgresql-11.5-rel/scripts'
	fmap['restore/merge_incr.py'] = '.'
	fmap['restore/wait_mysqlup.sh'] = '.'
	fmap['restore/wait_pgup.sh'] = '.'
	fmap['restore/start_mgr.sh'] = '.'
	for fname in fmap:
	    comstr = "bash dist.sh --hosts=%s --user=%s %s %s/%s\n"
	    tup=(ip, mach['user'], fname, mach['basedir'], fmap[fname])
	    comf.write(comstr % tup)

	comstr = "bash remote_run.sh --user=%s %s 'cd %s; echo > postgresql-11.5-rel/etc/instances_list.txt 2>/dev/null'\n"
	comf.write(comstr % (mach['user'], ip, mach['basedir']))
	comstr = "bash remote_run.sh --user=%s %s 'cd %s; echo > percona-8.0.18-bin-rel/etc/instances_list.txt 2>/dev/null'\n"
	comf.write(comstr % (mach['user'], ip, mach['basedir']))

	# Set up the env.sh
	comstr = "bash dist.sh --hosts=%s --user=%s env.sh.template %s\n"
	extstr = ''' bash remote_run.sh --user=%s %s "cd %s; sed -s 's#KUNLUN_BASEDIR#%s#g' env.sh.template > env.sh" '''
	tup=(ip, mach['user'], mach['basedir'])
	exttup=(mach['user'], ip, mach['basedir'], mach['basedir'])
	comf.write(comstr % tup)
	comf.write(extstr % exttup)
	comf.write("\n")

    # The reason for not using commands map is that,
    # we need to keep the order for the commands.
    for cmd in commandslist:
	ip=cmd[0]
	mach = machines[ip]
	mkstr = "bash remote_run.sh --user=%s %s \"cd %s; source ./env.sh; cd %s; %s\"\n"
	tup= (mach['user'], ip, mach['basedir'], cmd[1], cmd[2])
	comf.write(mkstr % tup)

    comf.close()

def checkdirs(dirs):
    for d in dirs:
	if not os.path.exists(d):
	    os.mkdir(d)

def usage():
    print 'Usage: generate-scripts.py action=install|start|stop|clean|backup|restore config=/path/to/confile/file \
defuser=default_user defbase=default_base backuptype=full|incremental|init fulldir=path/to/full_backup incrdir=path/to/incr_backup'

if  __name__ == '__main__':
    args = dict([arg.split('=') for arg in sys.argv[1:]])
    action='install'
    if args.has_key('action'):
	action=args['action']
    else:
	args['action']=action

    if args.has_key('defuser'):
	defuser=args['defuser']
    if args.has_key('defbase'):
	defbase=args['defbase']

    if not args.has_key('config'):
	usage()
	sys.exit(1)

    actions=["install", "start", "stop", "clean"]
    checkdirs(actions)

    print str(args)

    jsconf = open(args['config'])
    jstr = jsconf.read()
    jscfg = json.loads(jstr)
    # print str(jscfg)

    if action == 'install':
	generate_install_scripts(jscfg)
    elif action == 'start':
	generate_start_scripts(jscfg)
    elif action == 'stop':
	generate_stop_scripts(jscfg)
    elif action == 'clean':
	generate_clean_scripts(jscfg)
    elif action == 'backup':
	if not args.has_key('fulldir'):
	    usage()
	    sys.exit(1)
	if not args.has_key('incrdir'):
	    usage()
	    sys.exit(1)
	if not args.has_key('backuptype'):
	    usage()
	    sys.exit(1)
	if args['backuptype'] == 'init':
	    generate_backup_init(jscfg, args['fulldir'], args['incrdir'])
	else:
	    generate_backup_scripts(jscfg, args['backuptype'], args['fulldir'], args['incrdir'])
    elif action == 'restore':
	generate_restore_scripts(jscfg)
    else:
	usage()
	sys.exit(1)
