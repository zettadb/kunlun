# Copyright (c) 2019 ZettaDB inc. All rights reserved.
# This source code is licensed under Apache 2.0 License,
# combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

# Test global deadlock detection using 2 computing nodes and 2 storage shards
import psycopg2
import argparse
import json
import time
import select

# computing node config file format:
#[
#	{
#		'host':'127.0.0.1',
#		'port':6401,
#		'user':'abc',
#		'password':'abc',
#		'database':'postgres'
#	},
#	{
#		'host':'127.0.0.1',
#		'port':6402,
#		'user':'abc',
#		'password':'abc',
#		'database':'postgres'
#	}
#]
def list_diff(l1,l2):
	l = []
	for i in l1:
		if i not in l2:
			l.append(i)
	return l

def trans_status_str(status):
	if status == psycopg2.extensions.TRANSACTION_STATUS_IDLE:
		ret = "TRANSACTION_STATUS_IDLE"
	elif status == psycopg2.extensions.TRANSACTION_STATUS_ACTIVE:
		ret = "TRANSACTION_STATUS_ACTIVE"
	elif status == psycopg2.extensions.TRANSACTION_STATUS_INTRANS:
		ret = "TRANSACTION_STATUS_INTRANS"
	elif status == psycopg2.extensions.TRANSACTION_STATUS_INERROR:
		ret = "TRANSACTION_STATUS_INERROR"
	else:
		ret = "TRANSACTION_STATUS_UNKNOWN"
	return ret

def poll_status_str(status):
	if status == psycopg2.extensions.POLL_OK:
		ret = "POLL_OK"
	elif status == psycopg2.extensions.POLL_READ :
		ret = "POLL_READ"
	elif status == psycopg2.extensions.POLL_WRITE :
		ret = "POLL_WRITE"
	elif status == psycopg2.extensions.POLL_ERROR :
		ret = "POLL_ERROR"
	else:
		ret = "UNKNOWN"
	return ret

# wait for results to return from all connections.
def wait_for_all_conns(conns):
	rlist = conns
	aborted_conn = None
	while True:
		wl = []
		rl = []
		for conn in rlist:
			try:
				state = conn.poll()
				if state == psycopg2.extensions.POLL_READ:
					rl.append(conn)
				elif state == psycopg2.extensions.POLL_WRITE:
					wl.append(conn)
				elif state == psycopg2.extensions.POLL_ERROR:
					print "Got poll error on connection: {}".format(conn)
				elif state == psycopg2.extensions.POLL_OK:
					if aborted_conn != None and aborted_conn is conn:
						print "Found gdd victim in connection: {}".format(str(conn))
						return conn
			except psycopg2.Error as pgerr:
				print "Got error on connection {} of {}, {}: {}.".format(str(conn), str(rlist), trans_status_str(conn.get_transaction_status()), str(pgerr))
				#rl.append(conn)
				#wl.append(conn)
				aborted_conn = conn

		rlist = []
		rlist = rl + wl
		if rlist == []:
			return aborted_conn
		try:
			selres = select.select(rl, wl, [])
		except psycopg2.Error as err:
			print "Got error at select.select({}, {}, []) : ".format(str(rl), str(wl), str(err))

def end_trans(conn,cur):
	ts = conn.get_transaction_status()
	if (ts == psycopg2.extensions.TRANSACTION_STATUS_INERROR):
		cur.execute("rollback")
		res = 'rollback'
	else:
		cur.execute("commit")
		res = 'commit'
	return res

def end_txns(allconns, concurs, killed_conn):
	other_concurs = []
	other_conns = []
	for concur in concurs:
		if killed_conn is concur[0]:
			end_trans(concur[0], concur[1])
		else:
			other_concurs.append(concur)
			other_conns.append(concur[0])

	# wait for deadlock victim to be aborted and others to return from the blocked update stmt.
	wait_for_all_conns(allconns)
	# commit txns in other conns
	for concur in other_concurs:
		res = end_trans(concur[0], concur[1])
	wait_for_all_conns(other_conns)

def end_concurs(concurs):
	for concur in concurs:
		concur[1].close()
		concur[0].close()

def form_2_nodes_cycle(nodes, same_compnode) :
	compcfg1 = nodes[0]
	if same_compnode:
		compcfg2 = nodes[0]
	else:
		compcfg2 = nodes[1]
	start_ts = time.time()
	conn1 = psycopg2.connect(host=compcfg1['host'], port=compcfg1['port'], user=compcfg1['user'], database=compcfg1['database'], password=compcfg1['password'], async=1)
	conn2 = psycopg2.connect(host=compcfg2['host'], port=compcfg2['port'], user=compcfg2['user'], database=compcfg2['database'], password=compcfg2['password'], async=1)
	allconns = [conn1,conn2]

	wait_for_all_conns(allconns)
	cur1 = conn1.cursor()
	cur2 = conn2.cursor()
	cur1.execute("start transaction") # GT1
	cur2.execute("start transaction") # GT2
	wait_for_all_conns(allconns)

	cur1.execute("update t1 set b=b+1 where a = 1")
	cur2.execute("update t1 set b=b+1 where a = 11")
	wait_for_all_conns(allconns)
   
	# GT1->GT2
	cur1.execute("update t1 set b=b+1 where a = 11;commit")
	# GT2->GT1
	cur2.execute("update t1 set b=b+1 where a = 1;commit")
	killed_conn = wait_for_all_conns(allconns)

	end_trans(killed_conn, killed_conn.cursor())
	print "Time taken: {} second(s)".format(time.time() - start_ts)

def form_3_nodes_cycle(nodes, same_compnode) :
	compcfg1 = nodes[0]
	if same_compnode:
		compcfg2 = nodes[0]
	else:
		compcfg2 = nodes[1]
	start_ts = time.time()
	conn1 = psycopg2.connect(host=compcfg1['host'], port=compcfg1['port'], user=compcfg1['user'], database=compcfg1['database'], password=compcfg1['password'], async=1)
	conn2 = psycopg2.connect(host=compcfg2['host'], port=compcfg2['port'], user=compcfg2['user'], database=compcfg2['database'], password=compcfg2['password'], async=1)
	conn3 = psycopg2.connect(host=compcfg2['host'], port=compcfg2['port'], user=compcfg2['user'], database=compcfg2['database'], password=compcfg2['password'], async=1)
	allconns = [conn1,conn2, conn3]

	wait_for_all_conns(allconns)
	cur1 = conn1.cursor()
	cur2 = conn2.cursor()
	cur3 = conn3.cursor()
	cur1.execute("start transaction") # GT1
	cur2.execute("start transaction") # GT2
	cur3.execute("start transaction") # GT3
	wait_for_all_conns(allconns)

	cur1.execute("update t1 set b=b+1 where a = 1")
	cur2.execute("update t1 set b=b+1 where a = 11")
	cur3.execute("update t1 set b=b+1 where a = 12")
	wait_for_all_conns(allconns)
   
	# GT1->GT2
	cur1.execute("update t1 set b=b+1 where a = 11;commit")
	# GT2->GT3
	cur2.execute("update t1 set b=b+1 where a = 12;commit")
	# GT3->GT1
	cur3.execute("update t1 set b=b+1 where a = 1;commit")
	killed_conn = wait_for_all_conns(allconns)
	end_trans(killed_conn, killed_conn.cursor())


	print "Time taken: {} second(s)".format(time.time() - start_ts)

def form_3_nodes_cycle_with_forks(nodes, same_compnode) :
	compcfg1 = nodes[0]
	if same_compnode:
		compcfg2 = nodes[0]
	else:
		compcfg2 = nodes[1]
	start_ts = time.time()
	conn1 = psycopg2.connect(host=compcfg1['host'], port=compcfg1['port'], user=compcfg1['user'], database=compcfg1['database'], password=compcfg1['password'], async=1)
	conn2 = psycopg2.connect(host=compcfg2['host'], port=compcfg2['port'], user=compcfg2['user'], database=compcfg2['database'], password=compcfg2['password'], async=1)
	conn3 = psycopg2.connect(host=compcfg1['host'], port=compcfg1['port'], user=compcfg1['user'], database=compcfg1['database'], password=compcfg1['password'], async=1)
	conn4 = psycopg2.connect(host=compcfg1['host'], port=compcfg1['port'], user=compcfg1['user'], database=compcfg1['database'], password=compcfg1['password'], async=1)
	conn5 = psycopg2.connect(host=compcfg2['host'], port=compcfg2['port'], user=compcfg2['user'], database=compcfg2['database'], password=compcfg2['password'], async=1)
	conn6 = psycopg2.connect(host=compcfg2['host'], port=compcfg2['port'], user=compcfg2['user'], database=compcfg2['database'], password=compcfg2['password'], async=1)
	allconns = [conn1,conn2, conn3, conn4, conn5, conn6]

	wait_for_all_conns(allconns)
	cur1 = conn1.cursor()
	cur2 = conn2.cursor()
	cur3 = conn3.cursor()
	cur4 = conn4.cursor()
	cur5 = conn5.cursor()
	cur6 = conn6.cursor()
	cur1.execute("start transaction") # GT1
	cur2.execute("start transaction") # GT2
	cur3.execute("start transaction") # GT3
	cur4.execute("start transaction") # GT4
	cur5.execute("start transaction") # GT5
	cur6.execute("start transaction") # GT6
	wait_for_all_conns(allconns)

	cur1.execute("update t1 set b=b+1 where a = 1")
	cur2.execute("update t1 set b=b+1 where a = 11")
	cur3.execute("update t1 set b=b+1 where a = 12")
	
	cur4.execute("update t1 set b=b+1 where a = 3")
	cur5.execute("update t1 set b=b+1 where a = 5")
	cur6.execute("update t1 set b=b+1 where a = 15")
	wait_for_all_conns(allconns)
   
	# GT1->GT2, GT4
	cur1.execute("update t1 set b=b+1 where a = 11 or a=3;commit")
	# GT2->GT3, GT5
	cur2.execute("update t1 set b=b+1 where a = 12 or a=5;commit")
	# GT3->GT1, GT6
	cur3.execute("update t1 set b=b+1 where a = 1 or a=15;commit")

	print "wait 5 secs for gdd to resolve the dd"
	time.sleep(5)
	cur4.execute("commit")
	cur5.execute("commit")
	cur6.execute("commit")

	killed_conn = wait_for_all_conns([conn1, conn2, conn3])
	end_trans(killed_conn, killed_conn.cursor())
	wait_for_all_conns([conn4, conn5, conn6])

	print "Time taken: {} second(s)".format(time.time() - start_ts)

def form_2_nested_3_nodes_cycles(nodes, same_compnode) :
	compcfg1 = nodes[0]
	if same_compnode:
		compcfg2 = nodes[0]
	else:
		compcfg2 = nodes[1]
	start_ts = time.time()
	conn1 = psycopg2.connect(host=compcfg1['host'], port=compcfg1['port'], user=compcfg1['user'], database=compcfg1['database'], password=compcfg1['password'], async=1)
	conn2 = psycopg2.connect(host=compcfg2['host'], port=compcfg2['port'], user=compcfg2['user'], database=compcfg2['database'], password=compcfg2['password'], async=1)
	conn3 = psycopg2.connect(host=compcfg1['host'], port=compcfg1['port'], user=compcfg1['user'], database=compcfg1['database'], password=compcfg1['password'], async=1)
	conn4 = psycopg2.connect(host=compcfg1['host'], port=compcfg1['port'], user=compcfg1['user'], database=compcfg1['database'], password=compcfg1['password'], async=1)
	conn5 = psycopg2.connect(host=compcfg2['host'], port=compcfg2['port'], user=compcfg2['user'], database=compcfg2['database'], password=compcfg2['password'], async=1)
	conn6 = psycopg2.connect(host=compcfg2['host'], port=compcfg2['port'], user=compcfg2['user'], database=compcfg2['database'], password=compcfg2['password'], async=1)
	allconns = [conn1,conn2, conn3, conn4, conn5, conn6]

	wait_for_all_conns(allconns)
	cur1 = conn1.cursor()
	cur2 = conn2.cursor()
	cur3 = conn3.cursor()
	cur4 = conn4.cursor()
	cur5 = conn5.cursor()
	cur6 = conn6.cursor()
	cur1.execute("start transaction") # GT1
	cur2.execute("start transaction") # GT2
	cur3.execute("start transaction") # GT3
	cur4.execute("start transaction") # GT4
	cur5.execute("start transaction") # GT5
	cur6.execute("start transaction") # GT6
	wait_for_all_conns(allconns)

	cur1.execute("update t1 set b=b+1 where a = 1")
	cur2.execute("update t1 set b=b+1 where a = 11")
	cur3.execute("update t1 set b=b+1 where a = 12")
	
	cur4.execute("update t1 set b=b+1 where a = 3")
	cur5.execute("update t1 set b=b+1 where a = 5")
	cur6.execute("update t1 set b=b+1 where a = 15")
	wait_for_all_conns(allconns)
   
	# GT1->GT2, GT4
	cur1.execute("update t1 set b=b+1 where a = 11 or a=3;commit")
	# GT2->GT3, GT5
	cur2.execute("update t1 set b=b+1 where a = 12 or a=5;commit")
	# GT3->GT1, GT6
	cur3.execute("update t1 set b=b+1 where a = 1 or a=15;commit")

	cur4.execute("update t1 set b=b+1 where a = 5;commit") # GT4 -> GT5
	cur5.execute("update t1 set b=b+1 where a = 15;commit") # GT5->GT6
	cur6.execute("update t1 set b=b+1 where a = 3;commit") # GT6->GT1
	print "wait 5 secs for gdd to resolve the 2 dd cycles"
	time.sleep(5)
	killed_conn = wait_for_all_conns([conn4, conn5, conn6])
	end_trans(killed_conn, killed_conn.cursor())

	killed_conn = wait_for_all_conns([conn1, conn2, conn3])
	end_trans(killed_conn, killed_conn.cursor())

	print "Time taken: {} second(s)".format(time.time() - start_ts)

def form_4_nodes_cycle(nodes, same_compnode) :
	compcfg1 = nodes[0]
	if same_compnode:
		compcfg2 = nodes[0]
	else:
		compcfg2 = nodes[1]
	start_ts = time.time()
	conn1 = psycopg2.connect(host=compcfg1['host'], port=compcfg1['port'], user=compcfg1['user'], database=compcfg1['database'], password=compcfg1['password'], async=1)
	conn2 = psycopg2.connect(host=compcfg2['host'], port=compcfg2['port'], user=compcfg2['user'], database=compcfg2['database'], password=compcfg2['password'], async=1)
	conn3 = psycopg2.connect(host=compcfg1['host'], port=compcfg1['port'], user=compcfg1['user'], database=compcfg1['database'], password=compcfg1['password'], async=1)
	conn4 = psycopg2.connect(host=compcfg2['host'], port=compcfg2['port'], user=compcfg2['user'], database=compcfg2['database'], password=compcfg2['password'], async=1)
	allconns = [conn1,conn2, conn3, conn4]

	wait_for_all_conns(allconns)
	cur1 = conn1.cursor()
	cur2 = conn2.cursor()
	cur3 = conn3.cursor()
	cur4 = conn4.cursor()
	cur1.execute("start transaction") # GT1
	cur2.execute("start transaction") # GT2
	cur3.execute("start transaction") # GT3
	cur4.execute("start transaction") # GT4
	wait_for_all_conns(allconns)

	cur1.execute("update t1 set b=b+1 where a = 1")
	cur2.execute("update t1 set b=b+1 where a = 11")
	cur3.execute("update t1 set b=b+1 where a = 12")
	cur4.execute("update t1 set b=b+1 where a = 3")
	wait_for_all_conns(allconns)
   
	# GT1->GT2
	cur1.execute("update t1 set b=b+1 where a = 11;commit")
	# GT2->GT3
	cur2.execute("update t1 set b=b+1 where a = 12;commit")
	# GT3->GT4
	cur3.execute("update t1 set b=b+1 where a = 3;commit")
	# GT4->GT1
	cur4.execute("update t1 set b=b+1 where a = 1;commit")
	killed_conn = wait_for_all_conns(allconns)

	end_trans(killed_conn, killed_conn.cursor())

	print "Time taken: {} second(s)".format(time.time() - start_ts)

# prerequisite: create a cluster with 2 computing nodes and 2 shards. the shard could have only a master node(set it to super_readonly=false).
if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Test global deadlock detection using 2 computing nodes and 2 storage shards')
	parser.add_argument('comp_nodes', type=str, help="computing nodes config file path")
	parser.add_argument('--do_init', type=bool, help="whether to create&populate tables")

	args = parser.parse_args()

	node_jsconf = open(args.comp_nodes)
	node_jstr = node_jsconf.read()
	node_jscfg = json.loads(node_jstr)

	compcfg = node_jscfg[0]
	# test preparation: create a table and insert some rows for later use.
	#if (args.do_init):
	conn = psycopg2.connect(host=compcfg['host'], port=compcfg['port'], user=compcfg['user'], database='postgres', password=compcfg['password'])
	conn.autocommit = True
	cur = conn.cursor()
	cur.execute("drop table if exists t1")
	cur.execute("create table t1(a int primary key, b int) partition by range (a)")
	cur.execute("create table t11 partition of t1 for values from (0) to (10)")
	cur.execute("create table t12 partition of t1 for values from (10) to (20)")
	cur.execute("insert into t1 values(0,10),(1,11),(2,12),(3,13),(4,14),(5,15),(6,16),(7,17),(8,18),(9,19)")
	cur.execute("insert into t1 values(10,20),(11,31),(12,32),(13,33),(14,34),(15,35),(16,36),(17,37),(18,38),(19,39)")
	cur.close()
	conn.close()
	print "Wait 5 seconds for DDLs to be synced to all computing nodes."
	time.sleep(5)
	# TODO: enumerate all victim policies

	form_2_nodes_cycle(node_jscfg, True)
	form_2_nodes_cycle(node_jscfg, False)
	form_3_nodes_cycle(node_jscfg, False)
	form_3_nodes_cycle(node_jscfg, True)
	form_4_nodes_cycle(node_jscfg, True)
	form_4_nodes_cycle(node_jscfg, False)
	form_3_nodes_cycle_with_forks(node_jscfg, False)
	form_3_nodes_cycle_with_forks(node_jscfg, True)
	print "form_2_nested_3_nodes_cycles same comp nodes"
	form_2_nested_3_nodes_cycles(node_jscfg, True)
	print "form_2_nested_3_nodes_cycles different comp nodes"
	form_2_nested_3_nodes_cycles(node_jscfg, False)
	node_jsconf.close()
