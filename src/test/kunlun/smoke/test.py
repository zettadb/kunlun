#! /usr/bin/python
import psycopg2

def test():
	conn = psycopg2.connect(host='127.0.0.1', port=5404, user='abc', password='abc', database='postgres')
	conn.autocommit = True
	cur = conn.cursor()
	sqls=["drop table if exists t1111",
		"create table t1111(id int primary key, info text, wt int)",
	    "insert into t1111(id,info,wt) values(1, 'record1', 1)",
	    "insert into t1111(id,info,wt) values(2, 'record2', 2)",
	    "update t1111 set wt = 12 where id = 1", "select * from t1111",
	    "delete from t1111 where id = 1", "select * from t1111",
		"prepare q1(int) as select*from t1111 where id=$1","begin","execute q1(1)","execute q1(2)",
		"prepare q2(text,int, int) as update t1111 set info=$1 , wt=$2 where id=$3", "execute q2('Rec1',2,1)", "commit", "execute q2('Rec2',3,2)",
	    "drop table t1111"]
	for sql in sqls:
	    res = cur.execute(sql+";")
	    print "command:%s, res:%s" % (sql, str(res))

if __name__ == '__main__':
    test()

