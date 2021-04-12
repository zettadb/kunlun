For more information about Kunlun distributed RDBMS, visit www.zettadb.com


#Release Notes Kunlun version 0.8.1

In this release we completed the following features:
##kunlun computing node
###1. Sequences
Made PostgreSQL native sequence feature work with Kunlun architecture. A sequence metadata is stored in storage shard meta tables(mysql.sequences), and no matter it's created in which computing nodes, all computing nodes will have the sequence locally and be able to use it efficiently.

All PostgreSQL sequence features are supported including explicit create and use of sequences as well as implicit measures such as serial type, the 'generated as identity' option in 'create table' statement. Also implemented sequence grammar of Oracle and autoincrement grammar of MySQL.

###2. All DDLs
Now we support all DDLs that we plan to support in Kunlun RDBMS, including "ALTER TABLE/INDEX/SEQUENCE statements", among others. Unsupported grammers will return error gracefully.
So now DDLs are complete in Kunlun.

###3. Prepared statement
This is crucial for security and query processing performance.

###4. Cross shard joins and subqueries
The functionality is working now, but it needs extra optimization work for premium performance in certain cases, will be done in the 0.8 milestone.

###5. Aggregates and OLAP
The functionality is working now, but it needs extra optimization work for premium performance in certain cases, will be done in the 0.8 milestone.

###6. insert/update/delete ... returning clause
This is very important if you need to get all fields of modified rows atomically.

###7. Containerization
All Kunlun modules are released in docker images now and containers will continue to be one form of release for Kunlun in future.



#Release Notes Kunlun version 0.7

In this release we completed a group of very important features involving crash safety, auto failover and more.

##Kunlun-storage
Kunlun-storage development, in which all known XA bugs in official MySQL-8.0 are fixed. And also we enhanced its performance so that in standard sysbench performance tests, QPS improved by at most %50 and latency drops by at most 50%. See [this report](https://zhuanlan.zhihu.com/p/151664455) for details.

All other supporting features required for collaboration with computing nodes of a Kunlun DDC.

##Cluster_manager 
Added cluster_manager program which maintains correct MGR status of storage shards and metadata shard of Kunlun DDCs that are registered in the same meta-data shard, so that all nodes of an MGR shard keeps running online and XA transaction branches left over by node crashes are correctly handled.

##Computing nodes
###Global deadlock detection and resolve
Global deadlocks that couldn't be handled by MySQL Innodb storage engine could now be correctly detected and resolved, so that Kunlun DDC could execute transactions in optimal performance.

###Adaption to MGR primary nodes auto-failover
When a storage shard/metadata shard's primary node changes for any reason, computing nodes can automatically discover the incident and adapt to it.

##Crash safety
Crash safety for computing nodes, storage shards and meta-data shard. In all now a Kunlun DDC could survive network partition, and/or node failures caused by any accidents.



#Release Notes Kunlun version 0.6

##I. Features
   
###1. Data types supported
   
All standard SQL data types supported by PostgreSQL, and some PostgreSQL extended data types, are supported as below.

1.1 All integer types and numeric types: bool, smallint, int, bigint, numeric(P,S), money, float/real, double;

1.2 All text types: char(N), varchar(N), text, blob, user defined enum types, and bit(N)/varbit(N))

1.3 Most date and/or time types, including date, time, timetz, timestamp, timestamptz. interval is not yet supported.

1.4 Some PostgreSQL private types are supported: Name, Oid, CID, TID, XID, LSN, macaddr, macaddr8, cidr, uuid.

1.5 Some PostgreSQL's private extensions are NOT supported, including arrays, ranges, vectors, composite types, row types, table inheritance, table/relation types, etc.

1.6 Domains and user defined types that derive from basic numeric or text types are not supported now but they may be supported in future; All other user defined domains/types except enum types will not be supported.


###2. Functions and operators supported

All standard SQL operators and functions, and most postgreSQL specific functions&operators are supported. Although MySQL doesn't support non-SQL-standard operators and functions of PostgreSQL, Kunlun can handle such queries correctly, some by converting to MySQL supported ones, some by computing locally inside computing nodes and only push down the operators and functions that MySQL can handle to storage shards.

###3. Table sharding, using PostgreSQL table partitioning feature.

Partitions of a table partition may be stored in one or more storage shards, decided by the computing nodes. They are read/written as necessary, also decided by computing nodes.

###4. Most common DDL and DML operations, as detailed below.
   
####4.1 DDLs

Create/drop table/index/database/schema are well supported.

These DDLs are well supported as being 'automatic' --- when a client sends such a DDL to one computing node execution, the database object(table, schema, database) will be automatically created on all computing nodes and target storage shards.

create table NAME (like table-template); is supported;
UNLOGGED tables are created as MyISAM tables in storage nodes.
A TEMPORARY table is stored in the computing node since it's only used throughout the session where it's created.

Unsupported clauses of these DDLs are listed below.

For all other DDLs, users have to execute the same DDL on each computing node instance for now, and such DDLs don't need storage shard actions. This may be made partly automatic in future, so that users need only to send the same such DDLs only once to Kunlun system and it will take effect on all computing nodes of the cluster.


####4.2 DMLs

Insert/delete/update/select stmts are well supported, with below exceptions.
    update/delete don't support multi-table statements or 'returning' clause yet, will be supported in future.
    insert/update don't support 'ON CONFLICT' clauses yet, will be supported in future.

####Some distributed query processing

This is on-going effort, the ultimate goal is to support all of them in highly efficient manner.

###5. Distributed transaction processing

This is transparently done inside Kunlun system, users don't need to do anything special, they simply issue transaction commands the same way as using PostgreSQL. In terms of transaction processing, users can simply take Kunlun as if they were using a PostgreSQL database.
With mysql community server as storage shard node, global deadlock detector can't work, so there can be global deadlocks which can't be resolved by innodb. One needs specialized version which will be released soon.

##II. PostgreSQL Features not supported yet

Some of these will be supported in the future, others will never be supported, as detailed below. For those that 'may be supported in future' or 'never supported', using it on current version causes undefined behavior and undefined result to your data and cluster, so never attempt on a production deployment. This is denoted as 'behavior undefined' below.

For local catalog tables, everything works just as in original PostgreSQL, all the stuff here is about user tables whose payload data is actually stored in storage shards.

In all we can roughly conclude that most of PostgreSQL's 'advanced' features and 'private extensions' are not supported now or never in future. We only expose and make use of the most commonly used features of PostgreSQL as Kunlun's functionality set.

###1. DDLs Not Supported Yet
   
1.1 Alter table stmts

Some of them will be supported in future. For now, behavior undefined.

1.2 create table ... select from

never supported, per restriction by MySQL. For now, behavior undefined.

1.3 foreign keys

never supported, for performance reasons. Specifying them produces an error.

1.4 'generated as identity' in 'create table' stmt

may be supported in future. For now, behavior undefined.

1.5 db/table/index storage settings

These include 'with oids' table option, the tablespace settings and table storage parameters for dbs, tables and indexes.
Since user tables are not 'stored' in computing nodes, no such storage related settings are supported.
Specifying 'WITH OIDS' setting and any storage parameter of tables other than the newly added 'shard' parameter, produces an error.
Storage parameters for indexes and attributes are simply ignored; 
All tablespace settings are ignored.


1.7. forbidden index settings/clauses in 'create index'

Exclude, include, COLLATE, and partial indexing. Specifying them produces an error, they'll be never supported as restricted by mysql.
Expressions as index key parts is not supported for now, may support it in future.
'concurrent' clause is ignored.

1.8 DEFAULT partition of a partitioned table

may be supported in future.

1.9 triggers, stored procedures

may be supported in future.

1.10 TABLESPACES

CREATE/ALTER TABLESPACE, and tablespace settings in anywhere including CREATE/ALTER database/table/index.(never supported)
Users can still create/alter/remove tablespaces, but these tablespaces will never be effectively used.

1.11 ALTER DATABASE

Rename database will never be supported, doing so produces an error.
Other features of 'alter database' may be supported in future.

1.12 Cursors

Cursors will never be supported, per the nature of table sharding.
So DECLARE, FETCH, MOVE stmts, and the use of cursors in UPDATE/DELETE stmts, will never be supported, the result of using them is undefined.

1.13 unsupported table options

ON COMMIT clause in 'CREATE TABLE' is not supported.
create table using table inheritance is NOT supported. Using both of these stmts is behavior undefined.


###2. DMLs Not Supported Yet
   
2.1 multi-table join

This is on-going effort, some already working. It will be much better improved in future versions.

2.2 insert into ... select from

may be supported in future

2.3 access to system columns

System columns including OID, TableOid, xmin, xmax, txnid, etc are not available, since user data is not stores in PostgreSQL heap relations.

2.4 update/delete stmts

We don't support multi-table statements or 'returning' clause yet. It will be supported in future.



