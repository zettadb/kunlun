See ReleaseNotes.txt for the list of features released in each version of Kunlun distributed DBMS.

To build computing node program from source, use build.sh directly or refer to it for instructions.
To build kunlun-storage from source, see kunlun-storage/INSTALL.Kunlun for instructions.
To build cluster_mgr from source, see cluster_mgr/README for instructions.

Refer to INSTALL.Kunlun to install Kunlun distributed DBMS cluster.

=====================================


#KunLun Distributed database cluster

KunLun distributed database cluster(Kunlun DDC) is a distributed relational database management system developed to manage massive amount (tera-bytes up to peta-bytes) of relational data and serve massive concurrent data read and/or write access workloads with low latency and high throughput. It provides robust transaction ACID guarantees, high scalability, high availability and transparent data partitioning and elastic horizontal scale-out capabilities, and standard SQL query support over distributed or partitioned data. All of these features altogether are known as NewSQL capabilities, i.e. Kunlun DDC is a NewSQL OLTP distributed RDBMS.

Users and applications could connect to Kunlun DDC using JDBC/ODBC, client libraries of PostgreSQL and MySQL in future, and client libraries for scriting languages like php/python/go/ruby/.net, and interact with Kunlun DDC exactly the same way they would do with a standalone MySQL or PostgreSQL database instance using standard SQL using standard SQL, and get all the above NewSQL benefits without any work or effort on the client side. Especially, applications can utilize OR mapping tools like hibernate and Mybatis to access relational data so as to avoid manually writing SQL statements in application code.

By using Kunlun DDC, users can quickly develop robust, highly available and highly scalable information systems that are capable of processing hundreds of terabytes of data or more with no engineering effort to implement the NewSQL features, all the challenges are conquered by Kunlun DDC, which greatly reduces the cost and difficulty and timespan required to develop such system and improves the overall quality (availability, robustness, stability, scalability, and performance) of such systems.


See ReleaseNotes.txt for the detailed list of features released in each version.

##Architecture

A KunLun distributed database cluster consists of two types of components: one or more computing nodes, one or more storage shards. And it also shares with other Kunlun DDCs a cluster_manager program and a meta-data cluster.

This piece of software is Kunlun's computing node. Users are supposed to use the kunlun-storage software which is a deeply engineered branch of percona-mysql-8.0 to setup their storage shards and metadata shards, because Kunlun needs some supporting features which only exist in kunlun-storage, and also kunlun-storage has fixes of all community MySQL-8.0 XA transaction crash safety bugs and pitfalls. And also, kunlun-storage has more than 50% performance improvement in terms of XA transaction processing compared to the same version of MySQL-8.0.x.

A Computing node accepts and validates client connections using PostgreSQL client protocol(MySQL protocol will be supported in future), and execute SQL statements from connected client connections by interacting with the cluster's storage shards. Users can add more computing nodes as their workloads grow, each and every computing node can serve user read/write requests. A Kunlun DDC's computing nodes locally has all the meta-data of all database objects(tables, views, materialized views, sequences, stored procs/functions, users/roles and priviledges etc), but they don't store user data locally. Instead, computing nodes store it in storage shards.

To execute a client SQL query, a computing node parses the client SQL query, optimizes it and at execution, it forms one or more SQL queries to send to the target storage shards which contain portions of target data it needs for the client SQL query. And if the query is a SELECT or an INSERT/DELETE/UPDATE...RETURNING statement instead of a bare INSERT/DELETE/UPDATE statement, the computing node gets partial results from all target storage shards, and assembles them into final result to reply to the client. 

User data is stored in one or more storage shards, not in computing nodes. Each storage shard stores a subset of all user data in the KunLun cluster, data in different storage shards don't overlap(i.e. share nothing). Users can extend or shrink the NO. of shards as their data volumns and workloads grow or shrink. A storage shard is a MySQL binlog replication cluster, which currently uses standard MySQL binlog replication(MGR) to achieve high availability. The primary node of each shard receives from computing nodes write and/or read SQL queries to insert/update/delete user data, or return target user data. And the MySQL node executes such SQL queries and return results to the requesting computing node. 

A meta-data shard is also a kunlun-storage cluster. It stores the meta-data of a Kunlun cluster. Multiple KunLun DDC clusters can share the same metadata cluster.
A cluster manager program runs as daemon process to maintain correct running status for one or more Kunlun DDCs, it takes little computing resources during its work.

##Advantages

Kunlun is currently developped based on PostgreSQL-11.5. In order to support some advanced features such as automatic DDL synchronization, distributed transactions processing, etc, we modified PostgreSQL code extensively rather than simply using its FDW. We modified PostgreSQL in a modular and least intrusive way so that we can easily keep upgrading with official upstream PostgreSQL releases.

Kunlun distributed database cluster is built for high scalability, high availability, ACID guarantees of distributed transactions, and full-fledged distributed query processing and elastic horizontal scalability.

###Highly Scalable
Kunlun DDC is highly scalable. It not only scales up but also scales out: users can add more computing nodes to have more query processing power, every computing node can serve both write and read workloads; And users(DBAs) can add more storage shards for more data storage and transaction processing capability and Kunlun DDC will automatically move parts of data to the new shards to balance workloads.

###Highly Available(HA)
Kunlun DDC is highly available, partial node failures won't harm the availability of the cluster. For any single storage shard or meta-data cluster of 2*N+1 MySQL nodes, the shard/cluster can resist N simultaneous node failures and remain writable; and it can remain readable as long as one kunlun-storage node is still working;

And for computing nodes, as long as there is one computing node working, a kunlun cluster can keep serving clients. The crash/stall/hang of one computing node doesn't affect any other parts of Kunlun DDC. And a computing node doesn't need replicas for HA because a computing node's entire state can be rebuilt using the metadata cluster. DBAs can add an empty computing node at any time to a Kunlun DDC and the new empty computing node will automatically upgrade itself to latest local state by connecting to metadata cluster and replay the accumulated DDL logs. When executing concurrent DDLs, computing nodes are well coordinated so that every computing node execute exactly the same sequence of DDL operations so that their local states are identical always.

###Distributed Transaction&Query Processing
Kunlun distributed query processing aims to relieve users from having to write SQL queries according to their data's distribution, i.e. it partitions user data transparently. This is achieved partly via its distributed transaction processing features, and partly via its distributed query processing features. With the help of these features, users can simply write SQL queries as if they were using a traditional standalone PostgreSQL/MySQL database, they don't have to know or consider in which storage shards certain portions of data are stored in order to write a working SQL query and transaction.

Kunlun distributed database cluster automatically does distributed transaction processing using the robust and well validated two phase commit(2PC) protocol, and as long as storage shards is fully "XA resillient", a distributed transaction has ACID guarantees. However currently no official releases of MySQL community server is fully "XA resillient", they all have a list of unsupported XA features. [This doc](https://dev.mysql.com/doc/refman/8.0/en/xa-restrictions.html)  and [this one](https://dev.mysql.com/doc/refman/5.7/en/xa-restrictions.html) has the full list of unsupported features that make official MySQL not XA resillient.
When you use official MySQL(including Percona-mysql), if one or more primary nodes and/or replica nodes go down or MySQL binlog replication is broken/stopped, it's possible that some distributed transactions lose partial committed changes or becomes inconsistent, or MySQL binlog replication fails to resume working.
In Kunlun we provide kunlun-storage, which is an enhanced MySQL branch which proved to be fully XA resillient, and all these 'XA resillience' issues are well solved.

Kunlun's distributed query processing is made as an integral part of PostgreSQL's query parse/optimization/execution process. Optimization of remote queries takes into account the network transfer cost of partial data from storage shards to the initiating computing node, and the extra cost inside computing node to process such partial results from storage shards. And we try to minimize the overall cost of remote query processing in our query optimizations.

As of this latest version, Kunlun DDC can handle cross shard table joins and aggregates queries, and regular insert/delete/update/select statements, and it supports prepared statements, sequences, and all regular DDLs. More query optimization work is still going on and will be released soon.

With transparent SQL compatibility, application developers can utilize standard SQL workflows and tool chains to streamline their workflow for premium efficiency and productivity. For example they can use OR mapping tools like hibernate or MyBatis to avoid writing SQL statements by hand in their application development, which would be impossible if they were using sharding middleware or doing sharding in application code or using some other sharding solution which isn't totally SQL compatible. Our aim is to keep Kunlun as SQL compatible as the PostgreSQL version we base on, except the features that we explicitly reject to support, such as triggers, foreign keys, etc.

#### SQL features NOT supported in Kunlun
create table ... select from
select into ... from ...
foreign keys
triggers
multi-table update/delete statements, i.e. updating/deleting rows of multiple tables in one statement, and updating/deleting rows of one table by identifying target rows by joining other tables.

All other standard SQL syntax will be supported.

#### Standard SQL Data types supported
   
All standard SQL data types supported by PostgreSQL, and some PostgreSQL extended data types, are supported, as detailed below.

##### All integer types and numeric types: bool, smallint, int, bigint, numeric(P,S), money, float/real, double;

##### All text types: char(N), varchar(N), text, blob, user defined enum types, and bit(N)/varbit(N))

##### Most date and/or time types, including date, time, timetz, timestamp, timestamptz. interval is not yet supported.

##### Some PostgreSQL private types are supported: Name, Oid, CID, TID, XID, LSN, macaddr, macaddr8, cidr, uuid.


#### PostgreSQL specific SQL features that Kunlun won't support

##### Cursors statements
DECLARE, FETCH, MOVE stmts, and the use of cursors in UPDATE/DELETE stmts, will never be supported, the result of using them is undefined.

##### unsupported table options

ON COMMIT clause in 'CREATE TABLE' is not supported.
create table using table inheritance is NOT supported. Using both of these stmts is behavior undefined.

##### Tablespaces
CREATE/ALTER TABLESPACE, and tablespace settings used anywhere including CREATE/ALTER database/table/index.

##### Indexing settings
Exclude, include, COLLATE, and partial indexing. Specifying them produces an error, they'll be never supported as restricted by mysql.

##### Storage settings for db/table/index 

These include 'with oids' table option, the tablespace settings and table storage parameters for dbs, tables and indexes.
Since user tables are not 'stored' in computing nodes, no such storage related settings are supported.
Specifying 'WITH OIDS' setting and any storage parameter of tables other than the newly added 'shard' parameter, produces an error.
Storage parameters for indexes and attributes are simply ignored; 
All tablespace settings are ignored.

#### PostgreSQL specific data types
##### Some PostgreSQL's private extensions are NOT and will never supported, including arrays, ranges, vectors, composite types, row types, table inheritance, table/relation types, etc.

##### Domains and user defined types that derive from basic numeric or text types are not supported now but they may be supported in future; All other user defined domains/types except enum types will not be supported.

##### json and spatial types will be supported in future.

###Automatic DDL synchronization

Kunlun DDC supports automatic DDL synchronization, that is, any DDL statement executed in one computing node automatically takes effect on all computing nodes of the same cluster. And also the DDLs required to execute in relevant storage shards are automatically executed as part of the DDL statement execution. And such operations are made crash safe, so that if during the execution of such a DDL any computing node(s) or storage shard node(s) terminates/exits for any reason, the entire system data and metadata is consistent and integral. So this feature can greatly relieve DBA and devops engineers from repetitive and error prone routines and greatly improve their productivity.

For example, suppose we have connection conn1 connected to computing node CN1, and connection conn2 connected to computing node CN2. Now we create a table tx via conn1, during the DDL execution, the table is properly defined in CN1's catalog, and tx's storage table is automatically properly created in the selected storage shard, and after the "create table" statement completes, immediately(in configurable period of time) in CN2 and all other computing nodes we can use/access the table tx.

We name this feature 'automatic DDL synchronization'. All DDLs that involve storage shards are well supported, including create/drop/alter table/index/partition/sequence/view/materialized view/database/schema statements; And all commonly used DDLs in PostgreSQL are supported by kunlun DDC.

## Cautions
Although Kunlun DDC is under active development, it's still not suitable for production use, it's ready for POC now. You are encouraged to try it out and report any requirements or issues to us.

Do not modify anything (table, stored procedure, etc) in Kunlun_Metadata_DB database of the meta-data shard manually, otherwise Kunlun DDC may not work correctly and you may lose your data. At the same time, do not manually modify any metadata tables(i.e. whose names start with pg_ ) in computing nodes, such as pg_shard, pg_shard_node, etc, otherwise Kunlun DDC may not work correctly and you may lose your data. You can only modify system metadata using SQL commands and/or scripts provided in Kunlun DDC.

##Contact

Although we already have many useful features, some very useful features are still being actively developed, and you are welcome to give us feedbacks, bug reports and feature requests in this github page. Also please visit www.zettadb.com for more information about Kunlun distributed DBMS.

 
