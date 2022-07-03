# KunlunBase Introduction

## Directions

For more information and resources of KunlunBase, such as software, documentation, bug reports and features planned/under development, and release notes, please visit www.kunlunbase.com

See ReleaseNotes.md for the list of features released in each version of KunlunBase.

To build kunlun-server node program from source, use build.sh directly or refer to it for instructions.
To build kunlun-storage from source, see kunlun-storage/INSTALL.kunlun.md for instructions.
To build cluster_mgr from source, see cluster_mgr/README for instructions.

Refer to INSTALL.kunlun.md to install KunlunBase cluster.


## What is KunlunBase?

KunlunBase is a distributed relational database management system aimed to help users store and query massive amount (tera-bytes up to peta-bytes) of relational data and serve massive concurrent relational data access(read and/or write) workloads with low latency and high throughput. KunlunBase provides robust transaction ACID guarantees, high scalability, high availability, transparent data partitioning and elastic horizontal scale-out capabilities, and standard SQL query support over distributed and/or partitioned data. All of these features altogether are known as NewSQL capabilities, in one word, KunlunBase is a NewSQL OLTP distributed RDBMS with complete OLAP functionality.

Users and applications could connect to KunlunBase using JDBC and ODBC, and C/C++ client libraries provided in community PostgreSQL and MySQL distributions, as well as PostgreSQL and MySQL client libraries for most programming languages, such as php/python/go/rust/ruby/c#/ASP/.net etc.

KunlunBase is SQL compatible, it can correctly execute all test cases in TPC-C, TPC-H and TPC-DS with excellent performance, and passes all SQL compatibility test cases contained in PostgreSQL and MySQL.

Consequently, users and applications can interact with KunlunBase exactly the same way they would do with a community MySQL and/or PostgreSQL database instance, using either standard SQL or private DML extensions of MySQL and/or PostgreSQL, and get all the above NewSQL benefits without any work or effort on the client application side --- no need to modify application code or even rebuild it. Furthermore, applications can utilize object relational mapping(ORM) tools like Hibernate and Mybatis to access relational data with KunlunBase so as to avoid manually writing SQL statements in application code.

By using KunlunBase, software application architects and developers can quickly design and develop robust, highly available and highly scalable information systems that are capable of processing hundreds of terabytes of data with little extra engineering effort --- all the technical&engineering challenges are conquered in KunlunBase, which greatly reduces the cost and difficulty and timespan required to develop such powerful systems and improves the overall quality and reliability(availability, robustness, stability, scalability, and performance) of such systems.


## Architecture

A KunlunBase cluster consists of two types of components: one or more kunlun-server nodes, and one or more storage shards. And it also shares with other KunlunBase clusters a group of cluster_mgr instances and a metadata shard.



This piece of software is KunlunBase's kunlun-server component, it interacts with clients to for connection validation, access control, SQL query processing, etc.

Kunlun-server is currently developped based on PostgreSQL-11.5. In order to support some advanced features such as automatic DDL synchronization, distributed transactions processing, etc, we modified PostgreSQL code extensively rather than simply using its FDW. We modified PostgreSQL in a modular and least intrusive way so that we can easily keep upgrading with official upstream PostgreSQL releases.

A kunlun-server instance listens on a PostgreSQL port and a MySQL port configured during cluster installation. And it accepts and validates client connections requests connected from an application with either PostgreSQL or MySQL protocols. And when a connection is validatted and established, it communicates with the client using either PostgreSQL or MySQL protocols respectively.

A kunlun-server node receives SQL statements from connected client connections and execute them by interacting with the cluster's storage shards. Users can add more kunlun-server nodes any time as their workloads grow, each and every kunlun-server node can serve user read/write requests. A KunlunBase cluster's kunlun-server nodes locally has all the meta-data of all database objects(tables, views, materialized views, sequences, stored procs/functions, users/roles and priviledges etc), but they don't store user data locally. Instead, kunlun-server nodes store user data into storage shards.

To execute a client SQL query, a kunlun-server instance goes through standard PostgreSQL query processing steps --- it parses the client SQL query, optimizes it and as an extension for remote data(data stored in kunlun-storage shards), we developed extra plan nodes which form one or more SQL queries to send to the target storage shards containing portions of target data needed for the client SQL query. And if the query is a SELECT or an INSERT/DELETE/UPDATE...RETURNING statement instead of a bare INSERT/DELETE/UPDATE statement, the kunlun-server node gets partial results from all target storage shards, and assembles them into final result by executing the query plan, and reply the final result to the client 

User data is stored in one or more storage shards instead of kunlun-server nodes. Each storage shard stores a subset of all user data in the KunlunBase cluster, and data in different storage shards don't overlap(i.e. share nothing). Users can extend or shrink the NO. of shards as their data volumns and workloads grow or shrink. 

A storage shard is a MySQL binlog replication cluster, which currently uses either MySQL group replication or our proprietary fullsync replication to achieve high availability. 

In KunlunBase we require using our kunlun-storage software to deploy storage shards and metadata shard. Kunlun-storage is a deeply engineered branch of percona-mysql-8.0 with supporting features required by KunlunBase's components. Additionally, kunlun-storage contains fixes of all community MySQL-8.0 XA transaction crash safety bugs as well as kunlun-storage contains some performance improvement.

The primary node of each kunlun-storage shard receives from kunlun-server nodes DML SQL queries to insert/update/delete user data, or return target user data. And it executes such SQL queries and return results to the requesting kunlun-server node.

Also, KunlunBase supports read-write-split(RWS) --- executing read only queries in replica nodes to storage shards in order to decrease resource contention in primary nodes and utilize computing resources where the replica nodes are deployed. Consequently kunlun-server nodes could also send SELECT queries to replicas of any kunlun-storage shards under user configured conditions for data consistency and replication latency.

A KunlunBase cluster needs a meta-data shard, which is also a kunlun-storage cluster. It stores the meta-data of one or more KunlunBase clusters.

Finally we have a cluster of cluster_mgr instances which maintain correct running status for one or more KunlunBase clusters, and do extra work related to high availability, scale out, cluster data backup and restore, cluster management(e.g. kunlun-server or kunlun-storage instance installation), and so on.

## Advantages

KunlunBase distributed database cluster is built for high scalability, high availability, ACID guarantees of distributed transactions, and full-fledged distributed query processing and elastic horizontal scalability, as detailed below.

### Highly Scalable

KunlunBase clusters are highly scalable. It not only scales up but also scales out: users can add more kunlun-server nodes to have more query processing power, every kunlun-server node can serve both write and read workloads; And users(DBAs) can add more storage shards for more data storage and transaction processing capability and KunlunBase  will automatically move parts of user data to the new shards to balance workloads.

DBAs can also deploy more kunlun-server nodes for more query processing capability, especially for analytical OLAP workloads, so that instances running OLTP and OLAP workloads are totally seperated apart.

Also, KunlunBase supports read-write splits, so computer servers containing replica nodes of storage shards can be used to run read only queries. This is especially useful for OLAP workloads to avoid resource contention with OLTP workloads which are run on seperate kunlun-server nodes and primary nodes of storage shards.

### Highly Available(HA)

KunlunBase  clusters are  highly available, partial node failures won't harm the availability of the cluster. For any single storage shard or meta-data shard of 2*N+1 nodes, the shard can resist N simultaneous node failures and remain writable with no data loss; and it can remain readable as long as one kunlun-storage node is still running;

And for kunlun-server nodes, as long as there is one kunlun-server node working, a KunlunBase cluster can keep serving clients. The crash/stall/hang of one kunlun-server instance doesn't affect any other parts of a KunlunBase cluster. And a kunlun-server node doesn't need replicas for HA because its entire state can be rebuilt using the metadata shard.

DBAs can add an empty kunlun-server node at any time to a KunlunBase cluster and the new empty kunlun-server node will automatically synchronize itself to latest local state by communicate with the metadata cluster and replay the accumulated DDL logs. When executing concurrent DDLs, kunlun-server nodes are well coordinated so that every kunlun-server node execute exactly the same sequence of DDL operations and their local states are identical always.

### Distributed Transaction Processing

KunlunBase distributed query processing aims to relieve users from having to write SQL queries according to their data's distribution, i.e. it partitions user data transparently. This is achieved partly via its distributed transaction processing features, and partly via its distributed query processing features.

With the help of these features, users can simply write SQL queries as if they were using a traditional standalone PostgreSQL or MySQL database, and they don't have to know or consider in which storage shards certain portions of data are stored in order to write a working SQL query and transaction.

KunlunBase automatically does distributed transaction processing using the robust and well validated two phase commit(2PC) protocol, and as long as storage shards is fully "XA resillient", a distributed transaction has ACID guarantees. However currently no official releases of MySQL community server is fully "XA resillient", they all have a list of unsupported XA features. [This doc](https://dev.mysql.com/doc/refman/8.0/en/xa-restrictions.html)  and [this one](https://dev.mysql.com/doc/refman/5.7/en/xa-restrictions.html) has the full list of unsupported features that make official MySQL not XA resillient.

When you use official MySQL(including Percona-mysql), if one or more primary nodes and/or replica nodes go down or MySQL binlog replication is broken/stopped, it's possible that some distributed transactions lose partial committed changes or becomes inconsistent, or MySQL binlog replication fails to resume working.

In KunlunBase we provide kunlun-storage, which is an enhanced MySQL branch which proved to be fully XA resillient, and all these 'XA resillience' issues are well solved.

### Distributed Query Processing

As to SQL compatibility, our aim is to keep KunlunBase as SQL compatible as the PostgreSQL version kunlun-server is baded on(currently PostgreSQL-11.5), except the features that we explicitly removed, such as triggers, foreign keys, etc(detailed below).

KunlunBase can correctly execute test cases in TPC-C, TPC-H and TPC-DS with excellent performance, and passes all test cases contained in PostgreSQL-11.5 distribution, plus the extra huge amount of tests we added for our enhancement and extensions.

KunlunBase's distributed query processing is made as an integral part of PostgreSQL's query parse/optimization/execution process. Optimization of remote queries takes into account the network transfer cost of partial data from storage shards to the initiating kunlun-server node, and the extra cost inside kunlun-server node to process such partial results from storage shards. And we try to minimize the overall cost of remote query processing in our query optimizations.

KunlunBase can handle cross shard table joins and aggregates queries, and regular insert/delete/update/select statements, and it supports prepared statements, sequences, and all regular DDLs.

With transparent SQL compatibility, application developers can utilize standard SQL workflows and tool chains to streamline their workflow for premium efficiency and productivity. For example they can use ORM tools like hibernate or MyBatis to avoid writing SQL statements by hand in their application development, which would be impossible if they were using sharding middleware or doing sharding in application code or using some sharding middleware which support only simple SQL queries.

 
#### SQL features removed in KunlunBase

create table ... select from
select into ... from ...
foreign keys
triggers


All other standard SQL syntax will be supported.

#### SQL features to be supported later

1. multi-table update/delete statements, i.e. updating/deleting rows of multiple tables in one statement, and updating/deleting rows of one table by identifying target rows by joining other tables.

2. updating fields of partition columns

#### Standard SQL Data types supported
   
All standard SQL data types supported by PostgreSQL and MySQL, plus some PostgreSQL extended data types, and all private data types of MySQL, are supported, as detailed below.

##### All integer types and numeric types: bool, smallint, int, bigint, numeric(P,S), money, float/real, double;

##### All text types: char(N), varchar(N), text, blob, user defined enum types, and bit(N)/varbit(N))

##### Most date and/or time types, including date, time, timetz, timestamp, timestamptz. interval is not yet supported.

##### Some PostgreSQL private types are supported: Name, Oid, CID, TID, XID, LSN, macaddr, macaddr8, cidr, uuid.

##### All private data types of MySQL, e.g. {tiny|medium|long}{text|blob|int}

#### PostgreSQL specific SQL features that KunlunBase won't support

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
Since user tables are not 'stored' in kunlun-server nodes, no such storage related settings are supported.
Specifying 'WITH OIDS' setting and any storage parameter of tables other than the newly added 'shard' parameter, produces an error.
Storage parameters for indexes and attributes are simply ignored; 
All tablespace settings are ignored.

#### PostgreSQL specific data types
##### Some PostgreSQL's private extensions are NOT and will never supported, including arrays, ranges, vectors, composite types, row types, table inheritance, table/relation types, etc.

##### Domains and user defined types that derive from basic numeric or text types are not supported now but they may be supported in future; All other user defined domains/types except enum types will not be supported.

##### json and spatial types will be supported in future.

### Automatic DDL synchronization

KunlunBase supports automatic DDL synchronization, that is, any DDL statement executed in one kunlun-server node automatically takes effect on all kunlun-server nodes of the same cluster. And also the DDLs required to execute in relevant storage shards are automatically executed as part of the DDL statement execution. And such operations are made crash safe, so that if during the execution of such a DDL any kunlun-server node(s) or storage shard node(s) terminates/exits for any reason, the entire system data and metadata is consistent and integral. So this feature can greatly relieve DBA and devops engineers from repetitive and error prone routines and greatly improve their productivity.

For example, suppose we have connection conn1 connected to kunlun-server node CN1, and connection conn2 connected to kunlun-server node CN2. Now we create a table tx via conn1, during the DDL execution, the table is properly defined in CN1's catalog, and tx's storage table is automatically properly created in the selected (by kunlun-server or by user) storage shard, and after the "create table" statement completes, immediately(in configurable period of time) in CN2 and all other kunlun-server nodes we can use/access the table tx.

We name this feature 'automatic DDL synchronization'. All DDLs are well supported, such as create/drop/alter table/index/partition/sequence/view/materialized view/database/schema/user/role statements;

## Cautions

Do not modify anything (table, stored procedure, etc) in Kunlun_Metadata_DB database of the meta-data shard manually, otherwise KunlunBase may not work correctly and you may lose your data.

At the same time, do not manually modify any metadata tables(i.e. whose names start with pg_ , all in pg_catalog schema) in any kunlun-server nodes, such as pg_shard, pg_shard_node, etc, otherwise KunlunBase may not work correctly and you may lose your data. You can only modify system metadata using SQL commands and/or scripts provided in KunlunBase.

## Contact

You are welcome to give us feedbacks, bug reports and feature requests in this github page. Also please visit www.kunlunbase.com for more information about KunlunBase, visit downloads.kunlunbase.com to download docker images, built binaries and pdf docs about KunlunBase distributed database, and bugs.kunlunbase.com for the bugs and tasks we completed and plan to work on.

