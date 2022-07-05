# KunlunBase(昆仑分布式数据库)简介

昆仑分布式数据库集群（下文简称KunlunBase）是一个分布式关系数据库管理系统，面向TB和PB级别海量数据处理，帮助用户高吞吐量和低延时处理海量数据高并发读写请求。它提供健壮的事务ACID保障，高效易用的分布式查询处理，高可扩展性，高可用性和透明的数据分片存储和访问，业务层和终端用户无感知的水平扩展能力，以及在分布式的数据分片中实现SQL兼容性，是典型的 NewSQL分布式数据库系统，支持OLTP和OLAP负载。应用软件开发者按照使用单节点PostgreSQL 和MySQL 关系数据库相同的方法使用KunlunBase，就可以得到所有上述NewSQL数据库的优点，完全不需要考虑数据的分区方式等存储细节。这样，应用开发者就可以非常快速地开发健壮可靠的，高可用和高可扩展的信息系统，来处理TB乃至PB级海量数据。所有的海量数据管理的挑战和困难都由KunlunBase来解决，从而大大降低了开发分布式应用系统的时间成本和资金成本和技术难度，并且全面提升其产品质量，大大加快应用开发和变更过程中的上线进度。


用户应用系统可以使用多种方式连接到 KunlunBase，包括 JDBC and ODBC, 社区版的PostgreSQL 和 MySQL提供的C/C++ 客户端连接库，以及包括php/python/go/rust/ruby/c#/ASP/.net 在内的所有主流编程语言的 PostgreSQL 和 MySQL 客户端连接库。

KunlunBase 具备良好的SQL 兼容性, 可以优秀的性能通过 TPC-C, TPC-H and TPC-DS 测试，以及 PostgreSQL 和 MySQL的SQL兼容性测试.
因此，原本使用MySQL 或 PostgreSQL数据库的应用系统可以完全不需要任何代码修改或者重新构建，就可以与KunlunBase 集群协作和交互从而利用到KunlunBase强大的NewSQL能力。用户可以使用标准SQL语法，以及MySQL 或 PostgreSQL对DML SQL语法的扩展。这样，所有SQL生态圈工具都可以与KunlunBase协作，例如用户可以使用Hibernat和 Mybatis等对象关系映射（ORM）组件与KunlunBase交互，这样应用软件开发者就不需要写SQL语句来读写数据了，大大提升应用开发效率。

通过使用KunlunBase，应用软件架构师和开发者可以快速设计和开发健壮、高可靠、高可用、高可扩展的信息系统，来处理TB级数据，所有这些工作都不需要为数据的庞大规模做任何特殊的设计和开发，所有的技术挑战都由KunlunBase解决，这就大大降低了中大型应用系统开发的成本、难度和时间，并且提升应用系统的食量和可靠性。



## 架构

一个KunlunBase集群有3类组件构成：一个或者多个kunlun-server实例，一个或者多个storage shard（存储集群）以及一个metadata shard（元数据集群）。

本软件是KunlunBase的kunlun-server组件，基于PostgreSQL-11.5开发。为了实现自动的DDL同步及复制以及分布式事务，分布式查询处理等高级功能，我们大量地修改了PostgreSQL源代码，而不是直接使用其FDW接口。我们的代码保持了很好的模块化，方便将来可以继续跟随PostgreSQL版本更新。
kunlun-server 支持PostgreSQL连接协议和MySQL连接协议，每个kunlun-server实例同时监听一个PostgreSQL端口和一个MySQL端口来接收和验证用户的连接请求。连接请求验证通过后，就接收和处理连接上发来的查询并返回结果给客户端。用户可以根据工作负载来增减kunlun-server实例，每个kunlun-server实例彼此平等和独立，没有依赖关系，都可以处理用户连接和读写请求。kunlun-server含有每个数据表以及其他数据库对象的元数据，但是用户数据存储在存储shard中。执行一个SQL时，kunlun-server解析该语句，然后对它做分布式查询优化，然后通过与后端存储shard做交互来完成分布式查询执行。交互的方法就是根据SQL语句的需要和数据在后端shard的分布信息，为相关的后端存储shard生成SQL语句。如果执行的SQL语句是 SELECT 或者 INSERT/DELETE/UPDATE...RETURNING 而不是简单的 INSERT/DELETE/UPDATE, 那么kunlun-server会并发地发送语句然后接收结果，最后通过执行分布式查询计划来合并处理所有后端存储shard返回的结果，形成最终的查询结果，返回给客户端。

每个存储shard 存储着一部分用户表或者表分区，每个shard的数据子集没有交集；每个存储shard是一个MySQL binlog复制集群，通过标准的MySQL Group Replication（MGR） single master 模式或者基于KunlunBase企业版专有的fullsync （强同步）复制机制来实现高可用性。
kunlun-storage是KunlunBase的存储节点，它是我们基于percona-server-8.0深度优化开发的MySQL分支。用户必须使用kunlun-storage软件组建KunlunBase的存储集群和元数据集群，因为KunlunBase集群需要的关键功能只存在于kunlun-storage中，并且它还包含了社区版MySQL-8.0 XA事务处理的所有容灾缺陷的修复；最后，kunlun-storage 包含性能优化，在性能方面领先社区mysql。

一个shard的主节点接受来自kunlun-server的读写请求，执行请求并返回结果给kunlun-server；启用了备机读功能时，shard的备节点可以接收和处理来自kunlun-server的只读请求。用户可以根据数据量的增加和减少来增加或者减少存储shard，KunlunBase会自动把数据均匀分散到所有shard上面，从而达到自动和透明的高可扩展性。

元数据集群也是一个kunlun-storage实例组成的MGR集群，存储着一个KunlunBase集群的元数据。多个数据库集群可以共用同一个元数据集群。

最后，KunlunBase还有一个cluster_mgr实例组成的集群，它负责维护正确的集群和节点状态，控制备份恢复，扩容，集群管理（例如增删kunlun-server节点和kunlun-storage节点）等工作。

## 特点和优势

KunlunBase的主要设计目标是高可扩展性，高可用性和完备的容灾能力，高性能，分布式事务ACID保障，和透明易用的分布式查询处理。 

### 高可扩展性

KunlunBase高可扩展，不仅垂直可扩展（scale up），而且水平可扩展（scale out）：用户可以通过增加kunlun-server来提升查询处理性能，每个kunlun-server都可以服务读写请求；用户也可以增加更多的存储shard来存储更多的数据并获得更大的查询处理和事务处理能力。KunlunBase会自动把数据平滑地用户无感知的方式分散到新增的存储集群中，确保所有集群承担近乎平均的存储负载。

DBA还可以为OLAP负载专门部署一组kunlun-server实例，并且使用读写分离功能从存储集群的备机节点读取用户数据，这样，就可以使用完全不同的节点来处理OLTP负载和OLAP负载，避免OLAP负载影响OLTP负载的性能。

### 高可用性
KunlunBase集群具备高可用性，部分节点宕机不会导致KunlunBase集群不可用。对于一个拥有2N+1 个kunlun-storage节点的存储shard或者元数据集群来说，这个shard可以抵抗N个节点同时宕机并持续提供服务；任何kunlun-server 实例宕机并不会导致KunlunBase整体不可用，只要系统还有kunlun-server实例在运行，那么KunlunBase整体仍可以处理用户请求。一个kunlun-server实例宕机不会影响同一个KunlunBase集群内其他kunlun-server实例的正常工作。宕机的kunlun-server实例当时正在处理的事务会自动回滚，用户连接会断开，用户数据和系统元数据始终保持一致。

### 分布式事务处理
KunlunBase的分布式查询处理的目标是让用户在写查询语句时不需要考虑他的数据是如何在存储集群分布的。这部分得益于分布式事务处理功能，部分得益于分布式查询处理功能。有了这两大类功能，用户就可以像使用单机MySQL或者PostgreSQL那样写SQL查询语句，不需要知道其SQL语句所读写的任何部分的数据是如何分布在哪几个存储shard 这类问题，也就是说应用开发者完全不需要直接使用或者知道‘分布式事务处理’或者‘分布式查询处理’这样的概念，所有这些强大的功能都是在后台默默为用户服务的，给用户带来简单的使用体验。

KunlunBase基于久经考验的两阶段提交协议（2PC）来自动做分布式事务处理，事务提交时候根据需要做两阶段提交或者一阶段提交。只要存储shard是 ‘XA resilient’ 的，那么分布式事务就有ACID保障。 不过，目前MySQL官方发布的社区版本没有 ‘XA resilient’ 的，它们都有一些无法良好支持的XA事务处理特性。 [这个文档](https://dev.MySQL.com/doc/refman/8.0/en/xa-restrictions.html) 和 [这个文档](https://dev.MySQL.com/doc/refman/5.7/en/xa-restrictions.html)含有所有官方MySQL版本不支持的XA事务处理功能。 当用户使用官方社区版本时，如果一个或者多个主/备节点宕机或者binlog复制发生中断，或者主备连接断开，那么有可能一些分布式事务会部分丢失已提交的改动或者在多个存储shard的事务分支的提交状态不同（部分提交，部分回滚），或者binlog 复制无法恢复工作等问题。
所以用户必须使用kunlun-storage组建存储集群，它填补了所有官方MySQL版本的XA事务处理的空白，是完全 ‘XA resilient’ 的。

### 分布式查询处理


KunlunBase的SQL兼容性与PostgreSQL相同，支持聚集查询，多表连接，自查询，CTE，以及grouping sets，cube, roll up, window function等OLAP功能。除了我们主动去除的DDL功能（比如外键和触发器）之外。KunlunBase可以优秀的性能通过 TPC-C, TPC-H and TPC-DS 测试，以及 PostgreSQL 和 MySQL的SQL兼容性测试，以及我们额外增加的大量SQL功能测试.
KunlunBase的分布式查询处理被设计和实现成为PostgreSQL的查询处理流程的有机组成部分。查询优化会考虑到数据在存储shard和kunlun-server之间的网络传输代价，以及在kunlun-server内部的合并处理来自存储shard的子结果的额外的开销。在此基础之上，试图最小化整体的查询处理开销。

借助KunlunBase优秀的SQL兼容能力，应用程序开发者可以使用标准SQL工作流和工具链来提升其工作效率。例如用户可以使用Hibernat和 Mybatis等对象关系映射（ORM）组件与KunlunBase交互，这样应用软件开发者就不需要写SQL语句来读写数据了，大大提升应用开发效率。这对于分库分表中间件或者应用层分表来说，是完全做不到的。

### 自动DDL
KunlunBase集群支持自动DDL，也即是说，在一个kunlun-server执行一个DDL时候，它会自动在集群所有kunlun-server生效并且相关的存储集群上面需要为此DDL执行的DDL语句也会作为整体DDL语句的一部分被自动执行。并且这样的自动DDL是可以容灾的，DDL执行期间集群任何kunlun-server或者存储节点因为任何原因退出或者宕机，集群的用户数据和元数据的一致性和完整性可以得到保障。可以看出，这个功能可以大大滴把DBA和运维人员从重复和易错的繁重的运维劳动中解放出来，大大提升他们的工作效率。

例如，我们有两个连接conn1和conn2,分别连接着同一个集群的两个kunlun-serverCN1和CN2,现在我们在conn1中执行create table语句创建表tx，该语句执行期间，tx的会在CN1的catalog中被定义，tx在存储shard上面的表会在该DDL语句执行期间被妥善地在自动选定（会有多种策略）的shard上面创建好。该DDL语句执行成功后，立刻（可配置的若干秒）在CN2以及本集群所有kunlun-server上面我们就可以使用表tx，对它做增删改查等操作. 我把这个功能叫做‘自动DDL’。
所有涉及到存储shard的DDL语句目前都已经支持自动DDL，包括create/drop/alter table/partition/index/database/schema/sequence/materialized view。对于其他的DDL，用户在任何一个kunlun-server执行该语句，本集群所有kunlun-server上面都会生效。

## 注意事项
禁止手工修改元数据集群中的数据，以及计算节点中的元数据，而是执行 DDL 语句完成需要的功能。否则可能导致丢失用户数据等严重问题。

## 其他

访问我们的官网www.kunlunbase.com获取各类资源和文档，查看kunlunbase的bug报告和功能开发任务，以及下载KunlunBase软件程序。

## 联络和资源

KunlunBase讨论群咨询和讨论（QQ群，号码：336525623）或者访问bugs.zettadb.com 报告bug以及查看每个bug的详细信息，每一个报告了bug的朋友我们都会在contributors文件中列出以表达感谢。

KunlunBase已经开源，欢迎访问 https://github.com/zettadb 或者 https://gitee.com/zettadb/ 获取KunlunBase的各模块的源代码并且为Kunlun数据库项目加星来给予我们鼓励。


