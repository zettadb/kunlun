-- Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.
-- This source code is licensed under Apache 2.0 License,
-- combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

-- MySQL dump 10.13  Distrib 8.0.15-6, for Linux (x86_64)
--
-- Host: localhost    Database: Kunlun_Metadata_DB
-- ------------------------------------------------------
-- Server version	8.0.15-6-debug

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
 SET NAMES utf8mb4 ;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
/*!50717 SELECT COUNT(*) INTO @rocksdb_has_p_s_session_variables FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'performance_schema' AND TABLE_NAME = 'session_variables' */;
/*!50717 SET @rocksdb_get_is_supported = IF (@rocksdb_has_p_s_session_variables, 'SELECT COUNT(*) INTO @rocksdb_is_supported FROM performance_schema.session_variables WHERE VARIABLE_NAME=\'rocksdb_bulk_load\'', 'SELECT 0') */;
/*!50717 PREPARE s FROM @rocksdb_get_is_supported */;
/*!50717 EXECUTE s */;
/*!50717 DEALLOCATE PREPARE s */;
/*!50717 SET @rocksdb_enable_bulk_load = IF (@rocksdb_is_supported, 'SET SESSION rocksdb_bulk_load = 1', 'SET @rocksdb_dummy_bulk_load = 0') */;
/*!50717 PREPARE s FROM @rocksdb_enable_bulk_load */;
/*!50717 EXECUTE s */;
/*!50717 DEALLOCATE PREPARE s */;
create database Kunlun_Metadata_DB;
use Kunlun_Metadata_DB;
--
-- Table structure for table `commit_log`
--

DROP TABLE IF EXISTS `commit_log_template_table`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `commit_log_template_table` (
  `comp_node_id` int unsigned NOT NULL, -- no FK for perf
  `txn_id` bigint unsigned NOT NULL,
  `next_txn_cmd` enum('commit','abort') NOT NULL,
  `prepare_ts` timestamp(6) default current_timestamp(6),
  PRIMARY KEY (`txn_id`,`comp_node_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY LIST (`comp_node_id`)
SUBPARTITION BY HASH (((`txn_id` >> 32) DIV 86400))
SUBPARTITIONS 32
(PARTITION pplaceholder VALUES IN (null) ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `data_centers`
--

DROP TABLE IF EXISTS `data_centers`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `data_centers` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(128) NOT NULL,
  `dc_type` varchar(32) ,
  `owner` varchar(64),
  `province` varchar(128) ,
  `city` varchar(128) ,
  `district` varchar(128) ,
  `location` text ,
  `memo` text,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `db_clusters`
--

DROP TABLE IF EXISTS `db_clusters`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `db_clusters` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(120) NOT NULL,
  `owner` varchar(120) NOT NULL,
  `ddl_log_tblname` varchar(120) NOT NULL,
  `when_created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `business` varchar(120) NOT NULL,
  `memo` text,
  `ha_mode` enum('no_rep','mgr','rbr') not null,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `comp_nodes`
--

DROP TABLE IF EXISTS `comp_nodes_id_seq`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `comp_nodes_id_seq` (
  `id` int PRIMARY KEY AUTO_INCREMENT
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

DROP TABLE IF EXISTS `comp_nodes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `comp_nodes` (
  `id` int unsigned NOT NULL,
  `name` varchar(64) NOT NULL,
  `hostaddr` varchar(8192) NOT NULL,
  `port` smallint unsigned NOT NULL,
  `db_cluster_id` int unsigned NOT NULL,
  `when_created` timestamp NULL DEFAULT (now()),
  `user_name` varchar(64) NOT NULL,
  `passwd` varchar(16) NOT NULL,
  `status` enum('creating','inactive','active') DEFAULT 'creating',
  `svr_node_id` int unsigned NOT NULL,

  -- resource limits, 0 means unlimited
  cpu_cores smallint unsigned NOT NULL default 0,
  max_mem_MB int unsigned NOT NULL default 0,
  max_conns int unsigned NOT NULL DEFAULT 0,

  extra_props text,

  PRIMARY KEY (db_cluster_id, `id`),
  UNIQUE KEY `cluster_id_name` (db_cluster_id, `name`),
  FOREIGN KEY (db_cluster_id) references db_clusters(id),
  FOREIGN KEY (svr_node_id) references server_nodes(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `ddl_ops_log_template_table`
--

DROP TABLE IF EXISTS `ddl_ops_log_template_table`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `ddl_ops_log_template_table` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `objname` varchar(64) NOT NULL,
  `db_name` varchar(64) NOT NULL,
  `schema_name` varchar(64) NOT NULL,
  `user_name` varchar(64) NOT NULL,
  `role_name` varchar(64) NOT NULL,
  `optype` enum('create','drop','rename','alter','replace','others') NOT NULL,
  `objtype` enum('db','index','matview','partition','schema','seq','table','func','role_or_group','proc','stats','user','view', 'others') NOT NULL,
  `when_logged` timestamp(6) NULL DEFAULT current_timestamp(6),
  `sql_src` text NOT NULL,
  `sql_storage_node` text NOT NULL,
  `target_shard_id` int unsigned NOT NULL, -- no FK for perf, references shards.id
  `initiator` int unsigned NOT NULL, -- no FK for perf, references comp_nodes.id
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `meta_db_nodes`
--

DROP TABLE IF EXISTS `meta_db_nodes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `meta_db_nodes` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `hostaddr` varchar(8192) NOT NULL,
  `port` smallint unsigned NOT NULL,
  `user_name` varchar(64) NOT NULL,
  `passwd` varchar(120) NOT NULL,
  UNIQUE KEY `id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `server_nodes`
--
DROP TABLE IF EXISTS `server_nodes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `server_nodes` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `hostaddr` varchar(8192) character set latin1 NOT NULL,
  `dc_id` int unsigned,
  `rack_id` varchar(256) character set latin1 DEFAULT NULL,

  -- In all below 4 paths fields, there can be multiple paths, which are
  -- seperated by some char such as a colon(;), these path strings are
  -- only parsed by app code not SQL. Each path is often on one unique storage
  -- device, in order to achieve high parallel IO.

  -- full paths to computing node data dir
  `comp_datadir` varchar(8192) character set latin1, 
  -- full paths to store data directories of all computing nodes
  `datadir` varchar(8192) character set latin1,
  -- full paths to store binlog dirs of all storage nodes
  `logdir` varchar(8192) character set latin1,
  -- full paths to store wal log dirs of all storage nodes
  `wal_log_dir` varchar(8192) character set latin1,
  -- there may be multiple storage devices, detailed stats in server_nodes_stats
  -- `total_storage` bigint unsigned NOT NULL, -- in MBs.
  `total_mem` int unsigned NOT NULL, -- in MBs
  `total_cpu_cores` int unsigned NOT NULL,
  -- `network_bandwidth` int unsigned NOT NULL, -- in MBps this is often consistent in a DC
  -- when the server started to be in use
  `svc_since` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  -- the port number the node_mgr on this server is listening on, if not using default one. NULL: using default app defined port.
  nodemgr_port int,
  extra_props text,
  PRIMARY KEY (`id`),
  UNIQUE KEY `hostaddr_dcid_uniq` (`hostaddr`(512),`dc_id`),
  FOREIGN KEY (dc_id) references data_centers(id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;
-- Insert a pseudo server row in order to create cluster without registering valid server nodes.
insert into server_nodes(hostaddr, total_mem, total_cpu_cores) values('pseudo_server_useless',16*1024,8);


CREATE TABLE `server_nodes_stats` (
  `id` int unsigned NOT NULL PRIMARY KEY, -- server node id
   comp_datadir_used int not null default 0, -- in MBs
   comp_datadir_avail int not null, -- in MBs. available space for all storage devices of computing node data dirs
   datadir_used int not null default 0, -- in MBs
   datadir_avail int not null, -- in MBs
   wal_log_dir_used int not null default 0, -- in MBs
   wal_log_dir_avail int not null, -- in MBs
   log_dir_used int not null default 0, -- in MBs
   log_dir_avail int not null, -- in MBs
   avg_network_usage_pct int not null default 0,
  FOREIGN KEY (id)  references server_nodes(id)
)  ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- connectivity statistics between server nodes
CREATE TABLE `server_nodes_conn_stats` (
  `id1` int unsigned NOT NULL , -- server node id
  `id2` int unsigned NOT NULL , -- server node id
  rtt_us int unsigned NOT NULL , -- round trip time in microsecs
  time_diff_us bigint unsigned NOT NULL , -- local time difference in microsecs
  -- app guarantees no pair stored twice.
  PRIMARY KEY(id1, id2)
)  ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Table structure for table `shards`
--

DROP TABLE IF EXISTS `shards`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `shards` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(64) NOT NULL,
  `when_created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `num_nodes` smallint unsigned NOT NULL,
  `space_volumn` bigint unsigned NOT NULL DEFAULT '0',
  `num_tablets` int unsigned NOT NULL DEFAULT '0',
  `db_cluster_id` int unsigned NOT NULL,
  -- how many slave ACKs to wait for
  `sync_num` smallint unsigned NOT NULL default 1,
  -- extra properties
  extra_props text,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (db_cluster_id, `name`),
  FOREIGN KEY (db_cluster_id)  references db_clusters(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `shard_nodes`
--

DROP TABLE IF EXISTS `shard_nodes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `shard_nodes` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `ro_weight` smallint DEFAULT '0',
  `hostaddr` varchar(8192) NOT NULL,
  `port` smallint unsigned NOT NULL,
  `user_name` varchar(64) NOT NULL,
  `passwd` varchar(120) NOT NULL,
  `shard_id` int unsigned NOT NULL,
  `db_cluster_id` int unsigned NOT NULL,
  `svr_node_id` int unsigned NOT NULL,
  `when_created` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `master_priority` smallint NOT NULL default 1,
  `status` enum('creating','inactive','active') DEFAULT 'creating',

  -- resource limits, 0 means unlimited
  cpu_cores smallint unsigned NOT NULL default 0,
  initial_storage_GB int unsigned NOT NULL default 0,
  max_storage_GB int unsigned NOT NULL default 0,
  innodb_buffer_pool_MB int unsigned NOT NULL default 0,
  rocksdb_buffer_pool_MB int unsigned NOT NULL default 0,

  -- channel properties, such as channel name, etc.
  extra_props text,
  PRIMARY KEY (`id`),
  UNIQUE KEY `hostaddr_port_svrnodeid_uniq` (`hostaddr`(512),`port`,`svr_node_id`),
  FOREIGN KEY (db_cluster_id) references db_clusters(id),
  FOREIGN KEY (svr_node_id) references server_nodes(id),
  FOREIGN KEY (shard_id) references shards(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping routines for database 'Kunlun_Metadata_DB'
--
/*!50003 DROP PROCEDURE IF EXISTS `append_ddl_log_entry` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
-- DELIMITER ;;
CREATE PROCEDURE `append_ddl_log_entry`(tblname varchar(256), dbname varchar(64), schema_name varchar(64), role_name varchar(64), user_name varchar(64), objname varchar(64), obj_type varchar(16), op_type varchar(16), cur_opid bigint unsigned, sql_src text, sql_src_sn text, target_shardid int unsigned, initiator_id int unsigned, OUT my_opid bigint unsigned)
    MODIFIES SQL DATA
    SQL SECURITY INVOKER
BEGIN
    DECLARE conflicts INT DEFAULT 0;

    set @dbname = dbname;
    set @schema_name = schema_name;
	set @role_name = role_name;
	set @user_name = user_name;
    set @objname = objname;
    set @obj_type = obj_type;
    set @cur_opid = cur_opid;
    set @op_type = op_type;
    set @sql_src = sql_src;
    set @sql_src_sn = sql_src_sn;
    set @target_shardid = target_shardid;
    set @initiator_id = initiator_id;
    SET @sql1 = '';

    if @obj_type != 'db' then
        SET @sql1 = CONCAT('select exists(select id from ', tblname, ' where (db_name=? or (objname=? and objtype=\'db\') or objtype=\'user\') and initiator != ? and id > ? for update) into @conflicts');
    else
        SET @sql1 = CONCAT('select exists(select id from ', tblname, ' where (db_name=? or objtype=\'db\' or objtype=\'user\') and initiator != ? and id > ? for update) into @conflicts');
    end if;
    
    PREPARE stmt1 FROM @sql1;

    if @obj_type != 'db' then
        EXECUTE stmt1 USING @dbname, @dbname, @initiator_id, @cur_opid;
    else
        EXECUTE stmt1 USING @dbname, @initiator_id, @cur_opid;
    end if;
    
    IF  @conflicts = 1 THEN
        DEALLOCATE PREPARE stmt1;
        set my_opid = 0;
    ELSE
        SET @sql2 = CONCAT('INSERT INTO ', tblname, '(db_name, schema_name, role_name, user_name, objname, objtype, optype, sql_src, sql_storage_node, target_shard_id, initiator)values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
        PREPARE stmt2 FROM @sql2;
        EXECUTE stmt2 USING @dbname, @schema_name, @role_name, @user_name, @objname, @obj_type, @op_type, @sql_src, @sql_src_sn, @target_shardid, @initiator_id;
        set my_opid = LAST_INSERT_ID();
        DEALLOCATE PREPARE stmt1;
        DEALLOCATE PREPARE stmt2;
    END IF; 
END ;
-- ;;
-- delimiter ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50112 SET @disable_bulk_load = IF (@is_rocksdb_supported, 'SET SESSION rocksdb_bulk_load = @old_rocksdb_bulk_load', 'SET @dummy_rocksdb_bulk_load = 0') */;
/*!50112 PREPARE s FROM @disable_bulk_load */;
/*!50112 EXECUTE s */;
/*!50112 DEALLOCATE PREPARE s */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

create table backup_storage(
	id int unsigned primary key auto_increment,
	name varchar(256),
	stype enum('HDFS', 'S3', 'EBS','CEPH', 'OTHER'), -- type of storage service
	-- connection info
	conn_str varchar(8192),
	host_addr varchar(8192),
	port int,
	user_name varchar(128),
	passwd varchar(32),
	extra text
) ENGINE=InnoDB DEFAULT charset=utf8;
 
-- available successful cluster backups, which can be used to restore clusters
-- rows in this table comes from cluster_shard_backup_restore_log when a backup succeeds.
create table cluster_backups (
	id serial primary key,
	-- which backup media is this backup stored into ?
	storage_id int unsigned not null,
	-- which kunlun cluster is this from?
	cluster_id int unsigned not null,
	-- type of backup: all storage shards, only metadata shard, metadata shard and all storage shards
	backup_type enum('storage_shards', 'meta_shard', 'meta_and_storage_shards') not null,
	-- does the backup contain a computing node's full logical dump?
	has_comp_node_dump bool not null,
	-- start&end timestamps, comes from cluster_shard_backup_restore_log.
	start_ts timestamp(6) not null,
	end_ts timestamp(6) not null,
	name varchar(256), -- a human readable string to identify the backup
	-- extra info for expanding 
	memo text default null,
	backup_addr varchar(1024), -- the root path and/or directory of the backup files.
    FOREIGN KEY (storage_id) references backup_storage(id),
 	FOREIGN KEY (cluster_id) references db_clusters(id)
) ENGINE=InnoDB DEFAULT charset=utf8;

-- if a cluster backup or restore fails half way, try to avoid repeated shard node backups/restores which have
-- already succeeded. so record successful shard backups.
create table cluster_shard_backup_restore_log (
	id serial primary key,
	-- which backup media is this backup stored into ?
	storage_id int unsigned not null,
	-- which kunlun cluster is this from?
	cluster_id int unsigned not null,
	shard_id int unsigned not null,
	shard_node_id int unsigned not null,
	optype enum('backup', 'restore') not null,
	-- by default NULL and app define place under cluster_backups.backup_addr
	shard_backup_path varchar(8192) character set latin1,
	status enum('not_started', 'ongoing', 'done', 'failed') not null default 'not_started',
	-- extra info for expanding 
	memo text default null,
	when_started timestamp(6) not null default current_timestamp(6), -- when the operation was issued
	when_ended timestamp(6), -- when the operation ended(either done or failed)

    FOREIGN KEY (storage_id) references backup_storage(id),
 	FOREIGN KEY (cluster_id) references db_clusters(id), 
 	FOREIGN KEY (shard_id)  references shards(id),
 	FOREIGN KEY (shard_node_id) references shard_nodes(id)
) ENGINE=InnoDB DEFAULT charset=utf8;

-- general jobs including the create and drop of a cluster, a computing node, a shard and a shard node.
-- log them in order to recover from failures halfway.
create table cluster_general_job_log (
	id serial primary key,
	job_type enum('cluster_create', 'cluster_drop', 'comp_node_create', 'comp_node_drop', 'shard_create', 'shard_drop', 'shard_node_create', 'shard_node_drop') not null,
	-- the computing node id, shard_id or shard_node_id being
	-- operated on, or NULL of working on a cluster
	instance_id int unsigned,
	-- the cluster to work on
	cluster_id int unsigned not null,
	-- an operation's status goes through the 3 phases: not_started -> ongoing -> done/failed
	status enum ('not_started', 'ongoing', 'done', 'failed') not null default 'not_started',
	-- extra info for expanding 
	memo text default null,
	when_started timestamp(6) not null default current_timestamp(6), -- when the operation was issued
	when_ended timestamp(6), -- when the operation ended(either done or failed)
	job_name varchar(256), -- optional, for human checks
 	FOREIGN KEY (cluster_id) references db_clusters(id)
) ENGINE=InnoDB DEFAULT charset=utf8;

-- table move logs, used to recover from broken procedures of a table-move operation.
create table table_move_jobs (
	id serial primary key,
	table_name varchar(64) not null, -- target table to move
	-- db name (of storage shard) which contains the target table
	db_name varchar(150) not null,
	src_shard int unsigned not null, -- old shard id
	-- data source, dumping the table in this node
	src_shard_node int unsigned not null,
	dest_shard int unsigned not null, -- new shard id
	-- the shard node to move into, must be dest_shard's current master
	dest_shard_node int unsigned not null,
	-- where to replay binlogs from, replication-starting-point(file)
	snapshot_binlog_file_idx int unsigned not null,
	-- replication-starting-point(offset)
	snapshot_binlog_file_offset bigint unsigned not null,

	-- file format of the table being moved:
	-- logical: a logical dump produced by tools like mydumper, etc;
	-- physical: produced by EXPORT cmd;
	-- dyn_clone: produced by clone cmd(currently only available in innodb)
	tab_file_format enum('logical', 'physical', 'dyn_clone') not null,
	when_started timestamp(6) not null default current_timestamp(6),
	when_ended timestamp(6),
	status enum('not_started', 'dumped', 'transmitted', 'loaded', 'caught_up', 'renamed', 'rerouted', 'done', 'failed') not null default 'not_started',
	-- extra info for expanding 
	memo text default null,
 	FOREIGN KEY (src_shard)  references shards(id),
 	FOREIGN KEY (src_shard_node) references shard_nodes(id),
 	FOREIGN KEY (dest_shard)  references shards(id),
 	FOREIGN KEY (dest_shard_node) references shard_nodes(id)
) ENGINE=InnoDB DEFAULT charset=utf8;

-- Dump completed on 2020-01-04 11:14:45
