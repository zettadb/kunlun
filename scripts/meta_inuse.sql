-- Copyright (c) 2019 ZettaDB inc. All rights reserved.
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

DROP TABLE IF EXISTS `commit_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `commit_log` (
  `comp_node_id` int(10) unsigned NOT NULL,
  `txn_id` bigint(20) unsigned NOT NULL,
  `next_txn_cmd` enum('commit','abort') NOT NULL,
  `prepare_ts` timestamp  default current_timestamp,
  PRIMARY KEY (`txn_id`,`comp_node_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY LIST (`comp_node_id`)
SUBPARTITION BY HASH (((`txn_id` >> 32) DIV 86400))
SUBPARTITIONS 32
(PARTITION pplaceholder VALUES IN (null) ENGINE = InnoDB) */;
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
  `id` int(10) unsigned NOT NULL,
  `name` varchar(64) NOT NULL,
  `hostaddr` varchar(8192) NOT NULL,
  `port` smallint(5) unsigned NOT NULL,
  `db_cluster_id` int(10) unsigned NOT NULL,
  `when_created` timestamp NULL DEFAULT (now()),
  `user_name` varchar(64) NOT NULL,
  `passwd` varchar(16) NOT NULL,
  `status` enum('creating','inactive','active') DEFAULT 'creating',
  PRIMARY KEY (db_cluster_id, `id`),
  UNIQUE KEY `cluster_id_name` (db_cluster_id, `name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `db_clusters`
--

DROP TABLE IF EXISTS `db_clusters`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `db_clusters` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
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
-- Table structure for table `ddl_ops_log_template_table`
--

DROP TABLE IF EXISTS `ddl_ops_log_template_table`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `ddl_ops_log_template_table` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `objname` varchar(64) NOT NULL,
  `db_name` varchar(64) NOT NULL,
  `schema_name` varchar(64) NOT NULL,
  `optype` enum('create','drop','rename','alter','replace','others') NOT NULL,
  `objtype` enum('db','index','matview','partition','schema','seq','table','func','role_or_group','proc','stats','user','view', 'others') NOT NULL,
  `when_logged` timestamp NULL DEFAULT (now()),
  `sql_src` text NOT NULL,
  `sql_storage_node` text NOT NULL,
  `target_shard_id` int(10) unsigned NOT NULL,
  `initiator` int(10) unsigned NOT NULL,
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
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `hostaddr` varchar(8192) NOT NULL,
  `port` smallint(5) unsigned NOT NULL,
  `user_name` varchar(64) NOT NULL,
  `passwd` varchar(120) NOT NULL,
  UNIQUE KEY `id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `shard_nodes`
--

DROP TABLE IF EXISTS `shard_nodes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `shard_nodes` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `ro_weight` smallint(6) DEFAULT '0',
  `hostaddr` varchar(8192) NOT NULL,
  `port` smallint(5) unsigned NOT NULL,
  `user_name` varchar(64) NOT NULL,
  `passwd` varchar(120) NOT NULL,
  `shard_id` int(10) unsigned NOT NULL,
  `db_cluster_id` int(10) unsigned NOT NULL,
  `svr_node_id` int(10) unsigned NOT NULL,
  `when_created` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `master_priority` smallint(6) NOT NULL,
  `status` enum('creating','inactive','active') DEFAULT 'creating',
  PRIMARY KEY (`id`),
  UNIQUE KEY `hostaddr_port_svrnodeid_uniq` (`hostaddr`(256),`port`,`svr_node_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `shards`
--

DROP TABLE IF EXISTS `shards`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `shards` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(64) NOT NULL,
  `when_created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `num_nodes` smallint(5) unsigned NOT NULL,
  `space_volumn` bigint(20) unsigned NOT NULL DEFAULT '0',
  `num_tablets` int(10) unsigned NOT NULL DEFAULT '0',
  `db_cluster_id` int(10) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (db_cluster_id, `name`)
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
CREATE PROCEDURE `append_ddl_log_entry`(tblname varchar(256), dbname varchar(64), schema_name varchar(64), objname varchar(64), obj_type varchar(16), op_type varchar(16), cur_opid bigint unsigned, sql_src text, sql_src_sn text, target_shardid int unsigned, initiator_id int unsigned, OUT my_opid bigint unsigned)
    MODIFIES SQL DATA
    SQL SECURITY INVOKER
BEGIN
    DECLARE conflicts INT DEFAULT 0;

    set @dbname =dbname;
    set @schema_name = schema_name;
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
        SET @sql1 = CONCAT('select exists(select id from ', tblname, ' where (db_name=? or (objname=? and objtype=\'db\')) and initiator != ? and id > ? for update) into @conflicts');
    else
        SET @sql1 = CONCAT('select exists(select id from ', tblname, ' where (db_name=? or objtype=\'db\') and initiator != ? and id > ? for update) into @conflicts');
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
        SET @sql2 = CONCAT('INSERT INTO ', tblname, '(db_name, schema_name, objname, objtype, optype, sql_src, sql_storage_node, target_shard_id, initiator)values(?, ?, ?, ?, ?, ?, ?, ?, ?)');
        PREPARE stmt2 FROM @sql2;
        EXECUTE stmt2 USING @dbname, @schema_name, @objname, @obj_type, @op_type, @sql_src, @sql_src_sn, @target_shardid, @initiator_id;
        set my_opid = LAST_INSERT_ID();
        DEALLOCATE PREPARE stmt1;
        DEALLOCATE PREPARE stmt2;
    END IF; 
END ;
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

-- Dump completed on 2020-01-04 11:14:45
