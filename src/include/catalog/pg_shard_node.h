/*-------------------------------------------------------------------------
 *
 * pg_shard_node.h
 *	  definition of the "shard nodes(db instances)" system
 *	  catalog (pg_shard_node)
 *
 *
 * Copyright (c) 2019 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * src/include/catalog/pg_shard_node.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_NODE_H
#define PG_SHARD_NODE_H

#include "catalog/genbki.h"
#include "catalog/pg_shard_node_d.h"


/*
 * A shard has one master node(i.e. db instance) and multiple slaves nodes,
 * which contain identical data, and each shard node is stored as a mysql db
 * instance on a computer server.
 * A slave node replicates binlogs from master node to keep data in sync.
 * A computing node mostly sent queries to master node to execute, but it
 * can also send read only queries to slave nodes to execute.
 * */
CATALOG(pg_shard_node,12345,ShardNodeRelationId) BKI_SHARED_RELATION BKI_WITHOUT_OIDS
{
  Oid id; /* shard node id. */
  int32 port;
  Oid shard_id; /* The owner shard, references pg_shard(oid). BKI can't specify
				   foreign key though. */
  Oid svr_node_id;
  /*
   * When dispatching read only select stmts to a slave, choose the one with
   * max ro_weight of a shard.
   */
  int16 ro_weight;
  NameData ip; /* ip address, ipv6 or ipv4. */
  NameData user_name;
#ifdef CATALOG_VARLEN
  text passwd BKI_FORCE_NOT_NULL;
  timestamptz when_created BKI_DEFAULT(0);
#endif
} FormData_pg_shard_node;

typedef FormData_pg_shard_node* Form_pg_shard_node;

#endif /* !PG_SHARD_NODE_H */
