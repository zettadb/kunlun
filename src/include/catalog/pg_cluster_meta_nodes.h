/*-------------------------------------------------------------------------
 *
 * pg_cluster_meta_nodes.h
 *	  definition of the "cluster_meta_nodes" system catalog (pg_cluster_meta_nodes)
 *
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * src/include/catalog/pg_cluster_meta_nodes.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_CLUSTER_META_NODES_H
#define PG_CLUSTER_META_NODES_H

#include "catalog/genbki.h"
#include "catalog/pg_cluster_meta_nodes_d.h"


/*
 * This meta table contains connect information about the meta data cluster nodes.
 * */
CATALOG(pg_cluster_meta_nodes,12350,ClusterMetaNodesRelationId) BKI_SHARED_RELATION BKI_WITHOUT_OIDS
{
  Oid server_id; /* mysql server_id variable value. used as PK to identify the row */
  Oid cluster_id;/* owner cluster's id, it's the same for all rows of one such table. */
  /*
   * Whether this node is master. There can be only one master in this table.
   * FormData_pg_cluster_meta::cluster_master_id is the master's server_id.
   * */
  bool is_master;
  int32 port;
  NameData user_name;
#ifdef CATALOG_VARLEN
  text hostaddr; /* socket host address, ipv6 or ipv4, or domain name. */
  text passwd BKI_FORCE_NOT_NULL;
#endif
} FormData_pg_cluster_meta_nodes;

typedef FormData_pg_cluster_meta_nodes*Form_pg_cluster_meta_nodes;
#endif /* !PG_CLUSTER_META_NODES_H */
