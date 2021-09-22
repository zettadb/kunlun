/*-------------------------------------------------------------------------
 *
 * pg_cluster_meta.h
 *	  definition of the "cluster_meta (db cluster_meta)" system catalog (pg_cluster_meta)
 *
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * src/include/catalog/pg_cluster_meta.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_CLUSTER_META_H
#define PG_CLUSTER_META_H

#include "catalog/genbki.h"
#include "catalog/pg_cluster_meta_d.h"

#define KUNLUN_METADATA_DBNAME "Kunlun_Metadata_DB"

/*
 * This meta table always has one row only.
 * */
CATALOG(pg_cluster_meta,12349,ClusterMetaRelationId) BKI_SHARED_RELATION BKI_WITHOUT_OIDS
{
  Oid comp_node_id; /* ID and name of the computing node, used to identify this node in the cluster.*/
  Oid cluster_id; /* the ID and name of the cluster this computing node belongs to. */
  Oid cluster_master_id;/* the cluster's master node's server_id, references pg_cluster_meta_nodes.server_id */
  int ha_mode; /* 0: no_rep; 1: mgr; 2: rbr */
  NameData cluster_name;
  NameData comp_node_name;
} FormData_pg_cluster_meta;

typedef FormData_pg_cluster_meta*Form_pg_cluster_meta;
#endif /* !PG_CLUSTER_META_H */
