/*-------------------------------------------------------------------------
 *
 * remote_meta.h
 *	  POSTGRES remote access method definitions.
 *
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * src/include/access/remote_meta.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef REMOTE_META_H
#define REMOTE_META_H

#include "postgres.h"
#include "access/tupdesc.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "utils/algos.h"
#include "postmaster/bgworker.h"
#include "catalog/pg_sequence.h"
/*
  Current use: when dropping a db, we need to stop the applier process attached
  to the db. So we need to inform the main applier the db to drop in order to
*/
typedef struct PGPROC PGPROC;
typedef struct SyncSlot {
	Oid dbid;
	PGPROC* proc;
}SyncSlot;

typedef struct RemoteMetaIPCSync {
	int nused_slots;
	int main_applier_pid;
	BackgroundWorkerHandle main_bgw_hdl;
	SyncSlot dbslots[MAX_DBS_ALLOWED];
}RemoteMetaIPCSync;
extern RemoteMetaIPCSync *g_remote_meta_sync;


typedef struct RemoteAlterSeq
{
	StringInfoData update_stmt, update_stmt_peer;
	Oid newtypid;
	bool do_restart;
	bool for_identity;
	int64 restart_val;
} RemoteAlterSeq;

extern bool replaying_ddl_log;
extern bool enable_remote_relations;
extern char *remote_stmt_ptr;

extern void make_remote_create_table_stmt1(Relation heaprel, const TupleDesc tupDesc);
extern void make_remote_create_table_stmt2(
	Relation indexrel, Relation heaprel, const TupleDesc tupDesc, bool is_primary,
	bool is_unique, int16*coloptions);

extern void RemoteDDLCxtStartStmt(int topstmt, const char *sql);
extern const char *show_remote_sql(void);
extern void SetRemoteContextShardId(Oid shardid);
extern Oid GetRemoteContextShardId(void);
extern void ResetRemoteDDLStmt(void);
extern void InitRemoteDDLContext(void);
extern void TrackRemoteDropTable(Oid relid, bool is_cascade);
extern void TrackRemoteDropTableStorage(Relation rel);
extern void TrackRemoteCreatePartitionedTable(Relation rel);
extern void TrackRemoteCreateIndex(Relation heaprel, const char *idxname, Oid amid,
	bool is_unique, bool is_partitioned);
extern void TrackRemoteDropIndex(Oid relid, bool is_cascade, bool drop_as_constr);
extern void TrackRemoteDropIndexStorage(Relation rel);
extern void end_metadata_txn(bool commit_it);
extern void set_dropping_tree(bool b);
extern void end_remote_ddl_stmt(void);
extern void RemoteDDLSetSkipStorageIndexing(bool b);
extern void RemoteCreateDatabase(const char *dbname);
extern void RemoteCreateSchema(const char *schemaName);
extern void RemoteDropDatabase(const char *db);
extern void RemoteDropSchema(const char *schema);
extern bool is_metadata_txn(uint64_t*p_opid);
extern Size MetaSyncShmemSize(void);
extern void CreateMetaSyncShmem(void);
extern void WaitForDBApplierExit(Oid dbid);
typedef int (*on_remote_meta_sync_func_t)(Oid dbid);
extern int handle_remote_meta_sync_reqs(on_remote_meta_sync_func_t func);
extern int wakeup_remote_meta_sync_waiters(Oid dbid);
extern bool IsCurrentProcMainApplier(void);
extern void RemoteCreateSeqStmt(Relation rel, Form_pg_sequence seqform, CreateSeqStmt *seq, List*owned_by, bool toplevel);
extern bool findSequenceByName(const char *seqname);
extern Size RemoteSeqFetchShmemSize(void);
extern void CreateRemoteSeqFetchShmem(void);
extern void fetchSeqValues(void);
extern void accumulate_simple_ddl_sql(const char *sql, int start, int len);
extern bool is_supported_simple_ddl_stmt(NodeTag stmt);
extern bool is_banned_ddl_stmt(NodeTag stmt);
extern bool enable_remote_ddl(void);
extern bool is_object_stored_in_shards(ObjectType objtype);
extern int CurrentCommand(void);
extern void TrackColumnRename(Relation rel, const char*oldname, const char*newname);
extern void TrackRelationRename(Relation rel, const char*objname, bool isrel);
extern void TrackDropColumn(Relation rel, const char *colName);
extern Oid find_root_base_type(Oid typid0);
extern void TrackAddColumn(Relation rel, ColumnDef *coldef, char typtype, Oid typid, int32 typmod, Oid collOid);
extern void TrackColumnNullability(Relation rel, const char *colName, Oid typid, bool nullable, int32 typmod, Oid collOid);
extern const char* atsubcmd(AlterTableCmd *subcmd);
extern bool is_supported_alter_table_subcmd(AlterTableCmd *subcmd);
extern void TrackAlterTableGeneral(Oid relid);
extern Oid find_root_base_type(Oid typid0);
extern void TrackAlterColType(Relation rel, const char *colname, bool notnull,
	Oid targettype, int32 typmod, Oid collOid);
extern void TrackRenameGeneral(ObjectType objtype);
extern void update_colnames_indices(Relation attrelation, Relation targetrelation,
	int attnum, const char *oldattname, const char *newattname);
extern void TrackAlterSeq(Relation rel, List *owned_by, RemoteAlterSeq*raseq, bool toplevel, bool setval);
extern void InvalidateCachedSeq(Oid seqrelid);
extern void init_remote_alter_seq(RemoteAlterSeq *raseq);
extern void AlterDependentTables(Oid enum_type_oid);
extern void set_curtxn_started_curcmd(int i);
extern void init_curtxn_started_curcmd(void);
#endif /* !REMOTE_META_H */
