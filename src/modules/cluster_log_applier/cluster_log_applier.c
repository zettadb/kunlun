/* -------------------------------------------------------------------------
 *
 * cluster_log_applier.c
 *      cluster log applier, fetch DDL logs and sync logs from meta-data
 *      server and apply them one after another. DDL logs have to be replayed
 *      by multiple bgworker processes, one for each database. State sync logs
 *      are replayed by a single process.
 *
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * IDENTIFICATION
 *		src/modules/cluster_log_applier/cluster_log_applier.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */
#include <unistd.h>

#include "access/xact.h"
#include "access/remote_xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "tcop/utility.h"
#include "postmaster/xidsender.h"
#include "sharding/cluster_meta.h"
#include "utils/memutils.h"
#include "access/remote_meta.h"
#include "commands/dbcommands.h"
#include "sharding/sharding_conn.h"
#include "sharding/sharding.h"
#include "access/remote_dml.h"
#include "storage/ipc.h"
#include "tcop/debug_injection.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(cluster_log_applier_launch);

/* GUC variables */
static int	cluster_log_applier_naptime = 10;

#ifdef ENABLE_DEBUG
#define N_CLA_SLOTS  2 // to debug dynamic memory space mgmt easier.
#else
#define N_CLA_SLOTS  16
#endif

static const short INVALID_SLOT_IDX = -1;



typedef struct ClusterLogApplierState ClusterLogApplierState;
typedef struct ClusterLogApplierGlobal ClusterLogApplierGlobal;

static BgwHandleStatus check_worker_exited(ClusterLogApplierState *clas);
static void check_all_workers_exited(void);
static int apply_cluster_ddl_log(uint64_t newpos, const char *sqlstr,
	DDL_OP_Types optype, DDL_ObjTypes objtype, const char *objname, bool*execed);
static void cluster_log_applier_sigusr2_hdlr(SIGNAL_ARGS);
static void cluster_log_applier_sigterm(SIGNAL_ARGS);
static void cluster_log_applier_sighup(SIGNAL_ARGS);
static void setup_background_worker(ClusterLogApplierState *clas, Oid dbid);
static void cleanup_background_workers(dsm_segment *seg, Datum arg);
static void startup_cluster_log_appliers(void);
static ClusterLogApplierState *find_slot_by_dbname(const char *dbname);
static ClusterLogApplierState *find_slot_by_dbid(Oid dbid);
static ClusterLogApplierState *alloc_slot_for_db(Oid dbid, const char*dbname);
static void free_clas_slot(ClusterLogApplierState *clas);
static void free_clas_slot_nolock(ClusterLogApplierState *clas);
static void init_cla_global(ClusterLogApplierGlobal *clag, dsm_segment *dsm_seg);
static ClusterLogApplierGlobal*alloc_clag_shmem();
static void notify_other_appliers_to_exit(bool wait);
static BgwHandleStatus notify_wait_bgworker_applier_exit(Oid dbid);
static bool create_bgworker_all_db(Oid dbid, const char *db, void *ws);
static void cluster_log_applier_init_common(ClusterLogApplierState *clas);
static bool create_bgworker_new_db(Oid dbid, const char *db, void*ws);
static Oid cluster_log_applier_init(void);
static void cluster_log_applier_init_others(Datum main_arg);
static int on_remote_meta_sync(Oid dbid);
static void reset_clas(ClusterLogApplierState *clas);
static void free_all_clas_slots();
static void terminate_this_bgw(bool is_main, bool got_sigterm);
extern void find_metadata_master_and_recover(void);

void		_PG_init(void);
void		cluster_log_applier_main(Datum) pg_attribute_noreturn();
void        cluster_log_applier_main_others(Datum main_arg) pg_attribute_noreturn();

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;
extern bool skip_top_level_check;
/*
 * Each database needs one applier process, each process has one such slot.
 * At initialization, all create applier processes for all existing databases,
 * and then during runtime, when a db is created, call StartApplierProcessForDB()
 * to work for this new db; when a db is dropped, call EndApplierProcessForDB().
 *
 * Accesses to prev_idx, next_idx, owner fields are synced with lwlock;
 * the keep_running, is_running fields need no sync; the rest are
 * supposed to be accessed only by the owner process(latest_ddl_opid_executed), or
 * static after set(dbid,dbname, owner, bgw_hdl);
 * */
typedef struct ClusterLogApplierState
{
	Oid dbid;
	short prev_idx, next_idx;// links in list; INVALID_SLOT_IDX if none.
	bool keep_running, is_running;
	BackgroundWorkerHandle bgw_hdl;
	/*
	  'owner' ptr is only valid in main applier, do not use it in other
	  applier processes.
	*/
	struct ClusterLogApplierGlobal *owner;
	uint64_t latest_sync_opid_executed; // start from id + 1.
	NameData dbname;
} ClusterLogApplierState;


typedef struct ClusterLogApplierGlobal
{
	ClusterLogApplierState slots[N_CLA_SLOTS];
	dsm_segment *dsm_seg;  // the dsm segment ptr containing this object, returned by dsm_create.
	short nslots_used;
	short first_slot; // 1st allocated slot. if INVALID_SLOT_IDX, it's empty
	short first_free_slot; // if INVALID_SLOT_IDX, no more free slots.
	/*
	 * Create more sections if one isn't enough.
	 * */
	struct ClusterLogApplierGlobal *next_section;
} ClusterLogApplierGlobal;

typedef struct ApplierState
{
	Oid dbid;
	uint64_t local_max_opid;
	/*
	 * this applier's shared state in shared memory.
	 * */
	ClusterLogApplierState *clas;
	dsm_segment *dsm_seg;// the dsm seg containing this applier's ClusterLogApplierState object.
	/*
	 * shared state object allocated in shared memory. only main applier tracks clag list,
	 all other appliers have NULL ptrs.
	 * */
	ClusterLogApplierGlobal *first_clag, *last_clag;
	NameData dbname;
} ApplierState;

static ApplierState g_applier_state;



static void init_cla_global(ClusterLogApplierGlobal *clag, dsm_segment *dsm_seg)
{
	memset(clag, 0, sizeof(*clag));
	clag->first_slot = INVALID_SLOT_IDX;
	if (!g_applier_state.first_clag)
		g_applier_state.first_clag = g_applier_state.last_clag = clag;
	// link free list
	for (short i = 0; i < N_CLA_SLOTS; i++)
	{
		clag->slots[i].next_idx = (i == N_CLA_SLOTS - 1 ? INVALID_SLOT_IDX : i+1);
		clag->slots[i].prev_idx = i - 1;
		clag->slots[i].owner = clag;
	}
	clag->first_free_slot = 0;
	clag->dsm_seg = dsm_seg;
}


static ClusterLogApplierGlobal*alloc_clag_shmem()
{
	dsm_segment *dsm_seg = dsm_create(sizeof(ClusterLogApplierGlobal), 0);
	if (g_applier_state.dsm_seg == NULL)
	{
		g_applier_state.dsm_seg = dsm_seg;
	}
	// keep the dsm segment while server running.
	//dsm_pin_segment(dsm_seg);
	// we need the dsm seg in whole session.
	dsm_pin_mapping(dsm_seg);

	ClusterLogApplierGlobal *clag = dsm_segment_address(dsm_seg);
	init_cla_global(clag, dsm_seg);
	return clag;
}

/*
 * Find a clag with free slots. If not found, create a new one.
 * */
static ClusterLogApplierGlobal *find_append_clag()
{
	for (ClusterLogApplierGlobal *clag = g_applier_state.first_clag;
		 clag; clag = clag->next_section)
	{
		if (clag->nslots_used < N_CLA_SLOTS)
			return clag;
	}

	// alloc clag in dynamic shared memory
	ClusterLogApplierGlobal*new_clag = alloc_clag_shmem();

	g_applier_state.last_clag->next_section = new_clag;
	g_applier_state.last_clag = new_clag;
	return new_clag;
}

static ClusterLogApplierState *alloc_slot_for_db(Oid dbid, const char*dbname)
{
	LWLockAcquire(MetadataLogAppliersLock, LW_EXCLUSIVE);

	ClusterLogApplierGlobal *avail_clag = g_applier_state.last_clag;
	if (g_applier_state.last_clag->nslots_used == N_CLA_SLOTS)
	{
		Assert(g_applier_state.last_clag->first_free_slot == INVALID_SLOT_IDX);
		avail_clag = find_append_clag();
	}

	// take off 1st free slot from free list
	ClusterLogApplierState *ret = avail_clag->slots + avail_clag->first_free_slot;
	Assert(ret->prev_idx == INVALID_SLOT_IDX);
	avail_clag->first_free_slot = ret->next_idx;
	if (avail_clag->first_free_slot != INVALID_SLOT_IDX) {
		avail_clag->slots[avail_clag->first_free_slot].prev_idx = INVALID_SLOT_IDX;
	}

	ret->prev_idx = INVALID_SLOT_IDX;
	ret->next_idx = INVALID_SLOT_IDX;

	// link the slot to inuse list head.
	short retidx = ret - avail_clag->slots;
	ret->next_idx = avail_clag->first_slot;
	if (avail_clag->first_slot != INVALID_SLOT_IDX)
	{
		Assert(avail_clag->nslots_used != 0);
		avail_clag->slots[avail_clag->first_slot].prev_idx = retidx;
	}
	else
	{
		Assert(avail_clag->nslots_used == 0);
	}

	avail_clag->first_slot = retidx;
	ret->prev_idx = INVALID_SLOT_IDX;
	avail_clag->nslots_used++;

	ret->dbid = dbid;
	strncpy(ret->dbname.data, dbname, sizeof(ret->dbname) - 1);
	ret->latest_sync_opid_executed = 0;
	ret->keep_running = true;
	ret->is_running = false;
	LWLockRelease(MetadataLogAppliersLock);
	return ret;
}

static void free_all_clas_slots()
{
	LWLockAcquire(MetadataLogAppliersLock, LW_EXCLUSIVE);
	for (ClusterLogApplierGlobal *clag = g_applier_state.first_clag; clag; clag= clag->next_section)
	{
		for (int i = 0; i < N_CLA_SLOTS; i++)
		{
			ClusterLogApplierState *clas = clag->slots + i;
			clas->next_idx = i+1;
			clas->prev_idx = i - 1;
			reset_clas(clas);
		}
		clag->nslots_used = 0;
		clag->first_free_slot = 0;
		clag->first_slot = INVALID_SLOT_IDX;
	}
	LWLockRelease(MetadataLogAppliersLock);
}

static void reset_clas(ClusterLogApplierState *clas)
{
	clas->dbid = 0;
	clas->keep_running = clas->is_running = false;
	memset(&clas->bgw_hdl, 0, sizeof(clas->bgw_hdl));
	clas->owner = NULL;
	clas->latest_sync_opid_executed = 0;
	clas->dbname.data[0] = '\0';
}

static void free_clas_slot(ClusterLogApplierState *clas)
{
	LWLockAcquire(MetadataLogAppliersLock, LW_EXCLUSIVE);
	free_clas_slot_nolock(clas);
	LWLockRelease(MetadataLogAppliersLock);
}

static void free_clas_slot_nolock(ClusterLogApplierState *clas)
{
	ClusterLogApplierGlobal *owner = clas->owner;
	if (!owner) return;
	/*
	  If not main applier, clas->owner can't be used because it is a dynamic
	  shmem ptr alloced in main applier memory space. The same clag's address
	  mapped into current process memory space is g_applier_state.first_clag,
	  see cluster_log_applier_init_others() for details.
	*/
	if (owner != g_applier_state.first_clag &&
		g_applier_state.first_clag == g_applier_state.last_clag)
		owner = g_applier_state.first_clag;

	short slot_idx = clas - owner->slots;
	short prev_use = owner->slots[slot_idx].prev_idx;
	short next_use = owner->slots[slot_idx].next_idx;

	// take off from inuse list
	if (prev_use == INVALID_SLOT_IDX)
	{
		Assert(slot_idx == owner->first_slot);
		owner->first_slot = next_use;
	}
	else
		owner->slots[prev_use].next_idx = next_use;

	if (INVALID_SLOT_IDX != next_use)
		owner->slots[next_use].prev_idx = prev_use;

	// link to freelist head.
	clas->prev_idx = INVALID_SLOT_IDX;
	clas->next_idx = owner->first_free_slot;
	if (owner->first_free_slot != INVALID_SLOT_IDX)
	{
		owner->slots[owner->first_free_slot].prev_idx = slot_idx;
	}
	owner->first_free_slot = slot_idx;
	owner->nslots_used--;
	Assert((owner->nslots_used == 0 && owner->first_slot == INVALID_SLOT_IDX) ||
		   (owner->nslots_used > 0 && owner->first_slot != INVALID_SLOT_IDX));
	clas->dbid = 0;
	clas->dbname.data[0] = '\0';
	clas->latest_sync_opid_executed = 0;
	clas->keep_running = false;
	clas->is_running = false;
}

static ClusterLogApplierState *find_slot_by_dbid(Oid dbid)
{
	ClusterLogApplierState *ret = NULL;

	LWLockAcquire(MetadataLogAppliersLock, LW_EXCLUSIVE);
	for (ClusterLogApplierGlobal *clag = g_applier_state.first_clag; clag; clag= clag->next_section)
	{
		for (int idx = clag->first_slot; idx >= 0;)
		{
			ClusterLogApplierState *clas = clag->slots + idx;
			if (clas->dbid == dbid)
			{
				ret = clas;
				goto end;
			}
			idx = clas->next_idx;
		}
	}
end:
	LWLockRelease(MetadataLogAppliersLock);
	return ret;
}


static ClusterLogApplierState *find_slot_by_dbname(const char *dbname)
{
	ClusterLogApplierState *ret = NULL;

	LWLockAcquire(MetadataLogAppliersLock, LW_EXCLUSIVE);
	for (ClusterLogApplierGlobal *clag = g_applier_state.first_clag; clag; clag= clag->next_section)
	{
		for (int idx = clag->first_slot; idx >= 0;)
		{
			ClusterLogApplierState *clas = clag->slots + idx;
			if (strncmp(clas->dbname.data, dbname, NAMEDATALEN) == 0)
			{
				ret = clas;
				goto end;
			}
			idx = clas->next_idx;
		}
	}
end:
	LWLockRelease(MetadataLogAppliersLock);
	return ret;
}


static void cluster_log_applier_sigusr2_hdlr(SIGNAL_ARGS)
{
	int			save_errno = errno;
	/*
	  Don't kick too frequently otherwise metadata node is overwhelmed.
	*/
	static sig_atomic_t last_topo_kick = 0;
	time_t now = time(NULL);
	if (now - last_topo_kick < 1)
		return;

	SetLatch(MyLatch);
	last_topo_kick = now;
	errno = save_errno;
}





/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
cluster_log_applier_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 *		Set a flag to tell the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void
cluster_log_applier_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Startup applier processes for all other dbs than 'postgres'.
 * 'template0/1' are system dbs that should not be used, they are skipped here.
 */
static void
startup_cluster_log_appliers()
{
	/* Create worker state object. */
	/*
	 * Open pg_database table, go through all rows, create one bgworker
	 * process for each db.
	 * */
	scan_all_dbs(create_bgworker_all_db, NULL);
}

/*
 * dropped_dbname: NULL if not a 'drop database' stmt or not in main applier;
 * non-null if a main applier executing a drop db stmt.
 *
 * @retval 
 * when *execed false: -1: tobe dropped db not exist;
 *          -2: postmaster found died while waiting for bgworker connected to the to-be-dropped db
 *          0: sqlstr is NULL, nothing to execute. only update newpos.
 * when *execed true: sql stmt SPI execution result or error code, e.g. SPI_OK_UTILITY.
 * */
static int apply_cluster_ddl_log(uint64_t newpos, const char *sqlstr,
	DDL_OP_Types optype, DDL_ObjTypes objtype, const char *objname, bool*execed)
{
	int ret = 0;
	*execed = false;

	ResetRemoteDDLStmt();
	SetCurrentStatementStartTimestamp();
	/*
	 * CREATE/DROP DATABASE can not be executed in a txn block.
	 * */
	Assert(!IsTransactionState());
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "Applying ddl log records.");

	if (objtype == DDL_ObjType_db && optype == DDL_OP_Type_drop)
	{
		Assert(sqlstr != NULL);
		Oid dbid = get_database_oid(objname, true);
		/*
		* the db doesn't exist, no need to drop it. This is not an error
		* for the applier.
		* */
		if (dbid == InvalidOid)
		{
			elog(WARNING, "bgworker main ddl applier found database %s gone when applying drop database SQL stmt '%s' with log-id %ld",
				 objname, sqlstr, newpos);
			ret = SPI_OK_UTILITY;
			goto end;
		}


		/*
		 * the bgworker applier connected to the db must disconnect so
		 * we can drop the db. only main applier can do the notify, others
		 * simply skip this log.
		 * */
		BgwHandleStatus status;

		status = notify_wait_bgworker_applier_exit(dbid);
		if (status == BGWH_POSTMASTER_DIED)
		{
			return -2; // exit quickly.
		}
		delete_ddl_log_progress(dbid);
	}

	/* We can now execute queries via SPI */
	if (sqlstr)
	{
		ret = SPI_execute(sqlstr, false, 0);
		*execed = true;
	}
	if (objtype == DDL_ObjType_db && optype == DDL_OP_Type_create)
	{
	   	Oid dbid = get_database_oid(objname, true);
		Assert(dbid != InvalidOid);
		insert_ddl_log_progress(dbid, newpos);
	}
end:
	SPI_finish();
	if (sqlstr == NULL || ret == SPI_OK_UTILITY)
	{
		if (SetLatestDDLOperationId(newpos))
			g_applier_state.clas->latest_sync_opid_executed = newpos;
	}

	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
	return ret;
}

static int on_remote_meta_sync(Oid dbid)
{
	if (dbid == InvalidOid) return -1;

	BgwHandleStatus status = notify_wait_bgworker_applier_exit(dbid);
	if (status == BGWH_POSTMASTER_DIED)
	{
		return -2; // exit quickly.
	}
	return 0;
}

void
cluster_log_applier_main(Datum main_arg)
{
	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, cluster_log_applier_sighup);
	pqsignal(SIGTERM, cluster_log_applier_sigterm);
	pqsignal(SIGUSR2, cluster_log_applier_sigusr2_hdlr);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection("postgres", NULL, 0);
	Oid dbid = cluster_log_applier_init();
	int ndone = 0;
	bool do_once = true;
	 DEBUG_INJECT_IF("cluster_log_applier_main_sleep_set_debug", sleep(20););

	RequestShardingTopoCheck(METADATA_SHARDID);
	RequestShardingTopoCheckAllStorageShards();
	find_metadata_master_and_recover();
	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate
	 */
	PG_TRY();
	{

	while (!got_sigterm && g_applier_state.clas->keep_running)
	{
		CHECK_FOR_INTERRUPTS();

		/*
		 * In case of a SIGHUP, just reload the configuration.
		 */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/*
		 Check twice in each loop.
		 Crucial to check before fetch_apply_cluster_ddl_logs(), this can fix
		 many master switch cases. but fetch_apply_cluster_ddl_logs() could
		 still be stuck and causes this process to exit and be restarted.
		*/
		enable_remote_timeout();
		ProcessShardingTopoReqs();
		disable_remote_timeout();

		if (do_once)
		{
			// recovery
			do_once = false;
			enable_remote_timeout();
			ndone = fetch_apply_cluster_ddl_logs(dbid, "postgres",
				g_applier_state.local_max_opid, apply_cluster_ddl_log,
				true, true);
			disable_remote_timeout();
			/*
			 * postmaster dead, exit.
			 * */
			if (ndone == -2)
				break;
		}

		// task 0: fetch DDL new ddl stmts from metadata cluster and execute them.
		if (dbid != InvalidOid)
		{
			enable_remote_timeout();
			ndone = fetch_apply_cluster_ddl_logs(dbid, "postgres",
				g_applier_state.clas->latest_sync_opid_executed, apply_cluster_ddl_log,
				true, false);
			disable_remote_timeout();
		}
		/*
		 * postmaster dead, exit.
		 * */
		if (ndone == -2)
			break;
		// task 1: start new appliers for newly created dbs.
		Assert(!IsTransactionState());
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());
		pgstat_report_activity(STATE_RUNNING, "Applying ddl log records.");
		/*
		 * See if there are new dbs created, if so, create bgworker applier
		 * for them.
		 * */
		scan_all_dbs(create_bgworker_new_db, NULL);

		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		pgstat_report_activity(STATE_IDLE, NULL);

		// task 2: handle topology update requests.
		enable_remote_timeout();
		ProcessShardingTopoReqs();
		disable_remote_timeout();

		// task 3: kill connections/queries
		reapShardConnKillReqs();

		// task 4: fetch sequence values
		fetchSeqValues();

		// task 5: terminate appliers whose dbs are being dropped.
		enable_remote_timeout();
		int ndone2 = handle_remote_meta_sync_reqs(on_remote_meta_sync);
		disable_remote_timeout();

		/*
		 * postmaster dead, exit.
		 * */
		if (ndone2 == -2)
			break;

		/*
		  If we terminated some applier bg workers because the dbs they service
		  are dropped, we'd better wait for a while for them to be dropped
		  in backends, otherwise we will again find them and start appliers
		  for them. And if we did so it's little harm --- a useless bg worker
		  process is started, that's all. If the same name is created again,
		  the new bg process will service it correctly.
		*/
		if (ndone2 > 0) wait_latch(100);

		/*
		 * If we got error executing a DDL stmt, wait for a while and retry.
		 * Possible cause is that the db object is being used and can't be
		 * dropped/modified.
		 * */
		if (ndone <= 0)
			wait_latch(1000 * cluster_log_applier_naptime);
	}

	}
	PG_CATCH();
	{
		LWLockAcquire(RemoteMetaSyncLock, LW_EXCLUSIVE);
		g_remote_meta_sync->main_applier_pid = 0;
		LWLockRelease(RemoteMetaSyncLock);
		/*
		  this will free all waiters which started to wait before above
		  main_applier_pid turned off.
		*/
		notify_other_appliers_to_exit(ndone != -2);// by setting the keep_running flag.
		terminate_this_bgw(true, got_sigterm);
		PG_RE_THROW();
	}
	PG_END_TRY();
	/*
	 * if postmaster dies it will send SIGTERM to all bgworker processes
	 * including the dynamic ones, no need to alert them here. also we don't
	 * want to touch the shared memory in this case either.
	 * */
	if (ndone != -2)
	{
		LWLockAcquire(RemoteMetaSyncLock, LW_EXCLUSIVE);
		g_remote_meta_sync->main_applier_pid = 0;
		LWLockRelease(RemoteMetaSyncLock);
		/*
		  this will free all waiters which started to wait before above
		  main_applier_pid turned off.
		*/
		notify_other_appliers_to_exit(ndone != -2);// by setting the keep_running flag.
		terminate_this_bgw(true, got_sigterm);
	}

	proc_exit(1);
}

static bool create_bgworker_all_db(Oid dbid, const char *db, void *param)
{
	ClusterLogApplierState *clas = alloc_slot_for_db(dbid, db);

	// create bgworker process for the new db.
	setup_background_worker(clas, dbid);
	return true;
}

/*
 * the handler returns true to go on with next db, false stops the scan.
 * */
static bool create_bgworker_new_db(Oid dbid, const char *db, void*param)
{
	ClusterLogApplierState *clas = find_slot_by_dbid(dbid);
	/*
	 * bgworker for this db already exists.
	 * */
	if (clas != NULL)
		return true;

	clas = alloc_slot_for_db(dbid, db);

	// create bgworker process for the new db.
	setup_background_worker(clas, dbid);
	return true;
}

static void cluster_log_applier_init_common(ClusterLogApplierState *clas)
{
	replaying_ddl_log = true;
	XactIsoLevel = XACT_READ_COMMITTED;
	clas->keep_running = true;
	clas->is_running = true;
	clas->latest_sync_opid_executed = GetLatestDDLOperationId(&g_applier_state.local_max_opid);
	InitRemoteDDLContext();
	InitRemoteTypeInfo();
	ShardCacheInit();
	InitShardingSession();
	skip_top_level_check = true;// so we can execute CREATE/DROP DATABASE here.
	GetClusterName2(); // fetch it to memory so that in fetch_apply_cluster_ddl_logs(), no need for txn context.
}

static Oid cluster_log_applier_init()
{
	Assert(!IsTransactionState());
	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "initializing cluster_log_applier");

	alloc_clag_shmem();
	Oid dbid = get_database_oid("postgres", false);
	ClusterLogApplierState *clas = alloc_slot_for_db(dbid, "postgres");
	cluster_log_applier_init_common(clas);

	// Make sure g_remote_meta_sync->main_bgw_hdl is set by
	// postmaster(our launcher) before we read it. And other than here,
	// accesses to main applier's bgw_hdl don't need syncs since pm won't access it again.
	LWLockAcquire(RemoteMetaSyncLock, LW_EXCLUSIVE);
	clas->bgw_hdl = g_remote_meta_sync->main_bgw_hdl;
	LWLockRelease(RemoteMetaSyncLock);
	
	g_applier_state.clas = clas;
	g_applier_state.dbid = dbid;
	strcpy(g_applier_state.dbname.data, "postgres");
	before_shmem_exit(disconnect_request_kill_meta_conn, 0);

	/*
	 * Startup bgworker processes for all other dbs.
	 * */
	startup_cluster_log_appliers();

	// execute SQL queries if needed.
	//ret = SPI_execute(sqlstr, false, 0);
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
	g_remote_meta_sync->main_applier_pid = getpid();
	return dbid;
}

static void cluster_log_applier_init_others(Datum main_arg)
{
	Oid dbid = ((main_arg & 0xffffffff00000000) >> 32);
	int dsmhdl = (main_arg & 0xffffffff);

	/*  
	 * Connect to the dynamic shared memory segment.
	 *
	 * The backend that registered this worker passed us the ID of a shared
	 * memory segment to which we must attach for further instructions.
	 * At process exit, dsm will be auto detached.
	 */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "cluster_log_applier_main_others worker");
	dsm_segment *dsm_seg = dsm_attach(dsmhdl);
	if (dsm_seg == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("Kunlun-db: Unable to map dynamic shared memory segment for db %u and dsm handle %d.", dbid, dsmhdl),
				 errhint("Make sure the main applier bg process is still running.")));

	ClusterLogApplierGlobal *clag = dsm_segment_address(dsm_seg);
	g_applier_state.first_clag = g_applier_state.last_clag = clag;
	// other appliers only use the clag containing its clas, and they don't
	// attach to other dsm segments (and can't access them) anyway. their clas
	// was already alloced and filled by the main applier which started them.
	ClusterLogApplierState *clas = find_slot_by_dbid(dbid);

	if (!clas)
	{
		/*
		  the db was just dropped and its slot was cleared, but postmaster
		  doesn't know about it. the bg worker is still registered in
		  postmaster's registry, thus it's restarted. simply exit.
		*/
		return;
	}

	g_applier_state.clas = clas;
	g_applier_state.dsm_seg = dsm_seg;
	g_applier_state.dbid = dbid;
	strncpy(g_applier_state.dbname.data, clas->dbname.data, NAMEDATALEN - 1);

	/* Connect to our database */
	BackgroundWorkerInitializeConnection(clas->dbname.data, NULL, 0);

	Assert(!IsTransactionState());
	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "initializing cluster_log_applier");

	cluster_log_applier_init_common(clas);
	
	// execute SQL queries if needed.
	//ret = SPI_execute(sqlstr, false, 0);
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
}


void
cluster_log_applier_main_others(Datum main_arg)
{
	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, cluster_log_applier_sighup);
	pqsignal(SIGTERM, cluster_log_applier_sigterm);
	pqsignal(SIGUSR2, cluster_log_applier_sigusr2_hdlr);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	cluster_log_applier_init_others(main_arg);
	int ret = 0;
	bool do_once = true;
	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate
	 */
	PG_TRY();
	{
	while (!got_sigterm && g_applier_state.clas &&
			g_applier_state.clas->keep_running)
	{
		CHECK_FOR_INTERRUPTS();

		/*
		 * In case of a SIGHUP, just reload the configuration.
		 */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (do_once)
		{
			do_once = false;
			enable_remote_timeout();
			ret = fetch_apply_cluster_ddl_logs(g_applier_state.dbid, g_applier_state.dbname.data,
				g_applier_state.local_max_opid, apply_cluster_ddl_log,
				false, true);
			disable_remote_timeout();

			/*
			 * postmaster dead, exit.
			 * */
			if (ret == -2)
				break;
		}

		enable_remote_timeout();
		ret = fetch_apply_cluster_ddl_logs(g_applier_state.dbid, g_applier_state.dbname.data,
			g_applier_state.clas->latest_sync_opid_executed, apply_cluster_ddl_log,
			false, false);
		disable_remote_timeout();
		if (ret == -2)
			break;
		/*
		  Process sequence fetch requests for seqs of connected db.
		*/
		fetchSeqValues();

		if (ret <= 0)
			wait_latch(1000 * cluster_log_applier_naptime);
	}
	}
	PG_CATCH();
	{
		if (g_applier_state.dbid != InvalidOid)
			wakeup_remote_meta_sync_waiters(g_applier_state.dbid);
		terminate_this_bgw(false, got_sigterm);
		PG_RE_THROW();
	}
	PG_END_TRY();
	/*
	  we can come here if and only if:
	  1. postmaster dies(ret==-2), all processes of current db instance will exit, no need to inform waiters;
	  2. main applier inform this process to exit, and main applier will notify the 'drop db' waiter
	*/
	//wakeup_remote_meta_sync_waiters(g_applier_state.dbid);
	terminate_this_bgw(false, got_sigterm);
	proc_exit(1);
}


static void terminate_this_bgw(bool is_main, bool got_sigterm) {
	BackgroundWorkerHandle bgw_hdl;
	bool term_bgw = false;
	LWLockAcquire(MetadataLogAppliersLock, LW_SHARED);
	if (g_applier_state.clas) {
		/*
		  When main applier notifies an applier to exit, it terminated the
		  applier already, don't do it again.

		  Never terminate a main applier, it must be restarted by postmaster
		  to keep working.
		*/
		bgw_hdl = g_applier_state.clas->bgw_hdl;
		if (IsValidBGWHandle(&bgw_hdl) && !got_sigterm && !is_main) term_bgw = true;
	}
	LWLockRelease(MetadataLogAppliersLock);
	if (g_applier_state.clas && !is_main) free_clas_slot(g_applier_state.clas);
	if (is_main) free_all_clas_slots();
	// free bgw slot
	if (term_bgw) TerminateBackgroundWorker(&bgw_hdl);
}


static BgwHandleStatus notify_wait_bgworker_applier_exit(Oid dbid)
{
	ClusterLogApplierState *clas = find_slot_by_dbid(dbid);
	if (!clas || !clas->is_running)
		return -1;
	clas->keep_running = false;

	/*
	 * Make sure the bgworker connected to dbid has exited, so that
	 * we can drop the db.
	 * check_worker_exited() and hence this function can only be called in
	 * main applier process because only it has 
	 * the BackgroundWorkerHandle handles to other appliers.
	 *
	 * if postmaster died while we were waiting, don't touch shared memory,
	 * exit ASAP.
	 * */
	BgwHandleStatus status = check_worker_exited(clas);
	if (status == BGWH_POSTMASTER_DIED) goto end;
end:
	return status;
}

static void notify_other_appliers_to_exit(bool wait)
{
	LWLockAcquire(MetadataLogAppliersLock, LW_EXCLUSIVE);
	for (ClusterLogApplierGlobal *clag = g_applier_state.first_clag;
		 clag; clag = clag->next_section)
	{
		ClusterLogApplierState *clas = NULL;
		for (short idx = clag->first_slot; idx != INVALID_SLOT_IDX; )
		{
			clas = clag->slots + idx;
			clas->keep_running = false;
			idx = clas->next_idx;
			/*TerminateBackgroundWorker(&clas->bgw_hdl);
			clas->bgw_hdl.slot = -1;// make it invalid */
			//free_clas_slot_nolock(clas); can't do it here, the links would be broken.
			// caller will free all slots.
		}
	}

	LWLockRelease(MetadataLogAppliersLock);
	if (!wait)
		return;
	check_all_workers_exited();
}


/*
 * Entrypoint of this module.
 *
 * We register more than one worker process here, to demonstrate how that can
 * be done.
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	/* get the configuration */
	DefineCustomIntVariable("cluster_log_applier.naptime",
							"Duration between each check (in seconds).",
							NULL,
							&cluster_log_applier_naptime,
							10,
							1,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	if (!process_shared_preload_libraries_in_progress)
		return;


	/* set up common data for all our workers */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 0;
	sprintf(worker.bgw_library_name, "cluster_log_applier");
	sprintf(worker.bgw_function_name, "cluster_log_applier_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "cluster_log_applier main worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "cluster_log_applier");
	worker.bgw_notify_pid = 0;

	/*
	 * Now fill in worker-specific data, and do the actual registrations.
	 */
	worker.bgw_main_arg = 0;

	RegisterBackgroundWorker(&worker);
}

/*
 * Dynamically launch an SPI worker.
 */
Datum
cluster_log_applier_launch(PG_FUNCTION_ARGS)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle = NULL;
	BgwHandleStatus status;
	pid_t		pid;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 0;
	sprintf(worker.bgw_library_name, "cluster_log_applier");
	sprintf(worker.bgw_function_name, "cluster_log_applier_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "cluster_log_applier main worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "cluster_log_applier");
	worker.bgw_main_arg = 0;
	/* set bgw_notify_pid so that we can use WaitForBackgroundWorkerStartup */
	worker.bgw_notify_pid = MyProcPid;

	LWLockAcquire(RemoteMetaSyncLock, LW_EXCLUSIVE);
	if (!RegisterDynamicBackgroundWorker(&worker, &handle)) {
		LWLockRelease(RemoteMetaSyncLock);
		elog(WARNING,
			 "Could not register main applier background process for database postgres, you may need to increase max_worker_processes.");
		PG_RETURN_NULL();
	}

	/*
	  The dsm segments are alloced in main applier process, they are unaware in
	  postmaster, so we have to store the handle in somewhere the pm knows.
	  We want to isolate the dsm to be in the module code base, and we want them
	  dynamic since they are already so. we could have alloced a prefixed hundreds
	  of clas slots for every db.
	*/
	g_remote_meta_sync->main_bgw_hdl = *handle;
	LWLockRelease(RemoteMetaSyncLock);

	status = WaitForBackgroundWorkerStartup(handle, &pid);
	if (status == BGWH_STOPPED)
	{
		elog(WARNING,
			"Could not start main applier bg process for database postgres, or it exited.");
		TerminateBackgroundWorker(handle);
	}

	if (status == BGWH_POSTMASTER_DIED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("Kunlun-db: Postmaster died, cannot start background processes without it."),
				 errhint("Kill all remaining database processes and restart the database.")));
	Assert(status == BGWH_STARTED);

	PG_RETURN_INT32(pid);
}



/*
 * Register one background worker in main applier.
 */
static void
setup_background_worker(ClusterLogApplierState *clas, Oid dbid)
{
#ifdef ENABLE_DEBUG
	ClusterLogApplierState *clas0 = find_slot_by_dbid(dbid);
	Assert(clas0 == clas);
#endif
	BackgroundWorker worker;
	dsm_segment *dsm_seg = clas->owner->dsm_seg; //g_applier_state.dsm_seg;
	/*
	 * Arrange to kill all the workers if we abort before all workers are
	 * finished hooking themselves up to the dynamic shared memory segment.
	 */
	on_dsm_detach(dsm_seg, cleanup_background_workers, 0);


	/* Configure a worker. */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	/*
	  Workers can only be (re)started by main applier, because main applier allocs
	  dynamic shared memory for all appliers, but postmaster could restart bgworkers
	  in any order using the bgw_main_arg registered before, but the dsm_handle in
	  the bgw_main_arg is invalid if the main applier also exited. So some
	  appliers could be started earlier
	  than the main applier in case restart needed(e.g. if a crash happens to
	  any pg process), they would be accessing invalid dsm.
	*/
	worker.bgw_restart_time = BGW_NEVER_RESTART;

	sprintf(worker.bgw_library_name, "cluster_log_applier");
	sprintf(worker.bgw_function_name, "cluster_log_applier_main_others");
	snprintf(worker.bgw_type, BGW_MAXLEN, "cluster_log_applier");
	snprintf(worker.bgw_name, BGW_MAXLEN, "cluster_log_applier applier for db %s", clas->dbname.data);

	uint64_t bi = dbid;
	bi <<= 32;
	bi |= dsm_segment_handle(dsm_seg);
	worker.bgw_main_arg = UInt64GetDatum(bi);

	/* set bgw_notify_pid, so we can detect if the worker stops */
	worker.bgw_notify_pid = MyProcPid;

	/* Register the workers. */
	BackgroundWorkerHandle *handle = NULL;
	LWLockAcquire(MetadataLogAppliersLock, LW_EXCLUSIVE);
	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		free_clas_slot_nolock(clas);
		LWLockRelease(MetadataLogAppliersLock);
		elog(WARNING,
			 "Could not register applier background process for database (%u), you may need to increase max_worker_processes.",
			 dbid);
				 
		return;
	}
	/*
	  We must read/write other appliers' bgw_hdl field synced. Since DDLs are not frequently
	  executed, we simply use this one big lock. In each applier process sync
	  accesses to this field too. After below write, the bgw_hdl field will be
	  read only so later access take SHARED lock.
	*/
	memcpy(&clas->bgw_hdl, handle, sizeof(BackgroundWorkerHandle));
	LWLockRelease(MetadataLogAppliersLock);

	pid_t pid = 0;
	BgwHandleStatus status = WaitForBackgroundWorkerStartup(handle, &pid);

	/*
	  If bg worker got error, it will exit. but the main applier should not
	  exit too, it must keep working for potentially more new dbs created/dropped.
	  and if the error conditions could recover, later retries will start it up.
	*/
	if (status == BGWH_STOPPED)
	{
		free_clas_slot(clas);
		TerminateBackgroundWorker(handle);
		elog(WARNING,
			"Could not start applier bg process for database (%u), or it exited.",
			dbid);
		goto end;
	}
	if (status == BGWH_POSTMASTER_DIED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("Kunlun-db: Postmaster died, cannot start background processes without it."),
				 errhint("Kill all remaining database processes and restart the database.")));
	Assert(status == BGWH_STARTED);

end:
	cancel_on_dsm_detach(dsm_seg, cleanup_background_workers, 0);
	return;
}

static void
cleanup_background_workers(dsm_segment *seg, Datum arg)
{
	LWLockAcquire(MetadataLogAppliersLock, LW_EXCLUSIVE);
	for (ClusterLogApplierGlobal *clag = g_applier_state.first_clag; clag; clag= clag->next_section)
	{
		for (int idx = clag->first_slot; idx >= 0;)
		{
			ClusterLogApplierState *clas = clag->slots + idx;
			TerminateBackgroundWorker(&clas->bgw_hdl);
			idx = clas->next_idx;
		}
	}
	LWLockRelease(MetadataLogAppliersLock);
}

static void
check_all_workers_exited()
{
	BgwHandleStatus status;

	for (ClusterLogApplierGlobal *clag = g_applier_state.first_clag; clag; clag= clag->next_section)
	{
		for (int idx = clag->first_slot; idx >= 0;)
		{
			ClusterLogApplierState *clas = clag->slots + idx;
			// don't wait myself or already exited bgw.
			if (clas->dbname.data[0] == '\0' ||
				strcmp("postgres", clas->dbname.data) == 0)
			{
				idx = clas->next_idx;
				continue;
			}
			// caller should hold SHARED(exclusive is also OK) lock
			status = WaitForBackgroundWorkerShutdown(&clas->bgw_hdl);
			if (status == BGWH_POSTMASTER_DIED)
				return;
			Assert(status == BGWH_STOPPED);
			idx = clas->next_idx;
		}
	}
}

static BgwHandleStatus
check_worker_exited(ClusterLogApplierState *clas)
{
	BgwHandleStatus status;
	// caller should hold SHARED lock
	status = WaitForBackgroundWorkerShutdown(&clas->bgw_hdl);
	return status;
}
