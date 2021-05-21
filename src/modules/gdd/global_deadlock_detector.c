/* -------------------------------------------------------------------------
 *
 * global_deadlock_detector.c
 *      global deadlock detector, fetch wait-for relationships of transaction
 *      branches from each storage node, to build a wait-for graph of global
 *      transaction. Traverse the graph to find deadlocks.
 *
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * IDENTIFICATION
 *		src/backend/sharding/global_deadlock_detector.c
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
#include <time.h>

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(global_deadlock_detector_launch);

void		_PG_init(void);
void		global_deadlock_detector_main(Datum) pg_attribute_noreturn();

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sigusr2 = false;

/*
 * On SIGUSR2, wakeup and do one round of deadlock detection. Sessions waiting
 * too long for storage nodes to return can send this signal to activate DD.
 * */
static void global_deadlock_detector_sigusr2_hdlr(SIGNAL_ARGS)
{
	int			save_errno = errno;

	increment_gdd_reqs();
	got_sigusr2 = true;
	SetLatch(MyLatch);
	errno = save_errno;
}


/* GUC variables */
static int	global_deadlock_detector_naptime = 10;



/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
global_deadlock_detector_sigterm(SIGNAL_ARGS)
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
global_deadlock_detector_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Initialize workspace for a worker process: create the schema if it doesn't
 * already exist.
 */
static void
initialize_global_deadlock_detector()
{
	Assert(!IsTransactionState());
	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "initializing global_deadlock_detector");

	gdd_init();
	set_gdd_pid();

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
}

inline static int gdd_log_level()
{
	return trace_global_deadlock_detection ? LOG : DEBUG1;
}


void
global_deadlock_detector_main(Datum main_arg)
{
	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, global_deadlock_detector_sighup);
	pqsignal(SIGTERM, global_deadlock_detector_sigterm);
	pqsignal(SIGUSR2, global_deadlock_detector_sigusr2_hdlr);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection("postgres", NULL, 0);

	initialize_global_deadlock_detector();


	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate
	 */
	while (!got_sigterm)
	{
		if (!got_sigusr2)
		{
			bool timedout = wait_latch(global_deadlock_detector_naptime*1000);
			if (!timedout)
				elog(gdd_log_level(), "GDD waken up by backends.");
		}
		got_sigusr2 = false;

		CHECK_FOR_INTERRUPTS();

		/*
		 * In case of a SIGHUP, just reload the configuration.
		 */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		perform_deadlock_detect();
	}
	proc_exit(1);
}

/*
 * Entrypoint of this module.
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	/* get the configuration */
	DefineCustomIntVariable("global_deadlock_detector.naptime",
							"Duration between each check (in seconds).",
							NULL,
							&global_deadlock_detector_naptime,
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
	sprintf(worker.bgw_library_name, "global_deadlock_detector");
	sprintf(worker.bgw_function_name, "global_deadlock_detector_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "global_deadlock_detector worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "global_deadlock_detector");
	worker.bgw_notify_pid = 0;

	/*
	 * Now fill in worker-specific data, and do the actual registrations.
	 */
	worker.bgw_main_arg = 0;

	RegisterBackgroundWorker(&worker);
}

/*
 * Dynamically launch a global deadlock detector.
 */
Datum
global_deadlock_detector_launch(PG_FUNCTION_ARGS)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t		pid;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 0;
	sprintf(worker.bgw_library_name, "global_deadlock_detector");
	sprintf(worker.bgw_function_name, "global_deadlock_detector_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "global_deadlock_detector worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "global_deadlock_detector");
	worker.bgw_main_arg = 0;
	/* set bgw_notify_pid so that we can use WaitForBackgroundWorkerStartup */
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		PG_RETURN_NULL();

	status = WaitForBackgroundWorkerStartup(handle, &pid);

	if (status == BGWH_STOPPED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("Kunlun-db: could not start background process"),
				 errhint("More details may be available in the server log.")));
	if (status == BGWH_POSTMASTER_DIED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("Kunlun-db: cannot start background processes without postmaster"),
				 errhint("Kill all remaining database processes and restart the database.")));
	Assert(status == BGWH_STARTED);

	PG_RETURN_INT32(pid);
}


