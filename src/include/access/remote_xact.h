/*-------------------------------------------------------------------------
 *
 * remote_xact.h
 *	  POSTGRES global transaction mgmt code
 *
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * src/include/access/remote_xact.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef REMOTE_XACT_H
#define REMOTE_XACT_H

#include "access/xact.h"

typedef enum GTxnDD_Victim_Policy
{
	KILL_OLDEST,
	KILL_YOUNGEST,
	KILL_MOST_ROWS_CHANGED,
	KILL_LEAST_ROWS_CHANGED,
	KILL_MOST_ROWS_LOCKED,
	KILL_MOST_WAITING_BRANCHES,
	KILL_MOST_BLOCKING_BRANCHES
} GTxnDD_Victim_Policy;

extern int start_global_deadlock_detection_wait_timeout;
extern bool enable_global_deadlock_detection;
extern bool trace_global_deadlock_detection;
extern int g_glob_txnmgr_deadlock_detector_victim_policy;


extern Size GDDShmemSize(void);
extern void CreateGDDShmem(void);
extern void set_gdd_pid(void);
extern void kick_start_gdd(void);
extern size_t increment_gdd_reqs(void);

extern void perform_deadlock_detect(void);
extern void gdd_init(void);

extern void StartSubTxnRemote(const char *name);
extern void SendReleaseSavepointToRemote(const char *name);
extern void SendRollbackRemote(const char *txnid, bool xa_end, bool written_only);
extern void SendRollbackSubToRemote(const char *name);
extern bool Send1stPhaseRemote(const char *txnid);
extern void Send2ndPhaseRemote(const char *txnid);
extern void StartTxnRemote(StringInfo cmd);
extern char *MakeTopTxnName(TransactionId txnid, time_t now);
extern void insert_debug_sync(int where, int what, int which);
#endif // !REMOTE_XACT_H
