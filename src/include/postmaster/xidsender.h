/*-------------------------------------------------------------------------
 *
 * xidsender.h
 *	  definitions of types, and declarations of functions, that are used in
 *	  xidsender background process.
 *
 *
 * Copyright (c) 2019 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * src/include/postmaster/xidsender.h
 *
 *-------------------------------------------------------------------------
 */
typedef uint64_t GlobalTrxId;
extern void CreateSharedBackendXidSlots(void);
extern Size BackendXidSenderShmemSize(void);
extern void xidsender_initialize(void);
extern void XidSenderMain(int argc, char **argv);
extern int  xidsender_start(void);
extern char WaitForXidCommitLogWrite(Oid comp_nodeid, GlobalTrxId xid, time_t deadline, bool commit_it);
extern bool wait_latch(int millisecs);

extern bool XidSyncDone(void);
extern uint32_t get_cluster_conn_thread_id(void);
extern bool skip_tidsync;
extern void disconnect_request_kill_meta_conn(int c, Datum d);
