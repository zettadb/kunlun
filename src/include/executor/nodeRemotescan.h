/*-------------------------------------------------------------------------
 *
 * nodeRemotescan.h
 *
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 *
 * src/include/executor/nodeRemotescan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEREMOTESCAN_H
#define NODEREMOTESCAN_H

#include "access/parallel.h"
#include "nodes/execnodes.h"

extern RemoteScanState *ExecInitRemoteScan(RemoteScan *node, EState *estate, int eflags);
extern void ExecEndRemoteScan(RemoteScanState *node);
extern void ExecReScanRemoteScan(RemoteScanState *node);

/* parallel scan support */
extern void ExecRemoteScanEstimate(RemoteScanState *node, ParallelContext *pcxt);
extern void ExecRemoteScanInitializeDSM(RemoteScanState *node, ParallelContext *pcxt);
extern void ExecRemoteScanReInitializeDSM(RemoteScanState *node, ParallelContext *pcxt);
extern void ExecRemoteScanInitializeWorker(RemoteScanState *node,
							ParallelWorkerContext *pwcxt);
extern void ExecStoreRemoteTuple(TypeInputInfo *tii, MYSQL_ROW row,
	unsigned long *lengths, TupleTableSlot *slot);
extern void init_type_input_info(TypeInputInfo **tii, TupleTableSlot *slot,
	EState *estate);
extern void release_shard_conn(RemoteScanState *node);

extern void validate_column_reference(Var *colvar, Relation rel);
extern int append_cols_for_whole_var(Relation rel, TupleDesc *typeInfo, int cur_resno);
extern int add_unique_col_var(Relation rel, TupleDesc typeInfo, int cur_resno, TargetEntry *tle,
	Var *colvar, bool set_tle_resname);
#endif							/* NODEREMOTESCAN_H */
