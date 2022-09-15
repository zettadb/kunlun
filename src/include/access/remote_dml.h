/*-------------------------------------------------------------------------
 *
 * remote_dml.h
 *	  POSTGRES remote access method DML statements processing code.
 *
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * src/include/access/remote_dml.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef REMOTE_DML_H
#define REMOTE_DML_H
#include "utils/relcache.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "nodes/execnodes.h"

extern int remote_param_fetch_threshold;

typedef struct VarPickerCtx
{
	/*
	 * Target columns of target scanned relation that we will need to execute
	 * the query. Iff !has_alien_cols and nvars > 0 and snprint_expr() can serialize the expr
	 * will we use the serialized text as target, otherwise if (nvars > 0) we use the Vars in
	 * target_cols as target list; otherwise the target isn't sent to remote.
	 * */
	Var **target_cols;
	MemoryContext mctx;
	int nvars, nvar_buf;

	/*
	 * Whether the expr contains columns of other tables. 
	 * */
	bool has_alien_cols;

	/*
	 * If there are no vars to reference target rel, and all Nodes of the
	 * targetEntry->expr are locally
	 * evaluable, then the target can be processed locally without sending it
	 * to remote.
	 *
	 * Currently only Const and SQLValueFunction are known to be locally evaluable.
	 * */
	int local_evaluables;

	/*
	 * There are base/leaf primitive nodes that we can't handle, have to
	 * return error to client in this case.
	 * */
	int local_unevaluables;

	/*
	 * Range table index of target relation to scan.
	 * */
	int scanrelid;

	bool check_alien_vars;
} VarPickerCtx;

struct RemotePrintExprContext;
/*
 * Used to construct sql for pushdownable update&&delete
 */
typedef struct RemoteUD
{
   ModifyTableState *mtstate;
   int planIndex;
   int index;       /* Index of current result relation */
   RangeTblEntry *parent_rte;
   Relation parent;
   Relation rel;    /* Current result relation */
   int rti;         /* RTE number of current result relation */
   List *tlist;     /* Update target list */
   List *qual;      /* Where clause of current result relation */
   PlanState *from; /* From clause */
   List *sortlist;  /* Sort clause */
   List *sortop;    /* Sort operator */
   List *sortnullfirst;
   int64_t limit;   /* Limit clause */
   List *rellist;   /* All of the result relation */
   List *quallist;  /* All of the qual of result relation */
   ResultRelInfo *relinfo;
} RemoteUD;

#define RemoteUDRelations(p) \
   (list_length((p)->rellist))

extern void RemoteUDSetup(PlanState *planstate, RemoteUD *remote_updel);
extern bool RemoteUDNext(RemoteUD *remote_updel, struct RemotePrintExprContext *rpec, StringInfo sql, Oid *shardid);
extern bool RemoteUDEOF(RemoteUD *remote_updel);

extern bool CheckPartitionKeyModified(Relation,  Index attrno);
extern bool CanPushdownRemoteUD(PlanState *state, List *unused_tl, int *nleafs, const char **reaseon);

extern bool var_picker(Node *node, VarPickerCtx*ctx);
extern TupleDesc expandTupleDesc2(TupleDesc tpd);
extern void reset_var_picker_ctx(VarPickerCtx *vpc);
#endif // !REMOTE_DML_H
