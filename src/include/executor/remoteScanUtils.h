#ifndef REMOTESCANUTILS_H
#define REMOTESCANUTILS_H

#include "postgres.h"
#include "access/remote_dml.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "nodes/print.h"
typedef struct ScanTupleGenContext
{
	List *exprs;
	List *vars;
	EState *estate;
	TupleDesc tupledesc;

	List *unpushable_exprs;

	VarPickerCtx vpc;
	RemotePrintExprContext rpec;
}
ScanTupleGenContext;

extern void InitScanTupleGenContext(ScanTupleGenContext *context, PlanState *planstate, bool skipjunk);
extern Var* lookup_scanvar_for_expr(ScanTupleGenContext *context, Expr *expr);
extern bool alloc_scanvar_for_expr(ScanTupleGenContext *context, Expr *expr);
extern Node *replace_expr_with_scanvar_mutator(Node *node, ScanTupleGenContext *context);
extern Node *restore_scanvar_with_expr_mutator(Node *node, ScanTupleGenContext *context);
#endif