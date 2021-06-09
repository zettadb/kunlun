/*-------------------------------------------------------------------------
 *
 * execRemote.c
 *	  Remote execution utility functions.
 *
 * Copyright (c) 2019 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * IDENTIFICATION
 *	  src/backend/executor/execRemote.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "nodes/plannodes.h"

#include <limits.h>
#include <math.h>

#include "executor/executor.h"
#include "executor/execRemote.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#ifdef OPTIMIZER_DEBUG
#include "nodes/print.h"
#endif
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "catalog/pg_type.h"


/*
  Derived from ExecScan().
*/
TupleTableSlot *
ExecQualProjection(MaterialState *ms, TupleTableSlot *slot)
{
    ExprContext *econtext;
    ExprState  *qual;
    ProjectionInfo *projInfo;
	ScanState *node = (ScanState *)ms;

    /*  
     * Fetch data from node
     */
    qual = node->ps.qual;
    projInfo = node->ps.ps_ProjInfo;
    econtext = node->ps.ps_ExprContext;

	/* interrupt checks are in ExecScanFetch */

	/*
	 * If we have neither a qual to check nor a projection to do, just skip
	 * all the overhead and return the raw scan tuple.
	 */
	if (!qual && !projInfo)
	{
		return slot;
	}

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ResetExprContext(econtext);

	/*
	 * if the slot returned by the accessMtd contains NULL, then it means
	 * there is nothing more to scan so we just return an empty slot,
	 * being careful to use the projection result slot so it has correct
	 * tupleDesc.
	 */
	if (TupIsNull(slot))
	{
		if (projInfo)
			return ExecClearTuple(projInfo->pi_state.resultslot);
		else
			return slot;
	}

	/*
	 * place the current tuple into the expr context
	 */
	econtext->ecxt_scantuple = slot;

	/*
	 * check that the current tuple satisfies the qual-clause
	 *
	 * check for non-null qual here to avoid a function call to ExecQual()
	 * when the qual is null ... saves only a few cycles, but they add up
	 * ...
	 */
	if (qual == NULL || ExecQual(qual, econtext))
	{
		/*
		 * Found a satisfactory scan tuple.
		 */
		if (projInfo)
		{
			/*
			 * Form a projection tuple, store it in the result tuple slot
			 * and return it.
			 */
			return ExecProject(projInfo);
		}
		else
		{
			/*
			 * Here, we aren't projecting, so just return scan tuple.
			 */
			return slot;
		}
	}
	else
	{
		InstrCountFiltered1(node, 1);
		return NULL;
	}
}
