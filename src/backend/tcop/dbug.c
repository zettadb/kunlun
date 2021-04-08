/*-------------------------------------------------------------------------
 * dbug.c
 *		Debugging auxillary functions.
 *
 * Copyright (c) 2019 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * IDENTIFICATION
 *	  src/backend/tcop/dbug.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "tcop/debug_funcs.h"
#include "tcop/runtime.h"

StringInfoData session_debug;
StringInfoData global_debug;

void init_debug_sys()
{
}

Size DbugShmemSize()
{
	return 0;
}

void CreateDbugShmem()
{}

void
update_global_debug(const char *newval, void *extra)
{
}

void
update_session_debug(const char *newval, void *extra)
{
}

const char *show_global_debug(void)
{
	return NULL;
}

const char *show_session_debug(void)
{
	return NULL;
}

bool enable_stacktrace = true;
bool enable_coredump = true;

void set_fatal_signals_handling() {}
