/*-------------------------------------------------------------------------
 *
 * dbug.h
 *	  debug facility
 *
 *
 * Copyright (c) 2019 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * src/include/tcop/dbug.h
 *
 * NOTES
 *
 *-------------------------------------------------------------------------
 */
#ifndef DBUG_H
#define DBUG_H

#include "lib/stringinfo.h"

extern StringInfoData session_debug;
extern StringInfoData global_debug;

void init_debug_sys(void);
Size DbugShmemSize(void);
void CreateDbugShmem(void);

void update_session_debug(const char *newval, void *extra);
void update_global_debug(const char *newval, void *extra);
const char *show_global_debug(void);
const char *show_session_debug(void);
#endif // DBUG_H
