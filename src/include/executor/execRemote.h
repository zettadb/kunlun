/*-------------------------------------------------------------------------
 *
 * execRemote.h
 *
 * Copyright (c) 2019 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * IDENTIFICATION
 * 		src/include/executor/execRemote.h
 *
 *-------------------------------------------------------------------------
*/

#ifndef EXEC_REMOTE_H
#define EXEC_REMOTE_H

#include "postgres.h"


extern TupleTableSlot *
ExecQualProjection(MaterialState *ms, TupleTableSlot *slot);


#endif // !EXEC_REMOTE_H
