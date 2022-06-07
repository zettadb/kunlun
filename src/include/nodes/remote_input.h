/*-------------------------------------------------------------------------
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *-------------------------------------------------------------------------
 */
#ifndef REMOTE_INPUT_H
#define REMOTE_INPUT_H

#include "postgres.h"

#include "nodes/execnodes.h"
extern void myInputInfo(Oid typid, int typmode, TypeInputInfo *info);

extern Datum myInputFuncCall(TypeInputInfo *info, char *str, int len, enum enum_field_types mytype, bool *isnull);

#endif