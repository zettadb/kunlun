/*-------------------------------------------------------------------------
 *
 * remote_seq.h
 *	  definitions of types, and declarations of functions, that are used in
 *	  remote sequence implementation.
 *
 *
 * Copyright (c) 2019 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * src/include/commands/remote_seq.h
 *
 *-------------------------------------------------------------------------
 */

extern int64_t fetch_next_val(Relation seqrel);
extern void do_remote_setval(Relation seqrel, int64 next, bool is_called);
