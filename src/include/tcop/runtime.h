/*-------------------------------------------------------------------------
 * runtime.h
 *		runtime state
 *
 * Copyright (c) 2019 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * IDENTIFICATION
 *	  src/include/tcop/runtime.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RUNTIME_H
#define RUNTIME_H
struct Runtime_env {
	int argc;
	char **argv;
	const char *dbname;
	const char *username;
	const char *query_string;
	const char *obj_name;
};

extern struct Runtime_env g_runtime_env;
extern void set_fatal_signals_handling(void);
#endif // !RUNTIME_H
