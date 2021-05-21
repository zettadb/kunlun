/*-------------------------------------------------------------------------
 *
 * algos.c
 *      Various algorithm functions.
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/algos.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/algos.h"

static int ip_str_from_int(char *str, size_t len, uint32_t ip);
/*
 * convert an ipv4 integer to its string form.
 * @retval -1 if str buffer insufficient; 0 on success.
 * */
static int ip_str_from_int(char *str, size_t len, uint32_t ip)
{
	uint32_t p4 = ip % 256;
	ip = ip / 256;
	uint32_t p3 = ip % 256;
	ip = ip / 256;
	uint32_t p2 = ip % 256;
	ip = ip / 256;
	uint32_t p1 = ip;

	int ret = snprintf(str, len, "%u.%u.%u.%u", p1, p2, p3, p4);
	if (ret >= len)
		return -1;
	return 0;
}

