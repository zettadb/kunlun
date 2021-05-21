/*-------------------------------------------------------------------------
 *
 * bin_search.c
 *      Binary search algorithm, what's different from standard library's bsearch,
 *      bin_search can return the insert position if a key isn't found in the
 *      array. 
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/bin_search.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/algos.h"

static int bin_search_int(const void *key, const void *base, int start, int end,
				   size_t elesz, func_cmp_t cmpf, int *inspos);
/*
 * binary search in [base + start, base + end].
 * */
static int bin_search_int(const void *key, const void *base, int start, int end,
				   size_t elesz, func_cmp_t cmpf, int *inspos)
{
	if (start > end)
	{
		if (inspos)
			*inspos = start; // insert right before base[start].
		return -1;
	}

	int mid = (start + end) / 2;
	void *ptr = (char *)base + elesz*mid;
	int res = cmpf(key, ptr);
	if (res < 0)
		return bin_search_int(key, base, start, mid - 1, elesz, cmpf, inspos);
	else if (res > 0)
		return bin_search_int(key, base, mid + 1, end, elesz, cmpf, inspos);
	else
		return mid;
}

/*
 * find key from [base, base + num_elements), return array index if found, return -1
 * if not found and in this case *inspos is the position to insert this key to keep the
 * array sorted. return -2 if argument error.
 * */
int bin_search(const void *key, const void *base, size_t num_elems,
			   size_t elesz, func_cmp_t cmpf, int *inspos)
{
	if (key == NULL || base == NULL || cmpf == NULL)
		return -2;

	if (num_elems == 0)
	{
		if (inspos)
			*inspos = 0;
		return -1;
	}

	return bin_search_int(key, base, 0, num_elems-1, elesz, cmpf, inspos);
}
