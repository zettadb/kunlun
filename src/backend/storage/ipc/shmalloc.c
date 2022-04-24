/*-------------------------------------------------------------------------
 * shmalloc.c
 *		Shared memory space management.
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/shmalloc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "storage/shmalloc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include <limits.h>

#define MAX_SPACE_SIZE (256*1024*1024*1024)
// alloc a segment at least this big. only cut segement tail if left more than this.
#define MIN_SEG_SIZE  PG_CACHE_LINE_SIZE


const static shmoff_t Invalid_shmoff = -1;

typedef struct Shm_segment_hdr {
	//shmoff_t start; // segment start offset from g_shm_space->start
	shmsz_t len; // segment payload length starting after this header
	shmsz_t capacity; // segment total length starting from start, always alloc in aligned CACHELINESIZE.
	shmoff_t next; // offset of next segment. list ptr. Invalid_shmoff if no next.

	/* 
	 * MAX_ALIGN64 padding bytes.
	 *
	 * payload data bytes follow
	 * */
} Shm_segment_hdr;


static void *alloc_shmseg(Shm_space *spc, shmsz_t sz);
static int free_shmseg(void *ptr, Shm_space *spc);

static inline Shm_segment_hdr* off2ptr(shmoff_t off, Shm_space *spc) {
	if (off < 0 || off > spc->capacity) {
		Assert(false);
		return NULL;
	}
	return (Shm_segment_hdr*)((char *)spc + off);
}

static inline shmoff_t ptr2off(void *ptr, Shm_space *spc) {
	if ((char *)ptr < (char*)spc || (char *)ptr > (char*)spc + spc->capacity) {
		Assert(false);
		return Invalid_shmoff;
	}

	return (char*)ptr - (char*)spc;
}

/*
 * Allocate from shared memory, and put all space into one free segment hold by spc->lfree.
 * */
Shm_space* create_shm_space(const char *name, shmsz_t capacity)
{
	capacity = CACHELINEALIGN(capacity);

	bool found = false;

	Shm_space *spc = (Shm_space *)ShmemInitStruct(name, capacity, &found);

	if (!found)
	{
		/*
		 * We're the first - initialize.
		 */
		spc->capacity = capacity;
		spc->lfree = CACHELINEALIGN(sizeof(Shm_space));
		spc->lalloc = Invalid_shmoff;
		Shm_segment_hdr *hdr = off2ptr(spc->lfree, spc);
		hdr->capacity = capacity - CACHELINEALIGN(sizeof(Shm_space));
		hdr->next = Invalid_shmoff;
		hdr->len = 0;
	}
	return spc;
}


/*
 * Get next ajacent segment which is placed right after hdr if any.
 * */
inline static shmoff_t next_shmseg_off(Shm_space *spc, Shm_segment_hdr *hdr)
{
	return ptr2off(hdr, spc) + hdr->capacity;
}
inline static Shm_segment_hdr *next_shmseg(Shm_space *spc, Shm_segment_hdr *hdr)
{
	return off2ptr(next_shmseg_off(spc, hdr), spc);
}

void *shm_dynalloc(Shm_space *spc, shmsz_t sz)
{
	void *ret = NULL;
	LWLockAcquire(DynaShmSpaceAllocLock, LW_EXCLUSIVE);
	ret = alloc_shmseg(spc, sz);
	LWLockRelease(DynaShmSpaceAllocLock);
	return ret;
}

void shm_dynafree(void *ptr, Shm_space *spc)
{
	LWLockAcquire(DynaShmSpaceAllocLock, LW_EXCLUSIVE);
	free_shmseg(ptr, spc);
	LWLockRelease(DynaShmSpaceAllocLock);
}

/*
 * Whenever allocing, alloc one segment from free list. find the smallest free
 * segment with larger capacity than requested.
 * */
static void *alloc_shmseg(Shm_space *spc, shmsz_t sz)
{
	shmoff_t minpos = Invalid_shmoff;
	shmsz_t minsz = UINT_MAX, alloc_sz = 0;
	shmoff_t prev_minpos = Invalid_shmoff, prev_pos = Invalid_shmoff;
	Shm_segment_hdr *minhdr = NULL, *prev_minhdr = NULL, *prev_hdr = NULL;

	if (spc->lfree == Invalid_shmoff)
		return NULL;
	sz += MAXALIGN64(sizeof(Shm_segment_hdr));
	alloc_sz = CACHELINEALIGN(sz);

	for (shmoff_t pos = spc->lfree; pos != Invalid_shmoff;)
	{
				Shm_segment_hdr *hdr = off2ptr(pos, spc);
		/*
		 * Find the smallest one with bigger size than alloc_sz.
		 * If remaining capacity less than MIN_SEG_SIZE, the remaining
		 * space won't be cut out as a free segment.
		 * */
		if (hdr->capacity >= alloc_sz && minsz > hdr->capacity) {
			minhdr = hdr;
			minpos = pos;
			minsz = hdr->capacity;
			prev_minpos = prev_pos;
			prev_minhdr = prev_hdr;
			if (minsz < alloc_sz + MIN_SEG_SIZE)
				break;
		}
		prev_hdr = hdr;
		prev_pos = pos;
		pos = hdr->next;
	}

	if (minpos == Invalid_shmoff)
		return NULL;//Invalid_shmoff;

	Shm_segment_hdr *ahdr = NULL;//off2ptr(apos);
	shmoff_t apos = Invalid_shmoff;
	Assert(minpos != Invalid_shmoff && minhdr != NULL);
	if (minsz >= alloc_sz + MIN_SEG_SIZE) {
		// alloc at the tail of the free segment.
		Assert(minhdr->capacity == minsz);
		minhdr->capacity -= alloc_sz;
		apos = minpos + minhdr->capacity;
		ahdr = off2ptr(apos, spc);
	} else {
		// take minhdr segment off from free list, use it entirely.
		if (prev_minpos == Invalid_shmoff || prev_minhdr == NULL) {
			Assert(prev_minpos == Invalid_shmoff && prev_minhdr == NULL);
			Assert(spc->lfree == minpos);
			spc->lfree = minhdr->next;
		} else {
			Assert(prev_minhdr && prev_minhdr->next == minpos);
			prev_minhdr->next = minhdr->next;
		}
		apos = minpos;
		ahdr = minhdr;
	}

	ahdr->next = spc->lalloc;
	ahdr->len = 0;
	ahdr->capacity = alloc_sz;
	spc->lalloc = apos;
	return off2ptr(apos + MAXALIGN64(sizeof(Shm_segment_hdr)), spc);
}

/*
 * link sseg into spc's free list, whose free segments are sorted by starting
 * offsets. merge ajacent segments into a larger segment.
 * */
static int free_shmseg(void *ptr0, Shm_space *spc) {
	char *ptr = (char*)ptr0;
	Shm_segment_hdr *sseg = (Shm_segment_hdr *)(ptr - MAXALIGN64(sizeof(Shm_segment_hdr)));
	shmoff_t sseg_pos = ptr2off(sseg, spc);
	sseg->next = Invalid_shmoff; // take off from alloc list.
	shmoff_t sseg_next_pos = next_shmseg_off(spc, sseg);

	if (spc->lfree == Invalid_shmoff) {
		spc->lfree = sseg_pos;
		return 0;
	}

	int nmerged = 0;
	shmoff_t prev_pos = Invalid_shmoff;
	Shm_segment_hdr *prev_hdr = NULL;
	shmoff_t pos;

	for (pos = spc->lfree; pos != Invalid_shmoff;)
	{
		Shm_segment_hdr *hdr = off2ptr(pos, spc);
		shmoff_t hdr_next_pos = next_shmseg_off(spc, hdr);

		/*
		 * sseg is right after hdr, combine them into one segment.
		 * and hdr->next may be right after sseg, combine it too if so.
		 * */
		if (hdr_next_pos == sseg_pos) {
			Assert(hdr->next >= sseg_next_pos);
			hdr->capacity += sseg->capacity;
			nmerged++;
			if (hdr->next == sseg_next_pos) {
				Shm_segment_hdr *after_sseg = off2ptr(sseg_next_pos, spc);
				hdr->capacity += after_sseg->capacity;
				hdr->next = after_sseg->next;
				nmerged++;
			}
			break;
		}
		
		/*
		 * sseg is right before hdr, combine them into one segment.
		 * and fix ptrs of prev_hdr or spc->lfree.
		 * */
		if (sseg_next_pos == pos) {
			sseg->capacity += hdr->capacity;
			sseg->next = hdr->next;
			if (prev_hdr) {
				Assert(prev_hdr->next == pos);
				prev_hdr->next = sseg_pos;
			} else {
				Assert(pos == spc->lfree);
				spc->lfree = sseg_pos;
			}
			nmerged++;
			break;
		}
		
		if (sseg_pos > pos) {
			Assert(hdr_next_pos < sseg_pos);
			prev_hdr = hdr;
			prev_pos = pos;
			pos = hdr->next;
			/* 
			 * Keep looking for proper pos, make sure the free
			 * list is sorted by starting position in order to
			 * merge ajacent ones.
			 */
		} else if (sseg_pos < pos) {
			Assert(sseg_next_pos < pos);
			if (prev_pos == Invalid_shmoff) {
				Assert(prev_hdr == NULL);
				sseg->next = spc->lfree;
				spc->lfree = sseg_pos;
			} else {
				Assert(prev_hdr && prev_hdr->next == pos);
				sseg->next = pos;
				prev_hdr->next = sseg_pos;
			}
			break;
		}
	}

	if (pos == Invalid_shmoff) {
		Assert(prev_hdr);
		prev_hdr->next = sseg_pos;
	}

	return nmerged;
}

bool shm_dyna_contains(void *ptr, Shm_space *spc)
{
	return (char*)ptr >= (char*)spc && (char*)ptr < (char*)spc + spc->capacity;
}
