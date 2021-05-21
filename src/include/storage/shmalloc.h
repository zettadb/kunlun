/*-------------------------------------------------------------------------
 * shmalloc.h
 *		Shared memory space management type definitions.
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * IDENTIFICATION
 *	  src/include/storage/shmalloc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SHMALLOC_H
#define SHMALLOC_H

typedef uint32_t shmoff_t;
typedef uint32_t shmsz_t;
/*
 * Global state of the shared memory space, placed at the start of the
 * shared memory space.
 * */
typedef struct Shm_space {
	shmsz_t capacity;// total capacity in bytes of this shared memory space.
	shmoff_t lfree; // offset of 1st free segment
	shmoff_t lalloc;// offset of 1st allocated segment.
	/*
	 * Alignment padding bytes.
	 *
	 * segments follow.
	 * */
} Shm_space;

Shm_space* create_shm_space(const char *name, shmsz_t capacity);
void *shm_dynalloc(Shm_space *spc, shmsz_t sz);
void shm_dynafree(void *ptr, Shm_space *spc);
bool shm_dyna_contains(void *ptr, Shm_space *spc);
#endif // SHMALLOC_H
