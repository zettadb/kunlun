/*-------------------------------------------------------------------------
 *
 * sequence_service.h
 *
 *	Kunlun sequence implementation
 *
 * Copyright (c) 2021-2022 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 *-------------------------------------------------------------------------
 */
#ifndef SEQUENCE_SERIVCE_H
#define SEQUENCE_SERIVCE_H

#include "postgres.h"

#include "utils/rel.h"

void create_remote_sequence_shmmem(void);

/**
 * @brief invalidate the sequence cache entry of given sequence 
 */
void invalidate_seq_shared_cache(Oid dbid, Oid seqrelid, bool remove);

/**
 * @brief Fetch the next sequence value of the given sequence relation
 */
int64_t remote_fetch_nextval(Relation seqrel);

/**
 * @brief Reset the current value of the given sequence relation
 */
void remote_setval(Relation seqrel, int64_t next, bool iscalled);


/**
 * @brief The main loop of the sequence serivce
 */
void sequence_serivce_main(void);

#endif
