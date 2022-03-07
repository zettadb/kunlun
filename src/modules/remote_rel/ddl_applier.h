/*-------------------------------------------------------------------------
 *
 * ddl_applier.h
 *
 * Copyright (c) 2021-2022 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 *-------------------------------------------------------------------------
 */

#ifndef DDL_APPLIER_H
#define DDL_APPLIER_H

/**
 * @brief Creates an application message queue used by the postmaster 
 *   to notify the application service which database will be dropped
 */
extern void create_applier_message_queue(bool module_init);

/**
 * @brief Put the Oid of the database which will be dropped by the postmaster
 * into the message queue, to notify the ddl applier release all the connections
 * to that database.
 */
extern bool notify_applier_dropped_database(Oid dbid);

/**
 * @brief The main loop of the ddl applier service
 */
extern void ddl_applier_serivce_main(void);

#endif
