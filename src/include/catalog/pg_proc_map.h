/*-------------------------------------------------------------------------
 *
 * pg_proc_map.h
 *	  definition of the "procedure" system catalog (pg_proc)
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_proc.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PROC_MAP_H
#define PG_PROC_MAP_H

#include "catalog/genbki.h"
#include "catalog/pg_proc_map_d.h"

#include "catalog/objectaddress.h"
#include "nodes/pg_list.h"

/* ----------------
 *		pg_proc definition.  cpp turns this into
 *		typedef struct FormData_pg_proc
 * ----------------
 */
CATALOG(pg_proc_map,12551,ProcedureMapRelationId)  BKI_SHARED_RELATION BKI_SCHEMA_MACRO BKI_WITHOUT_OIDS
{
	/* procedure name */
	NameData	proname;

	/* OID of namespace containing this proc */
	Oid			pronamespace BKI_DEFAULT(PGNSP);

	/* if the map rule is valid */
	bool 		enable BKI_DEFAULT(t);

	/* parameter types (excludes OUT params) */
	oidvector	proargtypes BKI_LOOKUP(pg_type);

#ifdef CATALOG_VARLEN
	/* The format of  the equivalent mysql function */
	text		mysql	BKI_DEFAULT(_null_);
#endif
}
FormData_pg_proc_map;

/* ----------------
 *		Form_pg_proc corresponds to a pointer to a tuple with
 *		the format of pg_proc relation.
 * ----------------
 */
typedef FormData_pg_proc_map *Form_pg_proc_map;

#endif							/* PG_PROC_H */
