#ifndef PG_TYPE_MAP_H
#define PG_TYPE_MAP_H

#include "catalog/genbki.h"
#include "catalog/pg_type_map_d.h"

#include "catalog/objectaddress.h"
#include "nodes/nodes.h"

/* ----------------
 *		pg_type definition.  cpp turns this into
 *		typedef struct FormData_pg_type
 *
 *		Some of the values in a pg_type instance are copied into
 *		pg_attribute instances.  Some parts of Postgres use the pg_type copy,
 *		while others use the pg_attribute copy, so they must match.
 *		See struct FormData_pg_attribute for details.
 * ----------------
 */
CATALOG(pg_type_map,12470,TypeMapRelationId) BKI_SHARED_RELATION BKI_SCHEMA_MACRO BKI_WITHOUT_OIDS
{
	/* type name */
	NameData	typname;

	/* OID of namespace containing this type */
	Oid		typnamespace BKI_DEFAULT(PGNSP);

	/* Support it or not */
	bool		  enable BKI_DEFAULT(t);

	/* If shard can do coercion from/to string */
	bool		coercionfromstr	BKI_DEFAULT(t);
	bool 		coerciontostr BKI_DEFAULT(t);	

	/* in mysql text format (required) */
	regproc		myinput BKI_LOOKUP(pg_proc);
	regproc		myoutput BKI_LOOKUP(pg_proc);

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	/* The equivalent mysql type */
	text 		        mytype BKI_DEFAULT(_null_);
        text                    mycast BKI_DEFAULT(_null_);
#endif
} FormData_pg_type_map;

/* ----------------
 *		Form_pg_type corresponds to a pointer to a row with
 *		the format of pg_type relation.
 * ----------------
 */
typedef FormData_pg_type_map *Form_pg_type_map;

#endif							/* PG_TYPE_H */
