/*-------------------------------------------------------------------------
 *
 * format_type_remote.c
 *	  Map to remote DBMS types.
 *
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/format_type_remote.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <ctype.h>

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_map.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/syscache.h"
#include <stdlib.h>

/*
  mysql requires text/blob key fields to have length suffix, and we
  map several pg types to such mysql types.
*/
bool needs_mysql_keypart_len(Oid typid, int typmod)
{
	static Oid txt_types [] = {
		TEXTOID,
		BYTEAOID,
		XMLOID
	};

	for (int i = 0; i < sizeof(txt_types)/sizeof(Oid); i++)
		if (typid == txt_types[i] || (VARCHAROID == typid && typmod == -1))
			return true;

	return false;
}

bool type_is_enum_lite(Oid typid)
{
	return (type_is_enum(typid));
}

const char *format_type_remote(Oid typoid)
{
	HeapTuple tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typoid));
	const char *szmytype = NULL;
	if (HeapTupleIsValid(tp))
	{
		Form_pg_type typtup = (Form_pg_type)GETSTRUCT(tp);
		/* Find the coresponding mysql type */
		HeapTuple tp2 = SearchSysCache2(TYPEMAP,
						PointerGetDatum(&typtup->typname),
						ObjectIdGetDatum(typtup->typnamespace));

		if (HeapTupleIsValid(tp2))
		{
			Datum mytype;
			bool isnull = true;

			/* If the type is enabled, get the corresponding mysql type */
			if (((Form_pg_type_map)GETSTRUCT(tp2))->enable)
				mytype = SysCacheGetAttr(TYPEMAP, tp2, Anum_pg_type_map_mytype, &isnull);

			if (!isnull)
			{
				szmytype = TextDatumGetCString(mytype);
			}
			ReleaseSysCache(tp2);
		}
		else if (typtup->typcategory == TYPCATEGORY_ENUM)
		{
			szmytype = get_enum_type_mysql(typoid);
		}
		else if (typtup->typtype == TYPTYPE_DOMAIN && typtup->typbasetype != InvalidOid)
		{
			szmytype = format_type_remote(typtup->typbasetype);
		}
		ReleaseSysCache(tp);
	}
	return szmytype;
}

/* 
 * Whether a const value of type 'type_oid' needs to be quoted by single quote(').
 * @retval 1: needs quote; 0: don't need quote; -1:unknown type, not decided.
 * */
int const_output_needs_quote(Oid typoid)
{
	bool p;
	char cat;

	get_type_category_preferred(typoid, &cat, &p);
	if (cat == TYPCATEGORY_BOOLEAN ||
	    cat == TYPCATEGORY_NUMERIC ||
	    cat == TYPCATEGORY_ARRAY)
		return 0;
	return 1;
}

bool is_string_type(Oid typid)
{
	bool p;
	char cat;

	get_type_category_preferred(typid, &cat, &p);
	return (cat == TYPCATEGORY_STRING);
}


const char* mysql_can_cast(Oid typid)
{
	HeapTuple tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
	const char *szcast = NULL;
	if (HeapTupleIsValid(tp))
	{
		Form_pg_type typtup = (Form_pg_type)GETSTRUCT(tp);
		/* Find the coresponding mysql type */
		HeapTuple tp2 = SearchSysCache2(TYPEMAP,
						PointerGetDatum(&typtup->typname),
						ObjectIdGetDatum(typtup->typnamespace));

		if (HeapTupleIsValid(tp2))
		{
			bool isnull;
			Datum cast = SysCacheGetAttr(TYPEMAP, tp2, Anum_pg_type_map_mycast, &isnull);
			if (isnull == false)
				szcast = TextDatumGetCString(cast);

			ReleaseSysCache(tp2);
		}
		ReleaseSysCache(tp);
	}

	return szcast;
}
