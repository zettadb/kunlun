/*-------------------------------------------------------------------------
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup.h"
#include "access/htup_details.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_map.h"
#include "nodes/execnodes.h"
#include "nodes/remote_input.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"

void myInputInfo(Oid typid, int typmod, TypeInputInfo *info)
{
        bool found = false;
        HeapTuple tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));

        if (HeapTupleIsValid(tup))
        {
                info->typioparam = getTypeIOParam(tup);
                info->typmod = typmod;

                /* For none-enum type, get the input func for mysql result  */
                info->typisenum = type_is_enum(typid);

                if (info->typisenum)
                {
                        info->enum_label_enties = GetAllEnumValueOidLabelSorted(typid, &info->nslots);
                        found = true;
                }
                else
                {
                        Form_pg_type typTup = (Form_pg_type)GETSTRUCT(tup);
                        
                        /* cache type category */
                        info->typcat = typTup->typcategory;

                        HeapTuple typmapTup = SearchSysCache2(TYPEMAP,
                                                              PointerGetDatum(&typTup->typname),
                                                              ObjectIdGetDatum(typTup->typnamespace));

                        if (HeapTupleIsValid(typmapTup))
			{
				bool isnull;
				info->typinput = SysCacheGetAttr(TYPEMAP,
								 typmapTup,
								 Anum_pg_type_map_myinput,
								 &isnull);
				ReleaseSysCache(typmapTup);
				if (!isnull)
				{
					found = true;
					fmgr_info(info->typinput, &info->flinfo);
					InitFunctionCallInfoData(info->fcinfo, &info->flinfo, 5, InvalidOid, NULL, NULL);
					info->fcinfo.argnull[0] = false;
					info->fcinfo.argnull[1] = false;
					info->fcinfo.argnull[2] = false;
					info->fcinfo.argnull[3] = false;
					info->fcinfo.argnull[4] = false;
				}
			}
		}

                ReleaseSysCache(tup);
        }

        if (!found)
        {
                elog(ERROR, "Could not find input function to deserialize kunlun storage result to pg type.");
        }
}

Datum myInputFuncCall(TypeInputInfo *info, char *str, int len, enum enum_field_types mytype, bool *isnull)
{
        Datum result = (Datum) 0;

        *isnull = false;
        if (str == NULL)
        {
                *isnull = true;
        }
        else if (info->typisenum)
        {
                result = GetEnumLabelOidCached(info->enum_label_enties, info->nslots, str);
        }
        else
	{
		FunctionCallInfo fcinfo = &info->fcinfo;

		fcinfo->arg[0] = CStringGetDatum(str);
		fcinfo->arg[1] = ObjectIdGetDatum(info->typioparam);
		fcinfo->arg[2] = Int32GetDatum(info->typmod);
		fcinfo->arg[3] = Int32GetDatum(len);
		fcinfo->arg[4] = Int32GetDatum(mytype);

		result = FunctionCallInvoke(fcinfo);

		/* Should get null result if and only if str is NULL */
		if (fcinfo->isnull)
			elog(ERROR, "input function %u returned NULL",
			     fcinfo->flinfo->fn_oid);
	}

	return result;
}