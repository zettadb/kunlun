/*-------------------------------------------------------------------------
 *
 * utils.c
 *
 *	Helper function implementation
 *
 * Copyright (c) 2021-2022 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 *-------------------------------------------------------------------------
 */
#include "utils.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/genam.h"
#include "access/remote_meta.h"
#include "access/sysattr.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_type.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_language.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_authid.h"
#include "commands/dbcommands.h"
#include "commands/proclang.h"
#include "commands/tablespace.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/nodes.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "parser/parse_func.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/tqual.h"
#include "sharding/mysql/mysql.h"

char* escape_mysql_string(const char *from)
{
	if (!from) return NULL;
	size_t len = strlen(from);
	char *dst = palloc(len * 2 + 1);
	mysql_escape_string(dst, from, len);
	return dst;
}

void print_pg_attribute(Oid relOid, int attnum, bool justname, StringInfo str)
{
	/*
	 * Compute a default security label of the new column underlying the
	 * specified relation, and check permission to create it.
	 */
	Relation rel = relation_open(AttributeRelationId, NoLock);

	ScanKeyData skey[2];

	ScanKeyInit(&skey[0],
				Anum_pg_attribute_attrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relOid));
	ScanKeyInit(&skey[1],
				Anum_pg_attribute_attnum,
				BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(attnum));

	SysScanDesc sscan = systable_beginscan(rel, AttributeRelidNumIndexId, true,
										   SnapshotSelf, 2, &skey[0]);

	HeapTuple tuple = systable_getnext(sscan);
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "could not find tuple for column %d of relation %u",
			 attnum, relOid);

	Form_pg_attribute attForm = (Form_pg_attribute)GETSTRUCT(tuple);

	appendStringInfo(str, "%s ", attForm->attname.data);
	
	if (!justname)
	{
		build_column_data_type(str,
							   attForm->atttypid,
							   attForm->atttypmod,
							   attForm->attcollation);
		if (attForm->attnotnull)
			appendStringInfo(str, " not null");
	}

	systable_endscan(sscan);

	relation_close(rel, NoLock);
}

static
bool depend_on_temp_object_impl(Oid classid, Oid objId, Oid subId, List **travel_list)
{
	if (classid == RelationRelationId && subId == 0)
	{
		Relation rel = relation_open(objId, NoLock);
		char p = rel->rd_rel->relpersistence;
		relation_close(rel, NoLock);
		if (p == RELPERSISTENCE_TEMP)
			return true;
	}

	ScanKeyData key[3];

	/* Search the dependency table for the index */
	Relation depRel = heap_open(DependRelationId, NoLock);

	ScanKeyInit(&key[0],
							Anum_pg_depend_classid,
							BTEqualStrategyNumber, F_OIDEQ,
							ObjectIdGetDatum(classid));
	ScanKeyInit(&key[1],
							Anum_pg_depend_objid,
							BTEqualStrategyNumber, F_OIDEQ,
							ObjectIdGetDatum(objId));
	ScanKeyInit(&key[2],
							Anum_pg_depend_objsubid,
							BTEqualStrategyNumber, F_INT4EQ,
							Int32GetDatum(subId));

	SysScanDesc scan = systable_beginscan(depRel, DependDependerIndexId, true,
																				NULL, 3, key);
	HeapTuple tup;
	bool found_temp = false;
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend deprec = (Form_pg_depend)GETSTRUCT(tup);

		if (*travel_list)
		{
			ListCell *lc;
			bool found = false;
			foreach (lc, *travel_list)
			{
				ObjectAddress *obj = (ObjectAddress *)lfirst(lc);
				if (obj->classId == deprec->refclassid &&
						obj->objectId == deprec->refobjid &&
						obj->objectSubId == deprec->refobjsubid)
				{
					found = true;
					break;
				}
			}
			if (found)
				continue;
		}
		/* Remember accessed object */
		ObjectAddress *obj = (ObjectAddress*)palloc(sizeof(ObjectAddress));
		obj->classId = deprec->refclassid;
		obj->objectId = deprec->refobjid;
		obj->objectSubId = deprec->refobjsubid;
		*travel_list = lappend(*travel_list, obj);

		if ((found_temp = depend_on_temp_object_impl(deprec->refclassid, deprec->refobjid, deprec->refobjsubid, travel_list)))
			break;
	}

	systable_endscan(scan);
	relation_close(depRel, NoLock);
	return found_temp;
}

bool depend_on_temp_object(Oid classid, Oid objId, Oid subId)
{
	List *travel_list = NIL;
	bool res = depend_on_temp_object_impl(classid, objId, subId, &travel_list);
	list_free_deep(travel_list);
	return res;
}

bool check_temp_object(ObjectType objtype, List *objects, bool *allistemp)
{
	if (list_length(objects) == 0)
		return false;
	ListCell *lc;
	int num = 0;
	foreach (lc, objects)
	{
		ObjectAddress address;
		Node *object = lfirst(lc);
		Relation relation = NULL;

		/* Get an ObjectAddress for the object. */
		address = get_object_address(objtype,
									 object,
									 &relation,
									 AccessExclusiveLock,
									 true/*no error*/);

		/* Check if is a temp object */
		bool is_temp = false;
		if (relation)
		{
			is_temp = (relation->rd_rel->relpersistence == RELPERSISTENCE_TEMP);
			relation_close(relation, NoLock);
		}
		else if (OidIsValid(address.objectId))
		{
			is_temp = depend_on_temp_object(address.classId,
											address.objectId,
											address.objectSubId);
		}

		if (!is_temp)
			++num;
	}

	*allistemp = (0 == num);
	return num < list_length(objects);
}

static
Datum get_modified_rel_options(Oid relid, Oid shardid)
{
	/* Fetch heap tuple */
	HeapTuple tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);

	/* Get the old reloptions */
	bool isnull;
	Datum datum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions,
								  &isnull);

	DefElem *defElem;
	defElem = makeNode(DefElem);
	defElem->location = -1;
	defElem->defnamespace = NULL;
	defElem->defname = "shard";
	defElem->arg = makeInteger(shardid);

	/* Generate new proposed reloptions (text array) */
	Datum newOptions = transformRelOptions(isnull ? (Datum)0 : datum, list_make1(defElem),
										   NULL, NULL, false, false, NULL);
	ReleaseSysCache(tuple);

	return newOptions;
}

void change_relation_shardid(Oid relid, Oid shardid)
{
	Datum values[Natts_pg_class];
	bool nulls[Natts_pg_class]; 
	bool replaces[Natts_pg_class];

	ScanKeyData key;

	ScanKeyInit(&key,
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber,
				F_OIDEQ, relid);

	Relation pg_class_rel = relation_open(RelationRelationId, RowExclusiveLock);

	SysScanDesc scan = systable_beginscan(pg_class_rel,
										  ClassOidIndexId,
										  true,
										  NULL, 1, &key);
	HeapTuple tuple = NULL;
	if((tuple = systable_getnext(scan)) == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Kunlun-db: Faild to reset shardid of %d", relid)));
	}

	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));
	memset(replaces, 0, sizeof(replaces));
	
	replaces[Anum_pg_class_relshardid - 1] = true;
	values[Anum_pg_class_relshardid- 1] = (Datum)shardid;

	replaces[Anum_pg_class_reloptions - 1] = true;
	values[Anum_pg_class_reloptions - 1] = get_modified_rel_options(relid, shardid);

	HeapTuple newtuple = heap_modify_tuple(tuple, RelationGetDescr(pg_class_rel), values, nulls, replaces);

	CatalogTupleUpdate(pg_class_rel, &newtuple->t_self, newtuple);

	systable_endscan(scan);

	relation_close(pg_class_rel, RowExclusiveLock);

	CommandCounterIncrement();
}

/**
 * @brief Change all the relations with shardid=from to shardid=to.
 *
 *     Used to migrate the data of the entire cluster to other clusters with different shards.
 */
static void change_cluster_shardid(Oid from, Oid to)
{
	Datum values[Natts_pg_class];
	bool nulls[Natts_pg_class];
	bool replaces[Natts_pg_class];

	ScanKeyData key;

	if (from == to)
		return;

	ScanKeyInit(&key,
			ObjectIdAttributeNumber,
			BTGreaterEqualStrategyNumber,
			F_OIDGE, FirstNormalObjectId);

	Relation pg_class_rel = relation_open(RelationRelationId, RowExclusiveLock);

	SysScanDesc scan = systable_beginscan(pg_class_rel,
			ClassOidIndexId,
			true,
			NULL, 1, &key);
	HeapTuple tuple = NULL;
	Form_pg_class pg_class_tuple;
	while ((tuple = systable_getnext(scan)) != NULL)
	{
		pg_class_tuple = ((Form_pg_class)GETSTRUCT(tuple));
		if (pg_class_tuple->relshardid != from)
			continue;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));
		memset(replaces, 0, sizeof(replaces));

		replaces[Anum_pg_class_relshardid - 1] = true;
		values[Anum_pg_class_relshardid - 1] = (Datum)to;

		replaces[Anum_pg_class_reloptions - 1] = true;
		values[Anum_pg_class_reloptions - 1] = get_modified_rel_options(HeapTupleGetOid(tuple), to);

		HeapTuple newtuple = heap_modify_tuple(tuple, RelationGetDescr(pg_class_rel), values, nulls, replaces);

		CatalogTupleUpdate(pg_class_rel, &newtuple->t_self, newtuple);
	}

	systable_endscan(scan);

	relation_close(pg_class_rel, RowExclusiveLock);
}

void change_cluster_shardids(List *from, List *to)
{
	Assert(list_length(from) == list_length(to));
	ListCell *lc1, *lc2;
	forboth(lc1, from, lc2, to)
	{
		change_cluster_shardid(lfirst_oid(lc1), lfirst_oid(lc2));
	}
	CommandCounterIncrement();
}

List *
object_name_to_objectaddress(ObjectType objtype, List *objnames)
{
	List	   *objects = NIL;
	ListCell   *cell;
	ObjectAddress *address;

	Assert(objnames != NIL);

	switch (objtype)
	{
		case OBJECT_TABLE:
		case OBJECT_SEQUENCE:
			foreach(cell, objnames)
			{
				RangeVar   *relvar = (RangeVar *) lfirst(cell);
				Oid			relOid;

				relOid = RangeVarGetRelid(relvar, NoLock, false);
				address = (ObjectAddress*)palloc0(sizeof(ObjectAddress));
				address->classId = RelationRelationId;
				address->objectId = relOid;
				objects = lappend(objects, address);
			}
			break;
		case OBJECT_DATABASE:
			foreach(cell, objnames)
			{
				char	   *dbname = strVal(lfirst(cell));
				Oid			dbid;

				dbid = get_database_oid(dbname, false);
				address = (ObjectAddress*)palloc0(sizeof(ObjectAddress));
				address->classId = DatabaseRelationId;
				address->objectId = dbid;
				objects = lappend(objects, address);
			}
			break;
		case OBJECT_DOMAIN:
		case OBJECT_TYPE:
			foreach(cell, objnames)
			{
				List	   *typname = (List *) lfirst(cell);
				Oid			oid;

				oid = typenameTypeId(NULL, makeTypeNameFromNameList(typname));
				address = (ObjectAddress*)palloc0(sizeof(ObjectAddress));
				address->classId = TypeRelationId;
				address->objectId = oid;
				objects = lappend(objects, address);
			}
			break;
		case OBJECT_FUNCTION:
			foreach(cell, objnames)
			{
				ObjectWithArgs *func = (ObjectWithArgs *) lfirst(cell);
				Oid			funcid;

				funcid = LookupFuncWithArgs(OBJECT_FUNCTION, func, false);
				address = (ObjectAddress*)palloc0(sizeof(ObjectAddress));
				address->classId = ProcedureRelationId;
				address->objectId = funcid;
				objects = lappend(objects, address);
			}
			break;
		case OBJECT_LANGUAGE:
			foreach(cell, objnames)
			{
				char	   *langname = strVal(lfirst(cell));
				Oid			oid;

				oid = get_language_oid(langname, false);
				address = (ObjectAddress*)palloc0(sizeof(ObjectAddress));
				address->classId = LanguageRelationId;
				address->objectId = oid;
				objects = lappend(objects, address);
			}
			break;
		case OBJECT_LARGEOBJECT:
			foreach(cell, objnames)
			{
				Oid			lobjOid = oidparse(lfirst(cell));

				if (!LargeObjectExists(lobjOid))
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_OBJECT),
							 errmsg("large object %u does not exist",
									lobjOid)));

				address = (ObjectAddress*)palloc0(sizeof(ObjectAddress));
				address->classId = LargeObjectRelationId;
				address->objectId = lobjOid;
				objects = lappend(objects, address);
			}
			break;
		case OBJECT_SCHEMA:
			foreach(cell, objnames)
			{
				char	   *nspname = strVal(lfirst(cell));
				Oid			oid;

				oid = get_namespace_oid(nspname, false);
				address = (ObjectAddress*)palloc0(sizeof(ObjectAddress));
				address->classId = NamespaceRelationId;
				address->objectId = oid;
				objects = lappend(objects, address);
			}
			break;
		case OBJECT_PROCEDURE:
			foreach(cell, objnames)
			{
				ObjectWithArgs *func = (ObjectWithArgs *) lfirst(cell);
				Oid			procid;

				procid = LookupFuncWithArgs(OBJECT_PROCEDURE, func, false);
				address = (ObjectAddress*)palloc0(sizeof(ObjectAddress));
				address->classId = ProcedureRelationId;
				address->objectId = procid;
				objects = lappend(objects, address);
			}
			break;
		case OBJECT_ROUTINE:
			foreach(cell, objnames)
			{
				ObjectWithArgs *func = (ObjectWithArgs *) lfirst(cell);
				Oid			routid;

				routid = LookupFuncWithArgs(OBJECT_ROUTINE, func, false);
				address = (ObjectAddress*)palloc0(sizeof(ObjectAddress));
				address->classId = ProcedureRelationId;
				address->objectId = routid;
				objects = lappend(objects, address);
			}
			break;
		case OBJECT_TABLESPACE:
			foreach(cell, objnames)
			{
				char	   *spcname = strVal(lfirst(cell));
				Oid			spcoid;

				spcoid = get_tablespace_oid(spcname, false);
				address = (ObjectAddress*)palloc0(sizeof(ObjectAddress));
				address->classId = TableSpaceRelationId;
				address->objectId = spcoid;
				objects = lappend(objects, address);
			}
			break;
		case OBJECT_FDW:
			foreach(cell, objnames)
			{
				char	   *fdwname = strVal(lfirst(cell));
				Oid			fdwid = get_foreign_data_wrapper_oid(fdwname, false);

				address = (ObjectAddress*)palloc0(sizeof(ObjectAddress));
				address->classId = ForeignDataWrapperRelationId;
				address->objectId = fdwid;
				objects = lappend(objects, address);
			}
			break;
		case OBJECT_FOREIGN_SERVER:
			foreach(cell, objnames)
			{
				char	   *srvname = strVal(lfirst(cell));
				Oid			srvid = get_foreign_server_oid(srvname, false);

				address = (ObjectAddress*)palloc0(sizeof(ObjectAddress));
				address->classId = ForeignServerRelationId;
				address->objectId = srvid;
				objects = lappend(objects, address);
			}
			break;
		default:
			elog(ERROR, "unrecognized GrantStmt.objtype: %d",
				 (int) objtype);
	}

	return objects;
}

char* get_current_username()
{
	MemoryContext context = CurrentMemoryContext;
	bool freeTxn = false;
	if (!IsTransactionState())
	{
		freeTxn = true;
		StartTransactionCommand();
	}

	char *res = NULL;
	HeapTuple roletup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(GetUserId()));
	if (HeapTupleIsValid(roletup))
	{
		Form_pg_authid role_rec = (Form_pg_authid)GETSTRUCT(roletup);
		res = MemoryContextStrdup(context, NameStr(role_rec->rolname));
		ReleaseSysCache(roletup);
	}

	if (freeTxn)
		CommitTransactionCommand();
	
	return res;
}
