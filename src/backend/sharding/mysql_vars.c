/*-------------------------------------------------------------------------
 *
 * mysql_vars.c
 *		routines to cache mysql session variables and generate
 *		'set var' statements.
 *
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 *
 * IDENTIFICATION
 *	  src/backend/sharding/mysql_vars.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "sharding/mysql_vars.h"
#include "utils/memutils.h"
#include "lib/stringinfo.h"
#include <limits.h> // ULONG_MAX
#include <math.h> // HUGE_VALF, HUGE_VALL

static Var_def mysql_variable_defs[] =
{
	{ UINT8, "max_heap_table_size"},
	{ UINT8, "tmp_table_size"},
	{ UINT8, "long_query_time"},
	{ BOOL, "end_markers_in_json"},
	{ BOOL, "windowing_use_high_precision"},
	{ STR, "optimizer_switch"},
	{ STR, "optimizer_trace"},           ///< bitmap to tune optimizer tracing
	{ STR, "optimizer_trace_features"},  ///< bitmap to select features to trace
	{ INT8, "optimizer_trace_offset"},
	{ INT8, "optimizer_trace_limit"},
	{ UINT8, "optimizer_trace_max_mem_size"},
	{ STR, " sql_mode"},  ///< which non-standard SQL behaviour should be enabled
	{ STR, "option_bits"},  ///< OPTION_xxx constants, e.g. OPTION_PROFILING
	{ UINT8, "select_limit"},
	{ UINT8, "max_join_size"},
	{ UINT8, "auto_increment_increment"},
	{ UINT8, "auto_increment_offset"},
	{ UINT8, "bulk_insert_buff_size"},
	{ UINT4, "eq_range_index_dive_limit"},
	{ UINT4, "cte_max_recursion_depth"},
	{ UINT8, "histogram_generation_max_mem_size"},
	{ UINT8, "join_buff_size"},
	{ UINT8, "lock_wait_timeout"},
	{ UINT8, "max_allowed_packet"},
	{ UINT8, "max_error_count"},
	{ UINT8, "max_length_for_sort_data"},
	{ UINT8, "max_points_in_geometry"},
	{ UINT8, "max_sort_length"},
	{ UINT8, "max_insert_delayed_threads"},
	{ UINT8, "min_examined_row_limit"},
	{ UINT8, "net_buffer_length"},
	{ UINT8, "net_interactive_timeout"},
	{ UINT8, "net_read_timeout"},
	{ UINT8, "net_retry_count"},
	{ UINT8, "net_wait_timeout"},
	{ UINT8, "net_write_timeout"},
	{ UINT8, "optimizer_prune_level"},
	{ UINT8, "optimizer_search_depth"},
	{ UINT8, "parser_max_mem_size"},
	{ UINT8, "range_optimizer_max_mem_size"},
	{ UINT8, "preload_buff_size"},
	{ UINT8, "profiling_history_size"},
	{ UINT8, "read_buff_size"},
	{ UINT8, "read_rnd_buff_size"},
	{ UINT8, "div_precincrement"},
	{ UINT8, "sortbuff_size"},
	{ UINT8, "max_sp_recursion_depth"},
	{ UINT8, "default_week_format"},
	{ UINT8, "max_seeks_for_key"},
	{ UINT8, "range_alloc_block_size"},
	{ UINT8, "query_alloc_block_size"},
	{ UINT8, "query_prealloc_size"},
	{ UINT8, "trans_alloc_block_size"},
	{ UINT8, "trans_prealloc_size"},
	{ UINT8, "group_concat_max_len"},
	{ STR, "binlog_format"},  ///< binlog format for this thd (see enum_binlog_format)
	{ STR, "rbr_exec_mode"},  // see enum_rbr_exec_mode
	{ BOOL, "binlog_direct_non_trans_update"},
	{ STR, "binlog_row_image"},  // see enum_binlog_row_image
	{ STR, "binlog_row_value_options"},
	{ BOOL, "sql_log_bin"},
	{ STR, "transaction_write_set_extraction"},
	{ UINT8, "completion_type"},
	{ STR, "transaction_isolation"},
	{ UINT8, "updatable_views_with_limit"},
	{ UINT4, "max_user_connections"},
	{ UINT8, "my_aes_mode"},
	{ UINT8, "ssl_fips_mode"},
	{ UINT8, "resultset_metadata"},
	{ UINT4, "pseudo_thread_id"},
	{ BOOL, "transaction_read_only"},
	{ BOOL, "low_priority_updates"},
	{ BOOL, "new_mode"},
	{ BOOL, "keep_files_on_create"},
	{ BOOL, "old_alter_table"},
	{ BOOL, "big_tables"},
	{ STR, "character_set_filesystem"},
	{ STR, "character_set_client"},
	{ STR, "character_set_results"},
	{ STR, "collation_server"},
	{ STR, "collation_database"},
	{ STR, "collation_connection"},
	{ STR, "lc_messages"},
	{ STR, "lc_time_names"},
	{ STR, "time_zone"},
	{ BOOL, "explicit_defaults_for_timestamp"},
	{ BOOL, "sysdate_is_now"},
	{ BOOL, "binlog_rows_query_log_events"},
	{ UINT8, "log_slow_rate_limit"},
	{ STR, "log_slow_filter"},
	{ STR, "log_slow_verbosity"},
	{ UINT8, "innodb_io_reads"},
	{ UINT8, "innodb_io_read"},
	{ UINT8, "innodb_io_reads_wait_timer"},
	{ UINT8, "innodb_lock_que_wait_timer"},
	{ UINT8, "innodb_innodb_que_wait_timer"},
	{ UINT8, "innodb_page_access"},
	{ DOUBLE, "long_query_time_double"},
	{ BOOL, "pseudo_slave_mode"},
	{ STR, "gtid_next"},
	{ STR, "gtid_next_list"},
	{ STR, "session_track_gtids"},  // see enum_session_track_gtids
	{ UINT8, "max_execution_time"},
	{ BOOL, "session_track_schema"},
	{ BOOL, "session_track_state_change"},
	{ BOOL, "expand_fast_index_creation"},
	{ UINT4, "threadpool_high_prio_tickets"},
	{ UINT8, "threadpool_high_prio_mode"},
	{ STR, "session_track_transaction_info"},
	{ UINT8, "information_schema_stats_expiry"},
	{ BOOL, "show_create_table_verbosity"},
	{ BOOL, "show_old_temporals"},
	{ BOOL, "ft_query_extra_word_chars"},
	{ UINT8, "original_commit_timestamp"},
	{ STR, "internal_tmp_mem_storage_engine"},  // enum_internal_tmp_mem_storage_engine
	{ STR, "default_collation_for_utf8mb4"},
	{ STR, "use_secondary_engine"},
	{ STR, "group_replication_consistency"},
	{ STR, "debug_sync"},
	{ STR, "debug"},
	{ BOOL, "sql_require_primary_key"},
	{ UINT4, "original_server_version"},
	{ UINT4, "immediate_server_version"},
	{ UINT4, "binlog_stmt_cache_size"},
	{ UINT4, "bulk_insert_buffer_size"},
	{ UINT4, "innodb_lock_wait_timeout"},
	{ BOOL, "general_log"},
	{ UINT4, "innodb_buffer_pool_size"},
	{ BOOL, "slow_log"},
	{ UINT4, "innodb_change_buffer_max_size"}
};

#define ARRAY_LEN(arr) (sizeof(arr)/sizeof(arr[0]))

static int var_def_cmp(const void *a, const void *b)
{
	Var_def *v1 = (Var_def *)a;
	Var_def *v2 = (Var_def *)b;
	return strcmp(v1->var_name, v2->var_name);
}

Var_def *find_var_def(const char *varname)
{
	static bool is_sorted = false;
	if (!is_sorted)
	{
		qsort(mysql_variable_defs, ARRAY_LEN(mysql_variable_defs),
			  sizeof(Var_def), var_def_cmp);
		is_sorted = true;
	}

	Var_def key;
	key.var_name = varname;
	
	Var_def *var = (Var_def *)bsearch(&key, mysql_variable_defs,  
		ARRAY_LEN(mysql_variable_defs), sizeof(Var_def), var_def_cmp);
	return var;
}

static Var_section all_cached_vars;
static Var_section *last_section = NULL;
void init_var_cache()
{
	last_section = &all_cached_vars;
}


static Var_entry *find_cached_var(const char *varname)
{
	Var_section *s = &all_cached_vars;
	while (s != NULL)
	{
		for (int i = 0; i < ARRAY_LEN(s->vars) && i < s->n_used; i++)
		{
			Var_def *def = s->vars[i].var;
			if (def && strcmp(def->var_name, varname) == 0)
				return &(s->vars[i]);
		}
		s = s->next;
	}

	return NULL;
}

static Var_entry *alloc_var_entry()
{
	if (last_section->n_used >= N_VARS_SECTION)
	{
		Var_section *psect = (Var_section*)MemoryContextAllocZero(TopMemoryContext, sizeof(Var_section));
		last_section->next = psect;
		last_section = psect;
	}
	return &(last_section->vars[last_section->n_used++]);
}


/*
 * Translate boolean string 's' to bool value, 'pres' brings back result if s
 * is a valid boolean value, and return true; otherwise return false and *pres
 * is intact.
 * */
static bool str_to_bool(const char *s, bool *pres)
{
	static const char *positives[] = {"true", "on", "yes"};
	static const char *negs[] = {"false", "off", "no"};
	for (int i = 0; i < ARRAY_LEN(positives); i++)
	{
		if (strcasecmp(positives[i], s) == 0)
		{
			*pres = true;
			return true;
		}
	}

	for (int i = 0; i < ARRAY_LEN(negs); i++)
	{
		if (strcasecmp(negs[i], s) == 0)
		{
			*pres = false;
			return true;
		}
	}
	return false;
}

static void var_val_check_assign_uint(Var_entry *ve, uint64_t val)
{
	const static uint32_t ui4_max = 0xffffffff;
	const static uint16_t ui2_max = 0xffff;
	const static uint16_t ui1_max = 0xff;

	if ((ve->var->type == UINT4 && val > ui4_max) ||
		(ve->var->type == UINT2 && val > ui2_max) ||
		(ve->var->type == UINT1 && val > ui1_max) ||
		(ve->var->type == UCHAR && val > ui1_max))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
	    			 errmsg("Kunlun-db: Variable \"%s\" given out of range numeric value %lu.", ve->var->var_name, val)));
	switch (ve->var->type)
	{
	  case UINT8:
		ve->var_val.ui8 = val;
		break;
	  case UINT4:
		ve->var_val.ui4 = (uint32_t)val;
		break;
	  case UINT2:
		ve->var_val.ui2 = (uint16_t)val;
		break;
	  case UINT1:
		ve->var_val.ui1 = (uint8_t)val;
		break;
	  case UCHAR:
		ve->var_val.uc = (unsigned char)val;
		break;
	  default:
		Assert(false);
	}
}

static void var_val_check_assign_int(Var_entry *ve, int64_t val)
{
	const static int32_t i4_max = 0x7fffffff;
	const static int16_t i2_max = 0x7fff;
	const static int16_t i1_max = 0x7f;

	if ((ve->var->type == INT4 && val > i4_max) ||
		(ve->var->type == INT2 && val > i2_max) ||
		(ve->var->type == INT1 && val > i1_max) ||
		(ve->var->type == CHAR && val > i1_max))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
	    			 errmsg("Kunlun-db: Variable \"%s\" given out of range numeric value %ld.", ve->var->var_name, val)));
	switch (ve->var->type)
	{
	  case INT8:
		ve->var_val.i8 = val;
		break;
	  case INT4:
		ve->var_val.i4 = (int32_t)val;
		break;
	  case INT2:
		ve->var_val.i2 = (int16_t)val;
		break;
	  case INT1:
		ve->var_val.i1 = (int8_t)val;
		break;
	  case CHAR:
		ve->var_val.c = (char)val;
		break;
	  default:
		Assert(false);
	}
}


/*
 * Cache the var *after* it is sucessfully set to backend mysql instnace.
 * Find from cache, if found, update existing entry; if none, add new entry.
 * */
void cache_var(const char *var_name, const char *var_val)
{
	Var_def *def = find_var_def(var_name);
	if (!def)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_NAME),
				 errmsg("Kunlun-db: Variable \"%s\" is not a mysql session variable.", var_name)));
		return;
	}

	/* don't cache this var, it's effective only once in mysql after each set&trigger.*/
	if (strcasecmp(var_name, "debug_sync") == 0)
		return;

	Var_entry *ve = find_cached_var(var_name);
	if (!ve)
	{
		ve = alloc_var_entry();
		ve->var = def;
	}

	uint64_t uval;
	int64_t val;
	char *endptr = NULL;
	size_t vlen = 0;
	bool isok = false;
	double dval;

	switch (def->type)
	{
	  case STR:
		vlen = strlen(var_val) + 1;
		if (vlen < SHORT_STR_MAX)
		{
			ve->str_is_short = true;
			char *pend = stpncpy(ve->var_val.str, var_val, SHORT_STR_MAX);
			*pend = '\0';
		}
		else if (vlen < 1024*1024)
		{
			ve->str_is_short = false;
			char *pstr = (char *)MemoryContextAllocZero(TopMemoryContext, vlen);
			char *pend = stpncpy(pstr, var_val, vlen);
			*pend = '\0';
			ve->var_val.pstr = pstr;
		}
		else
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Kunlun-db: Variable \"%s\" given invalid string value: string longer than 1MB is not allowed.", var_name)));
		break;
	  case BOOL:
		isok = str_to_bool(var_val, &ve->var_val.b);
		if (!isok)
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Kunlun-db: Variable \"%s\" given invalid boolean value: %s.", var_name, var_val)));
		break;
	  case UINT8:
	  case UINT4:
	  case UINT2:
	  case UINT1:
	  case UCHAR:
		uval = strtoull(var_val, &endptr, 10);
		if ((errno == ERANGE && uval == ULONG_MAX) || (errno != 0 && uval == 0))
		{
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
	    			 errmsg("Kunlun-db: Variable \"%s\" given invalid numeric value %s: out of range", var_name, var_val)));
		}

		if (endptr == var_val)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
	    			 errmsg("Kunlun-db: Variable \"%s\" given invalid numeric value %s.", var_name, var_val)));
		}
		var_val_check_assign_uint(ve, uval);
		break;
	  case INT8:
	  case INT4:
	  case INT2:
	  case INT1:
	  case CHAR:
		val = strtoll(var_val, &endptr, 10);
		if ((errno == ERANGE && (val == LONG_MAX || val == LONG_MIN))
				|| (errno != 0 && val == 0))
		{
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
	    			 errmsg("Kunlun-db: Variable \"%s\" given invalid numeric value %s: out of range", var_name, var_val)));
		}

		if (endptr == var_val)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
	    			 errmsg("Kunlun-db: Variable \"%s\" given invalid numeric value %s.", var_name, var_val)));
		}
		var_val_check_assign_int(ve, val);
		break;
	  case FLOAT:
		Assert(false);  // So far no such mysql session var.
		break;
	  case DOUBLE:
		dval = strtod(var_val, &endptr);
		if ((errno == ERANGE &&
			 (dval == HUGE_VALF || dval == HUGE_VALL || dval == 0)) ||
			(errno != 0 && dval == 0))
		{
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
	    			 errmsg("Kunlun-db: Variable \"%s\" given invalid numeric value %s: out of range", var_name, var_val)));
		}

		if (endptr == var_val)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
	    			 errmsg("Kunlun-db: Variable \"%s\" given invalid numeric value %s.", var_name, var_val)));
		}
		ve->var_val.d = dval;

		break;
	}
}


/*
 * Use cached vars to produce 'set session xxx=xx' statements in order to
 * send to new connection.
 * returned memory is owned by caller and caller should pfree it after use.
 * */
char *produce_set_var_stmts(int *plen)
{
	Var_section *s = &all_cached_vars;
	static StringInfoData set_var_buf;
	const char *delim = NULL;
	int ret;

	Assert(plen);

	if (!set_var_buf.data)
	{
		initStringInfo2(&set_var_buf, 256, TopMemoryContext);
	}
	else
		set_var_buf.len = 0;

	while (s != NULL)
	{
		for (int i = 0; i < ARRAY_LEN(s->vars) && i < s->n_used; i++)
		{
			Var_entry *ve = s->vars + i;
			Var_def *def = ve->var;

			if (def->type == STR)
				delim = "'";
			else
				delim = "";
			ret = appendStringInfo(&set_var_buf, "%sset %s = %s",
				set_var_buf.len  > 0 ? ";" : "", def->var_name, delim);

			switch (def->type)
			{
			  case UINT8:
				ret = appendStringInfo(&set_var_buf, "%lu", ve->var_val.ui8);
				break;
			  case UINT4:
				ret = appendStringInfo(&set_var_buf, "%u", ve->var_val.ui4);
				break;
			  case UINT2:
				ret = appendStringInfo(&set_var_buf, "%u", ve->var_val.ui2);
				break;
			  case UINT1:
				ret = appendStringInfo(&set_var_buf, "%u", ve->var_val.ui1);
				break;
			  case INT8:
				ret = appendStringInfo(&set_var_buf, "%ld", ve->var_val.i8);
				break;
			  case INT4:
				ret = appendStringInfo(&set_var_buf, "%d", ve->var_val.i4);
				break;
			  case INT2:
				ret = appendStringInfo(&set_var_buf, "%d", ve->var_val.i2);
				break;
			  case INT1:
				ret = appendStringInfo(&set_var_buf, "%d", ve->var_val.i1);
				break;
			  case UCHAR:
				ret = appendStringInfo(&set_var_buf, "%c", ve->var_val.uc);
				break;
			  case CHAR:
				ret = appendStringInfo(&set_var_buf, "%c", ve->var_val.c);
				break;
			  case STR:
				ret = appendStringInfo(&set_var_buf, "%s", ve->str_is_short ? ve->var_val.str : ve->var_val.pstr);
				break;
			  case FLOAT:
				ret = appendStringInfo(&set_var_buf, "%f", ve->var_val.f);
				break;
			  case DOUBLE:
				ret = appendStringInfo(&set_var_buf, "%g", ve->var_val.d);
				break;
			  case BOOL:
				ret = appendStringInfo(&set_var_buf, "%s", ve->var_val.b ? "true" : "false");
				break;
			  default:
				Assert(false);
				break;
			}
			appendStringInfoString(&set_var_buf, delim);
		}
		s = s->next;
	}

	*plen = lengthStringInfo(&set_var_buf);
	if (*plen == 0)
		return NULL;

	return donateStringInfo(&set_var_buf);
}

