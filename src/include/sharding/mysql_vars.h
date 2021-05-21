/*-------------------------------------------------------------------------
 *
 * mysql_vars.h
 *      mysql session variable caching
 *
 *
 * Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
 *
 * src/include/sharding/mysql_vars.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SHARDING_VARS_H
#define SHARDING_VARS_H

typedef struct Var_def
{
    enum Var_type {INT8, UINT8, INT4, UINT4, INT2, UINT2, INT1, UINT1, CHAR, UCHAR, BOOL, FLOAT, DOUBLE, STR} type;
    const char *var_name;
} Var_def;

#define SHORT_STR_MAX 15

typedef union Var_val
{
    int64_t i8;
    uint64_t ui8;
    int32_t i4;
    uint32_t ui4;
    int16_t i2;
    uint16_t ui2;
    int8_t i1;
    uint8_t ui1;
    char c;
    unsigned char uc;
    bool b;

    float f;
    double d;
    char str[SHORT_STR_MAX];  // 14 chars and a '\0'.
    char *pstr; // allocate in TopMemoryContext
} Var_val;

typedef struct Var_entry
{
    Var_def *var;
    /*
     * Effective when var->type is STR. if true, Var_val.str is effective,
     * otherwise Var_val.pstr is effective.
     */
    bool str_is_short;
    Var_val var_val;
} Var_entry;

#define N_VARS_SECTION 16

typedef struct Var_section
{
    Var_entry vars[N_VARS_SECTION];
    uint16_t n_used; // slots used in vars array.
    struct Var_section *next;
} Var_section;

/*
 * Init global data of this module.
 * */
extern void init_var_cache(void);


/*
 * Cache the SESSION var *after* it is sucessfully set to backend mysql instnace.
 * Do not cache GLOBAL vars.
 * */
extern void cache_var(const char *var_name, const char *var_val);

/*
 * Use cached vars to produce 'set session xxx=xx' statements in order to
 * send to new connection.
 * */
extern char *produce_set_var_stmts(int *plen);

extern Var_def *find_var_def(const char *varname);
#endif // !SHARDING_VARS_H
