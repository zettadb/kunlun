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
#include "miscadmin.h"
#include "sharding/mysql/mysql.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/inet.h"
#include "utils/builtins.h"
#include "utils/cash.h"
#include "utils/syscache.h"
#include "utils/varbit.h"
#include "utils/pg_locale.h"
#include "utils/pg_lsn.h"

#include <stdlib.h>
#define MAXPG_LSNLEN            17

extern Datum jsonb_out(PG_FUNCTION_ARGS);
extern Datum timestamptz_in(PG_FUNCTION_ARGS);
extern Datum timetz_in(PG_FUNCTION_ARGS);
extern Datum uuid_out(PG_FUNCTION_ARGS);
extern Datum xml_out(PG_FUNCTION_ARGS);

static inline char *
pstrdup_quoted(const char *str)
{
        int len;
        char *result;
        len = strlen(str);
        result = palloc(len + 3);
        result[0] = '"';
        memcpy(&result[1], str, len);
        result[len + 1] = '"';
        result[len + 2] = '\0';

        return result;
}

Datum my_timetz_in(PG_FUNCTION_ARGS)
{
        char *str = PG_GETARG_CSTRING(0);
        size_t len = strlen(str);
        char buff[len + 8];
        /*
            In the current implementation, time with time zone is always converted to time
            without time zone before writing to storage. Therefore, when reading from storage,
            we need to manually add a zone, otherwise it will not be deserialized to the pg type correctly.
          */
	snprintf(buff, len + 8, "%s+00", str);
        fcinfo->arg[0] = PointerGetDatum(buff);
        return timetz_in(fcinfo);
}

Datum my_timetz_out(PG_FUNCTION_ARGS)
{
        TimeTzADT *time = PG_GETARG_TIMETZADT_P(0);
        char *result;
        struct pg_tm tt, *tm = &tt;
        fsec_t fsec;
        int tz;
        char buf[MAXDATELEN + 1];

        /* TODO: mysql not support time zone, we delete the zone roughly.
         fix it in the future*/
        timetz2tm(time, tm, &fsec, &tz);
        EncodeTimeOnly(tm, fsec, false, 0, USE_ISO_DATES, buf);

        result = pstrdup_quoted(buf);
        PG_RETURN_CSTRING(result);
}

Datum my_timestamptz_in(PG_FUNCTION_ARGS)
{
        char *str = PG_GETARG_CSTRING(0);
        size_t len = strlen(str);
        char buff[len + 8];
        /*
            In the current implementation, time with time zone is always converted to time
            without time zone before writing to storage. Therefore, when reading from storage,
            we need to manually add a zone, otherwise it will not be deserialized to the pg type correctly.
          */
	snprintf(buff, len + 8, "%s+00", str);
        fcinfo->arg[0] = PointerGetDatum(buff);
        return timestamptz_in(fcinfo);
}

Datum my_timestamptz_out(PG_FUNCTION_ARGS)
{
        TimestampTz dt = PG_GETARG_TIMESTAMPTZ(0);
        char *result;
        int tz;
        struct pg_tm tt,
            *tm = &tt;
        fsec_t fsec;
        const char *tzn;
        char buf[MAXDATELEN + 1];

        /* TODO: mysql not support time zone, we adjust the timestamp to zone '0'. fix it in the future*/
        if (TIMESTAMP_NOT_FINITE(dt))
                EncodeSpecialTimestamp(dt, buf);
        else if (timestamp2tm(dt, &tz, tm, &fsec, &tzn, pg_tzset("GMT")) == 0)
                EncodeDateTime(tm, fsec, false,0, NULL, USE_ISO_DATES, buf);
        else
                ereport(ERROR,
                        (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                         errmsg("timestamp out of range")));

	result = psprintf("CAST('%s' as DATETIME(6))", buf);
        PG_RETURN_CSTRING(result);
}

Datum my_timestamp_out(PG_FUNCTION_ARGS)
{
        Timestamp timestamp = PG_GETARG_TIMESTAMP(0);
        char *result;
        struct pg_tm tt, *tm = &tt;
        fsec_t fsec;
        char buf[MAXDATELEN + 1];

        if (TIMESTAMP_NOT_FINITE(timestamp))
                EncodeSpecialTimestamp(timestamp, buf);
        else if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) == 0)
                EncodeDateTime(tm, fsec, false, 0, NULL, USE_ISO_DATES, buf);
        else
                ereport(ERROR,
                        (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                         errmsg("timestamp out of range")));

	result = psprintf("CAST('%s' as DATETIME(6))", buf);
        PG_RETURN_CSTRING(result);
}

Datum my_time_out(PG_FUNCTION_ARGS)
{
        TimeADT time = PG_GETARG_TIMEADT(0);
        char *result;
        struct pg_tm tt,
            *tm = &tt;
        fsec_t fsec;
        char buf[MAXDATELEN + 1];

        time2tm(time, tm, &fsec);
        EncodeTimeOnly(tm, fsec, false, 0, USE_ISO_DATES, buf);

	result = psprintf("CAST('%s' as TIME(6))", buf);
        PG_RETURN_CSTRING(result);
}

Datum my_date_out(PG_FUNCTION_ARGS)
{
        DateADT date = PG_GETARG_DATEADT(0);
        char *result;
        struct pg_tm tt,
            *tm = &tt;
        char buf[MAXDATELEN + 1];

        if (DATE_NOT_FINITE(date))
                EncodeSpecialDate(date, buf);
        else
        {
                j2date(date + POSTGRES_EPOCH_JDATE,
                       &(tm->tm_year), &(tm->tm_mon), &(tm->tm_mday));
                EncodeDateOnly(tm, USE_ISO_DATES, buf);
        }

	result = psprintf("CAST('%s' as DATE)", buf);
        PG_RETURN_CSTRING(result);
}

static char *
my_network_out(inet *src, bool is_cidr)
{
        char tmp[sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128")];
        char *dst;
        int len;

        dst = inet_net_ntop(ip_family(src), ip_addr(src), ip_bits(src),
                            tmp, sizeof(tmp));
        if (dst == NULL)
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                         errmsg("could not format inet value: %m")));

        /* For CIDR, add /n if not present */
        if (is_cidr && strchr(tmp, '/') == NULL)
        {
                len = strlen(tmp);
                snprintf(tmp + len, sizeof(tmp) - len, "/%u", ip_bits(src));
        }

        return pstrdup_quoted(tmp);
}

Datum my_inet_out(PG_FUNCTION_ARGS)
{
        inet *src = PG_GETARG_INET_PP(0);

        PG_RETURN_CSTRING(my_network_out(src, false));
}

Datum my_cidr_out(PG_FUNCTION_ARGS)
{
        inet *src = PG_GETARG_INET_PP(0);

        PG_RETURN_CSTRING(my_network_out(src, true));
}

static inline char *
quote_and_escape(const char *str)
{
        size_t len = strlen(str);
        char *dst = palloc(2 * len + 3), *p = dst;

        /* escape the string */
        *p++ = '"';
        p += mysql_escape_string(p, str, len);
        *p++ = '"';
        *p = '\0';

        return dst;
}

Datum my_cstring_out(PG_FUNCTION_ARGS)
{
        char *str = PG_GETARG_CSTRING(0);

        PG_RETURN_CSTRING(quote_and_escape(str));
}

Datum my_textout(PG_FUNCTION_ARGS)
{
        Datum txt = PG_GETARG_DATUM(0);

        char *str = TextDatumGetCString(txt);
        char *dst = quote_and_escape(str);
        /* free the unused string*/
        pfree(str);

        PG_RETURN_CSTRING(dst);
}

Datum my_bpcharout(PG_FUNCTION_ARGS)
{
        Datum txt = PG_GETARG_DATUM(0);

        char *str = TextDatumGetCString(txt);
        char *dst = quote_and_escape(str);
        pfree(str);

        PG_RETURN_CSTRING(dst);
}

Datum my_varcharout(PG_FUNCTION_ARGS)
{
        Datum txt = PG_GETARG_DATUM(0);

        char *str = TextDatumGetCString(txt);
        char *dst = quote_and_escape(str);
        pfree(str);

        PG_RETURN_CSTRING(dst);
}

Datum my_nameout(PG_FUNCTION_ARGS)
{
        Name s = PG_GETARG_NAME(0);

        PG_RETURN_CSTRING(quote_and_escape(NameStr(*s)));
}

Datum my_charout(PG_FUNCTION_ARGS)
{
        char ch = PG_GETARG_CHAR(0);
        char *result = (char *)palloc(2);

        result[0] = ch;
        result[1] = '\0';
        char *dst = quote_and_escape(result);
        pfree(result);
        PG_RETURN_CSTRING(dst);
}

Datum my_xml_out(PG_FUNCTION_ARGS)
{
        Datum str = xml_out(fcinfo);
        char *dst = quote_and_escape(DatumGetCString(str));

        PG_RETURN_CSTRING(dst);
}

Datum my_json_out(PG_FUNCTION_ARGS)
{
        /* we needn't detoast because text_to_cstring will handle that */
        Datum txt = PG_GETARG_DATUM(0);
        char *src = TextDatumGetCString(txt);
        char *dst = quote_and_escape(src);
        pfree(src);

        PG_RETURN_CSTRING(dst);
}

Datum my_jsonb_out(PG_FUNCTION_ARGS)
{
        char *out = DatumGetCString(jsonb_out(fcinfo));
        char *dst = quote_and_escape(out);
        pfree(out);

        PG_RETURN_CSTRING(dst);
}

Datum my_byteain(PG_FUNCTION_ARGS)
{
        char *inputText = PG_GETARG_CSTRING(0);
        int len = PG_GETARG_INT32(3);
        int bc;
        bytea *result;

#if 0
        /* Recognize mysql binary input */
        if (inputText[0] != '0' || inputText[1] != 'x')
        {
                bc = (len - 2) / 2 + VARHDRSZ; /* maximum possible length */
                result = (bytea*)palloc(bc);
                bc = hex_decode(inputText + 2, len - 2, VARDATA(result));
                SET_VARSIZE(result, bc + VARHDRSZ); /* actual length */
        }
        else
#endif
        {
                bc = len + VARHDRSZ;
                result = (bytea *)palloc(bc);
                SET_VARSIZE(result, bc);
                memcpy(VARDATA(result), inputText, len);
        }
        PG_RETURN_BYTEA_P(result);
}

Datum my_byteaout(PG_FUNCTION_ARGS)
{
        bytea *vlena = PG_GETARG_BYTEA_PP(0);
        char *result;
        char *rp;
        /* Print hex format */
        rp = result = palloc(VARSIZE_ANY_EXHDR(vlena) * 2 + 2 + 1);
        *rp++ = '0';
        *rp++ = 'x';
        rp += hex_encode(VARDATA_ANY(vlena), VARSIZE_ANY_EXHDR(vlena), rp);
        *rp = '\0';

        PG_RETURN_CSTRING(result);
}

Datum my_uuid_out(PG_FUNCTION_ARGS)
{
        char *str = DatumGetCString(uuid_out(fcinfo));
        char *dst = quote_and_escape(str);
        pfree(str);

        PG_RETURN_CSTRING(dst);
}

Datum my_macaddr_out(PG_FUNCTION_ARGS)
{
        macaddr *addr = PG_GETARG_MACADDR_P(0);
        char *result;

        result = (char *)palloc(32 + 2);

        snprintf(result, 32, "'%02x:%02x:%02x:%02x:%02x:%02x'",
                 addr->a, addr->b, addr->c, addr->d, addr->e, addr->f);

        PG_RETURN_CSTRING(result);
}

Datum my_macaddr8_out(PG_FUNCTION_ARGS)
{
        macaddr8 *addr = PG_GETARG_MACADDR8_P(0);
        char *result;

        result = (char *)palloc(32 + 2);

        snprintf(result, 32, "'%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x'",
                 addr->a, addr->b, addr->c, addr->d,
                 addr->e, addr->f, addr->g, addr->h);

        PG_RETURN_CSTRING(result);
}

Datum my_boolout(PG_FUNCTION_ARGS)
{
        bool b = PG_GETARG_BOOL(0);
        char *result;
        if (b)
                result = pstrdup("true");
        else
                result = pstrdup("false");

        PG_RETURN_CSTRING(result);
}

Datum my_varbit_in(PG_FUNCTION_ARGS)
{
        char *str = PG_GETARG_CSTRING(0);
        int32 atttypmod = PG_GETARG_INT32(2);
        size_t len = PG_GETARG_INT32(3);
        enum enum_field_types type = PG_GETARG_INT32(4);
        char buff[8];

        if (type == MYSQL_TYPE_LONGLONG)
        {
                len = 0;
                ulong value = strtoul(str, NULL, 10);
                for (int i = 0; value && i < 8; ++i)
                {
                        str = &buff[7 - len];
                        *str = (value & 0xff);
                        ++len;
                        value >>= 8;
                }
                if (len == 0)
                {
                        len = 1;
                        buff[0] = 0;
                        str = buff;
                }
        }

        size_t bitlen = len * 8;
        size_t varlen = VARBITTOTALLEN(bitlen);
        VarBit *result = (VarBit *)palloc0(varlen);
        SET_VARSIZE(result, varlen);

        if (atttypmod < 0)
                atttypmod = bitlen;
        VARBITLEN(result) = atttypmod = Min(bitlen, atttypmod);

        bits8 *r = VARBITS(result);
        bits8 bit1 = 1 << ((atttypmod + 7) % 8);
        bits8 bit2 = 0x80;
        int i = (bitlen - atttypmod) / 8;
        while (i < len)
        {
                if (str[i] & bit1)
                        *r |= bit2;
                bit1 >>= 1;
                bit2 >>= 1;
                if (bit1 == 0)
                {
                        bit1 = 0x80;
                        i++;
                }
                if (bit2 == 0)
                {
                        bit2 = 0x80;
                        r++;
                }
        }
        PG_RETURN_VARBIT_P(result);
}

Datum my_varbit_out(PG_FUNCTION_ARGS)
{
        VarBit *s = PG_GETARG_VARBIT_P(0);
        char *result, *r;
        bits8 *sp;
        bits8 x;
        int i, k, len;

        len = VARBITLEN(s);
        result = (char *)palloc(len + 4);
        sp = VARBITS(s);
        r = result;
        /* add prefix b' */
        *r++ = 'b';
        *r++ = '\'';
        for (i = 0; i <= len - BITS_PER_BYTE; i += BITS_PER_BYTE, sp++)
        {
                /* print full bytes */
                for (k = 0, x = *sp; k < BITS_PER_BYTE; k++)
                {
                        *r++ = IS_HIGHBIT_SET(x) ? '1' : '0';
                        x <<= 1;
                }
        }
        if (i < len)
        {
                /* print the last partial byte */
                for (k = i, x = *sp; k < len; k++)
                {
                        *r++ = IS_HIGHBIT_SET(x) ? '1' : '0';
                        x <<= 1;
                }
        }
        *r++ = '\'';
        *r = '\0';

        PG_RETURN_CSTRING(result);
}

/*
 * Append seconds and fractional seconds (if any) at *cp.
 *
 * precision is the max number of fraction digits, fillzeros says to
 * pad to two integral-seconds digits.
 *
 * Returns a pointer to the new end of string.  No NUL terminator is put
 * there; callers are responsible for NUL terminating str themselves.
 *
 * Note that any sign is stripped from the input seconds values.
 */
static char *
AppendSeconds(char *cp, int sec, fsec_t fsec, int precision, bool fillzeros)
{
        Assert(precision >= 0);

        if (fillzeros)
                cp = pg_ltostr_zeropad(cp, Abs(sec), 2);
        else
                cp = pg_ltostr(cp, Abs(sec));

        /* fsec_t is just an int32 */
        if (fsec != 0)
        {
                int32 value = Abs(fsec);
                char *end = &cp[precision + 1];
                bool gotnonzero = false;

                *cp++ = '.';

                /*
                 * Append the fractional seconds part.  Note that we don't want any
                 * trailing zeros here, so since we're building the number in reverse
                 * we'll skip appending zeros until we've output a non-zero digit.
                 */
                while (precision--)
                {
                        int32 oldval = value;
                        int32 remainder;

                        value /= 10;
                        remainder = oldval - value * 10;

                        /* check if we got a non-zero */
                        if (remainder)
                                gotnonzero = true;

                        if (gotnonzero)
                                cp[precision] = '0' + remainder;
                        else
                                end = &cp[precision];
                }

                /*
                 * If we still have a non-zero value then precision must have not been
                 * enough to print the number.  We punt the problem to pg_ltostr(),
                 * which will generate a correct answer in the minimum valid width.
                 */
                if (value)
                        return pg_ltostr(cp, Abs(fsec));

                return end;
        }
        else
                return cp;
}

Datum my_interval_out(PG_FUNCTION_ARGS)
{
        Interval *span = PG_GETARG_INTERVAL_P(0);
        char *result;
        struct pg_tm tt,
            *tm = &tt;
        fsec_t fsec;
        char buf[MAXDATELEN + 32];

        if (interval2tm(*span, tm, &fsec) != 0)
                elog(ERROR, "could not convert interval to tm");

        {
                char *cp = buf;
                int year = tm->tm_year;
                int mon = tm->tm_mon;
                int mday = tm->tm_mday;
                int hour = tm->tm_hour;
                int min = tm->tm_min;
                int sec = tm->tm_sec;
                bool has_negative = year < 0 || mon < 0 ||
                                    mday < 0 || hour < 0 ||
                                    min < 0 || sec < 0 || fsec < 0;
                bool has_positive = year > 0 || mon > 0 ||
                                    mday > 0 || hour > 0 ||
                                    min > 0 || sec > 0 || fsec > 0;
                bool has_year_month = year != 0 || mon != 0;
                bool has_day_time = mday != 0 || hour != 0 ||
                                    min != 0 || sec != 0 || fsec != 0;
                bool sql_standard_value = !(has_negative && has_positive) &&
                                          !(has_year_month && has_day_time);

                if (!sql_standard_value)
                        elog(ERROR, "Only support SQL Standard interval");

                /*
                 * SQL Standard wants only 1 "<sign>" preceding the whole
                 * interval ... but can't do that if mixed signs.
                 */
                if (has_negative)
                {
                        *cp++ = '-';
                        year = -year;
                        mon = -mon;
                        mday = -mday;
                        hour = -hour;
                        min = -min;
                        sec = -sec;
                        fsec = -fsec;
                }

                if (has_year_month)
                {
                        sprintf(cp, "interval \"%d-%d\" year_month", year, mon);
                }
                else
                {
                        sprintf(cp, "interval \"%d %d:%02d:", mday, hour, min);
                        cp += strlen(cp);
                        cp = AppendSeconds(cp, sec, fsec, MAX_INTERVAL_PRECISION, true);
                        strcpy(cp, "\" day_microsecond");
                }
                cp += strlen(cp);
        }

        result = pstrdup(buf);
        PG_RETURN_CSTRING(result);
}

Datum my_enum_out(PG_FUNCTION_ARGS)
{
        Oid enumval = PG_GETARG_OID(0);
        char *result;
        HeapTuple tup;
        Form_pg_enum en;

        tup = SearchSysCache1(ENUMOID, ObjectIdGetDatum(enumval));
        if (!HeapTupleIsValid(tup))
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                         errmsg("invalid internal value for enum: %u",
                                enumval)));
        en = (Form_pg_enum)GETSTRUCT(tup);

        result = quote_and_escape(NameStr(en->enumlabel));

        ReleaseSysCache(tup);

        PG_RETURN_CSTRING(result);
}

/* cash_out()
 * Function to convert cash to a dollars and cents representation, using
 * the lc_monetary locale's formatting.
 */
Datum my_cash_out(PG_FUNCTION_ARGS)
{
        Cash value = PG_GETARG_CASH(0);
        char buf[128];
        char *bufptr;
        int digit_pos;
        int points;
        bool neg = false;
        struct lconv *lconvert = PGLC_localeconv();

        /* see comments about frac_digits in cash_in() */
        points = lconvert->frac_digits;
        if (points < 0 || points > 10)
                points = 2; /* best guess in this case, I think */

        if (value < 0)
        {
                /* make the amount positive for digit-reconstruction loop */
                value = -value;
				neg = true;
		}

        /* we build the digits+decimal-point+sep string right-to-left in buf[] */
        bufptr = buf + sizeof(buf) - 1;
        *bufptr = '\0';

        digit_pos = points;
        do
        {
                if (points && digit_pos == 0)
                {
                        /* insert decimal point, but not if value cannot be fractional */
                        *(--bufptr) = '.';
                }

                *(--bufptr) = ((uint64)value % 10) + '0';
                value = ((uint64)value) / 10;
                digit_pos--;
        } while (value || digit_pos >= 0);

        if (neg)
                *(--bufptr) = '-';

        PG_RETURN_CSTRING(pstrdup(bufptr));
}

Datum
my_pg_lsn_in(PG_FUNCTION_ARGS)
{
   char *str = PG_GETARG_CSTRING(0);
   char *end;
   unsigned long val;

   val = strtoul(str, &end, 10);

   if (errno == ERANGE)
   {
       ereport(ERROR,
           (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
            errmsg("invalid input syntax for type %s from shard: \"%s\"",
               "pg_lsn", str)));
   }

   PG_RETURN_LSN((uint64_t)val);
}

Datum
my_pg_lsn_out(PG_FUNCTION_ARGS)
{
   XLogRecPtr lsn = PG_GETARG_LSN(0);
   char buf[MAXPG_LSNLEN + 3];
   char *result;

   snprintf(buf, sizeof buf, "0x%LX", lsn);
   result = pstrdup(buf);
   PG_RETURN_CSTRING(result);
}
