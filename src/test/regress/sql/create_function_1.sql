--
-- CREATE_FUNCTION_1
--/home/kunlun/kunlun/src/test/regress

CREATE FUNCTION widget_in(cstring)
   RETURNS widget
   AS '//home/kunlun/kunlun/src/test/regress/regress.so'
   LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION widget_out(widget)
   RETURNS cstring
   AS '//home/kunlun/kunlun/src/test/regress/regress.so'
   LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION int44in(cstring)
   RETURNS city_budget
   AS '//home/kunlun/kunlun/src/test/regress/regress.so'
   LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION int44out(city_budget)
   RETURNS cstring
   AS '//home/kunlun/kunlun/src/test/regress/regress.so'
   LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION check_primary_key ()
	RETURNS trigger
	AS '//home/kunlun/kunlun/src/test/regress/refint.so'
	LANGUAGE C;

CREATE FUNCTION check_foreign_key ()
	RETURNS trigger
	AS '//home/kunlun/kunlun/src/test/regress/refint.so'
	LANGUAGE C;

CREATE FUNCTION autoinc ()
	RETURNS trigger
	AS '//home/kunlun/kunlun/src/test/regress/autoinc.so'
	LANGUAGE C;

CREATE FUNCTION trigger_return_old ()
        RETURNS trigger
        AS '//home/kunlun/kunlun/src/test/regress/regress.so'
        LANGUAGE C;

CREATE FUNCTION ttdummy ()
        RETURNS trigger
        AS '//home/kunlun/kunlun/src/test/regress/regress.so'
        LANGUAGE C;

CREATE FUNCTION set_ttdummy (int4)
        RETURNS int4
        AS '/home/kunlun/kunlun/src/test/regress/regress.so'
        LANGUAGE C STRICT;

CREATE FUNCTION make_tuple_indirect (record)
        RETURNS record
        AS '//home/kunlun/kunlun/src/test/regress/regress.so'
        LANGUAGE C STRICT;

CREATE FUNCTION test_atomic_ops()
    RETURNS bool
    AS '//home/kunlun/kunlun/src/test/regress/regress.so'
    LANGUAGE C;

-- Tests creating a FDW handler
CREATE FUNCTION test_fdw_handler()
    RETURNS fdw_handler
    AS '//home/kunlun/kunlun/src/test/regress/regress.so', 'test_fdw_handler'
    LANGUAGE C;

-- Things that shouldn't work:

CREATE FUNCTION test1 (int) RETURNS int LANGUAGE SQL
    AS 'SELECT ''not an integer'';';

CREATE FUNCTION test1 (int) RETURNS int LANGUAGE SQL
    AS 'not even SQL';

CREATE FUNCTION test1 (int) RETURNS int LANGUAGE SQL
    AS 'SELECT 1, 2, 3;';

CREATE FUNCTION test1 (int) RETURNS int LANGUAGE SQL
    AS 'SELECT $2;';

CREATE FUNCTION test1 (int) RETURNS int LANGUAGE SQL
    AS 'a', 'b';

CREATE FUNCTION test1 (int) RETURNS int LANGUAGE C
    AS 'nosuchfile';

CREATE FUNCTION test1 (int) RETURNS int LANGUAGE C
    AS '//home/kunlun/kunlun/src/test/regress/regress.so', 'nosuchsymbol';

CREATE FUNCTION test1 (int) RETURNS int LANGUAGE internal
    AS 'nosuch';
