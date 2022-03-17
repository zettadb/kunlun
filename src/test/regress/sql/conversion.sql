--
-- create user defined conversion
--
--DDL_STATEMENT_BEGIN--
CREATE USER regress_conversion_user WITH NOCREATEDB NOCREATEROLE;
--DDL_STATEMENT_END--
SET SESSION AUTHORIZATION regress_conversion_user;
--DDL_STATEMENT_BEGIN--
CREATE CONVERSION myconv FOR 'LATIN1' TO 'UTF8' FROM iso8859_1_to_utf8;
--DDL_STATEMENT_END--
--
-- cannot make same name conversion in same schema
--
--DDL_STATEMENT_BEGIN--
CREATE CONVERSION myconv FOR 'LATIN1' TO 'UTF8' FROM iso8859_1_to_utf8;
--DDL_STATEMENT_END--
--
-- create default conversion with qualified name
--
--DDL_STATEMENT_BEGIN--
CREATE DEFAULT CONVERSION public.mydef FOR 'LATIN1' TO 'UTF8' FROM iso8859_1_to_utf8;
--DDL_STATEMENT_END--
--
-- cannot make default conversion with same schema/for_encoding/to_encoding
--
--DDL_STATEMENT_BEGIN--
CREATE DEFAULT CONVERSION public.mydef2 FOR 'LATIN1' TO 'UTF8' FROM iso8859_1_to_utf8;
--DDL_STATEMENT_END--
-- test comments
COMMENT ON CONVERSION myconv_bad IS 'foo';
COMMENT ON CONVERSION myconv IS 'bar';
COMMENT ON CONVERSION myconv IS NULL;
--
-- drop user defined conversion
--
--DDL_STATEMENT_BEGIN--
DROP CONVERSION myconv;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP CONVERSION mydef;
--DDL_STATEMENT_END--
--
-- Note: the built-in conversions are exercised in opr_sanity.sql,
-- so there's no need to do that here.
--
--
-- return to the super user
--
RESET SESSION AUTHORIZATION;
--DDL_STATEMENT_BEGIN--
DROP USER regress_conversion_user;
--DDL_STATEMENT_END--