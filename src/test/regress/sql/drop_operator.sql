--DDL_STATEMENT_BEGIN--
CREATE OPERATOR === (
        PROCEDURE = int8eq,
        LEFTARG = bigint,
        RIGHTARG = bigint,
        COMMUTATOR = ===
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE OPERATOR !== (
        PROCEDURE = int8ne,
        LEFTARG = bigint,
        RIGHTARG = bigint,
        NEGATOR = ===,
        COMMUTATOR = !==
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP OPERATOR !==(bigint, bigint);
--DDL_STATEMENT_END--

SELECT  ctid, oprcom
FROM    pg_catalog.pg_operator fk
WHERE   oprcom != 0 AND
        NOT EXISTS(SELECT 1 FROM pg_catalog.pg_operator pk WHERE pk.oid = fk.oprcom);

SELECT  ctid, oprnegate
FROM    pg_catalog.pg_operator fk
WHERE   oprnegate != 0 AND
        NOT EXISTS(SELECT 1 FROM pg_catalog.pg_operator pk WHERE pk.oid = fk.oprnegate);
		
--DDL_STATEMENT_BEGIN--
DROP OPERATOR ===(bigint, bigint);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE OPERATOR <| (
        PROCEDURE = int8lt,
        LEFTARG = bigint,
        RIGHTARG = bigint
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE OPERATOR |> (
        PROCEDURE = int8gt,
        LEFTARG = bigint,
        RIGHTARG = bigint,
        NEGATOR = <|,
        COMMUTATOR = <|
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP OPERATOR |>(bigint, bigint);
--DDL_STATEMENT_END--

SELECT  ctid, oprcom
FROM    pg_catalog.pg_operator fk
WHERE   oprcom != 0 AND
        NOT EXISTS(SELECT 1 FROM pg_catalog.pg_operator pk WHERE pk.oid = fk.oprcom);

SELECT  ctid, oprnegate
FROM    pg_catalog.pg_operator fk
WHERE   oprnegate != 0 AND
        NOT EXISTS(SELECT 1 FROM pg_catalog.pg_operator pk WHERE pk.oid = fk.oprnegate);
		
--DDL_STATEMENT_BEGIN--
DROP OPERATOR <|(bigint, bigint);
--DDL_STATEMENT_END--