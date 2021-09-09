DROP TABLE if exists brintest;
CREATE TABLE brintest (byteacol bytea,
	charcol "char",
	namecol name,
	int8col bigint,
	int2col smallint,
	int4col integer,
	textcol text,
	oidcol oid,
	tidcol tid,
	float4col real,
	float8col double precision,
	macaddrcol macaddr,
	inetcol inet,
	cidrcol cidr,
	bpcharcol character,
	datecol date,
	timecol time without time zone,
	timestampcol timestamp without time zone,
	timestamptzcol timestamp with time zone,
	timetzcol time with time zone,
	bitcol bit(10),
	varbitcol bit varying(16),
	numericcol numeric,
	uuidcol uuid,
	lsncol pg_lsn
);

INSERT INTO brintest SELECT
	repeat(stringu1, 8)::bytea,
	substr(stringu1, 1, 1)::"char",
	stringu1::name, 142857 * tenthous,
	thousand,
	twothousand,
	repeat(stringu1, 8),
	unique1::oid,
	format('(%s,%s)', tenthous, twenty)::tid,
	(four + 1.0)/(hundred+1),
	odd::float8 / (tenthous + 1),
	format('%s:00:%s:00:%s:00', to_hex(odd), to_hex(even), to_hex(hundred))::macaddr,
	inet '10.2.3.4/24' + tenthous,
	cidr '10.2.3/24' + tenthous,
	substr(stringu1, 1, 1)::bpchar,
	date '1995-08-15' + tenthous,
	time '01:20:30',
	timestamp '1942-07-23 03:05:09',
	timestamptz '1972-10-10 03:00',
	timetz '01:30:20+02',
	thousand::bit(10),
	tenthous::bit(16)::varbit,
	tenthous::numeric(36,30) * fivethous * even / (hundred + 1),
	format('%s%s-%s-%s-%s-%s%s%s', to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'))::uuid,
	format('%s/%s%s', odd, even, tenthous)::pg_lsn
FROM tenk1 ORDER BY unique2 LIMIT 100;

-- throw in some NULL's and different values
INSERT INTO brintest (inetcol, cidrcol) SELECT
	inet 'fe80::6e40:8ff:fea9:8c46' + tenthous,
	cidr 'fe80::6e40:8ff:fea9:8c46' + tenthous
FROM tenk1 ORDER BY thousand, tenthous LIMIT 25;
