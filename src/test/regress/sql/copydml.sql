--
-- Test cases for COPY (INSERT/UPDATE/DELETE) TO
--
create table copydml_test (id serial, t text);
insert into copydml_test (t) values ('a');
insert into copydml_test (t) values ('b');
insert into copydml_test (t) values ('c');
insert into copydml_test (t) values ('d');
insert into copydml_test (t) values ('e');

--
-- Test COPY (insert/update/delete ...)
--
copy (insert into copydml_test (t) values ('f') returning id) to stdout;
copy (update copydml_test set t = 'g' where t = 'f' returning id) to stdout;
copy (delete from copydml_test where t = 'g' returning id) to stdout;

--
-- Test \copy (insert/update/delete ...)
--
\copy (insert into copydml_test (t) values ('f') returning id) to stdout;
\copy (update copydml_test set t = 'g' where t = 'f' returning id) to stdout;
\copy (delete from copydml_test where t = 'g' returning id) to stdout;

-- Error cases
copy (insert into copydml_test default values) to stdout;
copy (update copydml_test set t = 'g') to stdout;
copy (delete from copydml_test) to stdout;

copy (insert into copydml_test default values) to stdout;
copy (insert into copydml_test default values) to stdout;
copy (insert into copydml_test default values) to stdout;
copy (insert into copydml_test default values) to stdout;

copy (update copydml_test set t = 'f') to stdout;
copy (update copydml_test set t = 'f') to stdout;
copy (update copydml_test set t = 'f') to stdout;
copy (update copydml_test set t = 'f') to stdout;

copy (delete from copydml_test) to stdout;
copy (delete from copydml_test) to stdout;
copy (delete from copydml_test) to stdout;
copy (delete from copydml_test) to stdout;

copy (insert into copydml_test (t) values ('f') returning id) to stdout;
copy (update copydml_test set t = 'g' where t = 'f' returning id) to stdout;
copy (delete from copydml_test where t = 'g' returning id) to stdout;

drop table copydml_test;
