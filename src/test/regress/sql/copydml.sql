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

-- triggers
create function qqq_trig() returns trigger as $$
begin
if tg_op in ('INSERT', 'UPDATE') then
    raise notice '% %', tg_op, new.id;
    return new;
else
    raise notice '% %', tg_op, old.id;
    return old;
end if;
end
$$ language plpgsql;
create trigger qqqbef before insert or update or delete on copydml_test
    for each row execute procedure qqq_trig();
create trigger qqqaf after insert or update or delete on copydml_test
    for each row execute procedure qqq_trig();
copy (insert into copydml_test (t) values ('f') returning id) to stdout;
copy (update copydml_test set t = 'g' where t = 'f' returning id) to stdout;
copy (delete from copydml_test where t = 'g' returning id) to stdout;

drop table copydml_test;
drop function qqq_trig();						 
