#include <sys/time.h>
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

#include "libpq-fe.h"
// gcc -o implicit_commit ./implicit_commit.c -I/home/dzw/mysql_installs/postgresql-11.5-dbg/include -L/home/dzw/mysql_installs/postgresql-11.5-dbg/lib -lpq -g

// $0 host port dbname user password
int main(int argc, char**argv)
{
	PGconn* con;
	PGresult* res;
	char buf[200];
        snprintf(buf, sizeof(buf), "host=%s port=%s dbname=%s user=%s password=%s",
                        argv[1], argv[2], argv[3], argv[4], argv[5]);
        con = PQconnectdb(buf);
	if (PQstatus(con)!= CONNECTION_OK) {
		fprintf(stderr, "Connection to database failed:%s\n", PQerrorMessage(con));
	}
	res = PQexec(con, "drop table if exists t1357 cascade; drop table if exists t1 cascade; create table t1(a serial primary key, b int); ");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        fprintf(stderr, "stmt failed: %s",
                PQerrorMessage(con));
    }
    PQclear(res);

	res = PQexec(con, "begin; insert into t1(b) values(6);");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        fprintf(stderr, "stmt failed: %s",
                PQerrorMessage(con));
    }
    PQclear(res);

	res = PQprepare(con, "abc", "create table t1357(a int)", 0, NULL);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        fprintf(stderr, "prepare stmt failed: %s",
                PQerrorMessage(con));
    }
    PQclear(res);

	res = PQexecPrepared(con, "abc", 0, NULL, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        fprintf(stderr, "exec stmt failed: %s",
                PQerrorMessage(con));
    }
    PQclear(res);

	res = PQexec(con, "set autocommit=false; insert into t1(b) values(6);commit");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        fprintf(stderr, "exec stmt failed: %s",
                PQerrorMessage(con));
    }
    PQclear(res);
}
