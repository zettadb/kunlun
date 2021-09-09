#! /bin/sh
sql="$1"

q() {
echo "$1"
exit 1
}

test "$sql" = "" &&  q "script is empty!"
test "$PGURL" = "" && q "PGURL is empty!"
test "$PGURL2" = "" && q "PGURL2 is empty!"

psql -a -f $sql $PGURL >& 1.out
psql -a -f $sql $PGURL2 >&  2.out
python processout.py 1.out
python processout.py 2.out

diff 1.out 2.out > out.diff.orig
diff 1.out.p 2.out.p > out.diff
