#! /bin/sh
sql="$1"

q() {
echo "$1"
exit 1
}

test "$sql" = "" &&  q "script is empty!"
test "$PGURL" = "" && q "PGURL is empty!"

psql -a -f "$sql" "$PGURL" >& 1.out
python processout.py 1.out
