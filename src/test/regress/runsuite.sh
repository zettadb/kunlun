#! /bin/bash

seconds="${1:-10}"

cat serial_schedule | grep -v '^#' | sed '/^[ 	]*$/d' | awk '{print $2}' | while read f; do
	if test -f "skips/$f.skip"; then
		echo "Skipping sql/$f.sql currently ......"
	elif test ! -f "sql/$f.sql"; then
		echo "sql/$f.sql : No such file or directory"
	else
		echo "Running sql/$f.sql ......"
		cat "sql/$f.sql" | grep -v DDL_STATEMENT > "$f.sql"
		cp -f "$f.sql" "sql/$f.sql"
		bash run_single.sh "sql/$f.sql"
		mv 1.out.p $f.out
		sleep $seconds
	fi
done
