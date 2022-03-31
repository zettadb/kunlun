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
		bash run.sh "sql/$f.sql"
		mv out.diff $f.diff
		mv out.diff.orig $f.diff.orig
		mv 1.out $f.out1
		mv 1.out.p $f.out1.p
		mv 2.out $f.out2
		mv 2.out.p $f.out2.p
		echo "======= diff content with official pg =========="
		diff "$f.out1.p" "$f.out2.p"
		sleep $seconds
	fi
done
