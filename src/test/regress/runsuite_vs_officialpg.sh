#! /bin/bash

seconds="${1:-10}"

cat serial_schedule | grep -v '^#' | sed '/^[ 	]*$/d' | awk '{print $2}' | while read f; do
	if test -f "skips/$f.skip"; then
		echo "Skipping sql/$f.sql currently ......"
	else
		echo "Running sql/$f.sql ......"
		bash run.sh sql/$f.sql
		mv out.diff $f.diff
		mv out.diff.orig $f.diff.orig
		mv 1.out $f.out1
		mv 1.out.p $f.out1.p
		mv 2.out $f.out2
		mv 2.out.p $f.out2.p
		echo "======= diff content for $f.diff =========="
		cat $f.diff
		sleep $seconds
	fi
done
