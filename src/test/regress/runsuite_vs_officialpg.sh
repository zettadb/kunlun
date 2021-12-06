#! /bin/bash

cat serial_schedule | grep -v '^#'  | awk '{print $2}' | while read f; do
	if test -f "skips/$f.skip"; then
		echo "Skipping sql/$f.sql currently ......"
	else
		echo "Running sql/$f.sql ......"
		bash run.sh sql/$f.sql
		mv out.diff $f.diff
		echo "======= diff content for $f.diff =========="
		cat $f.diff
	fi
done
