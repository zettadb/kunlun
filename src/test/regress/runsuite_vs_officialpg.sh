#! /bin/bash

seconds="${1:-10}"

cat serial_schedule | grep -v '^#' | sed '/^[ 	]*$/d' | awk '{print $2}' | while read f; do
	if test -f "skips/$f.skip"; then
		echo "Skipping sql/$f.sql currently ......"
	elif test ! -f "sql/$f.sql"; then
		echo "sql/$f.sql : No such file or directory"
	elif test ! -f "expected/$f.out"; then
		echo "expected/$f.out : No such file or directory"
	else
		echo "Running sql/$f.sql ......"
		bash run.sh sql/$f.sql
		mv out.diff $f.diff
		mv out.diff.orig $f.diff.orig
		mv 1.out $f.out1
		mv 1.out.p $f.out1.p
		mv 2.out $f.out2
		mv 2.out.p $f.out2.p
		diff "$f.out1.p" "$f.out2.p" >/dev/null
		ret1="$?"
		diff "$f.out1.p" "expected/$f.out" >/dev/null
		ret2="$?"
		if test "$ret1" = "0" -a "$ret2" = "0"; then
			echo "EXPECTED: Same with official pg and expected output"
		elif test "$ret1" = "0"; then
			echo "UNEXPECTED_RESULT: Same with official pg, but different with expected output, expected output may need to update"
		elif test "$ret2" = "0"; then
			echo "EXPECTED: Different with official pg, but same with expected output"
		else
			echo "UNEXPECTED_RESULT: Different with official pg and expected output"
			echo "======= diff content with official pg =========="
			diff "$f.out1.p" "$f.out1.p"
			echo "======= diff content with expected output =========="
			diff "$f.out1.p" "expected/$f.out"
		fi
		sleep $seconds
	fi
done
