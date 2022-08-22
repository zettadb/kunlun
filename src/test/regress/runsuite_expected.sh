#! /bin/bash

seconds="${1:-10}"
listfile="${2:-serial_schedule}"

cat $listfile | grep -v '^#' | sed '/^[ 	]*$/d' | awk '{print $2}' | while read f; do
	if test -f "skips/$f.skip"; then
		echo "Skipping sql/$f.sql currently ......"
	elif test ! -f "sql/$f.sql"; then
		echo "sql/$f.sql : No such file or directory"
	else
		echo "Running sql/$f.sql ......"
		cat "sql/$f.sql" | grep -v DDL_STATEMENT > "$f.sql"
		cp -f "$f.sql" "sql/$f.sql"
		bash run_compare.sh "sql/$f.sql"
		mv out.diff $f.diff
		mv out.diff.orig $f.diff.orig
		mv 1.out $f.out1
		mv 1.out.p $f.out1.p
		mv 2.out $f.out2
		mv 2.out.p $f.out2.p
		diff "$f.out1.p" "$f.out2.p" >/dev/null
		ret1="$?"
		if test "$ret1" = "0"; then
			# it is same with official pg, no need to do other things.
			echo "EXPECTED: Same with official pg"
		else
			if test -f "expected/$f.out"; then
				diff "$f.out1.p" "expected/$f.out" >/dev/null
				ret2="$?"
				if test "$ret2" = 0; then
					echo "EXPECTED: Different with official pg, but same with expected output"
				else
					echo "UNEXPECTED: Different with official pg and expected output"
					echo "======= diff content with expected output =========="
					diff "$f.out1.p" "expected/$f.out"
				fi
			else
				echo "UNEXPECTED: Different with official pg, no expected output"
				echo "======= diff content with official pg =========="
				diff "$f.out1.p" "$f.out2.p"
			fi

		fi
		sleep $seconds
	fi
done
