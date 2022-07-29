#! /bin/bash

seconds="${1:-10}"

cat serial_schedule | grep -v '^#' | sed '/^[ 	]*$/d' | awk '{print $2}' | while read f; do
	if test ! -f "sql/$f.sql"; then
		echo "sql/$f.sql : No such file or directory"
	else
		echo "Running sql/$f.sql ......"
		cat "sql/$f.sql" | grep -v DDL_STATEMENT > "$f.sql"
		cp -f "$f.sql" "sql/$f.sql"
		bash run_single.sh "sql/$f.sql"
		mv 1.out.p $f.out
		test "$LOCFROM" = "" || test "$LOCTO" = "" || sed -i "s#$LOCFROM#$LOCTO#g" $f.out
		# Preprocessing the output and expected output if necessary before comparing.
		sed -i 's/shard=[0-9]*/shard=1/g' $f.out
		sed -i 's/MySQL storage node ([0-9]*, [0-9]*)/MySQL storage node (1, 1)/g' $f.out
		sed -i 's/transaction [0-9]*-[0-9]*-[0-9]*/transaction x-x-x/g' $f.out
		test -f "expected/$f.out" && sed -i 's/transaction [0-9]*-[0-9]*-[0-9]*/transaction x-x-x/g' expected/$f.out
		sed -i 's/at ([0-9.]*, [0-9]*)/at (x, x)/g' $f.out
		sed -i "s/on '[0-9.]*'/on 'x'/g" $f.out
		test -f "expected/$f.out" && sed -i 's/at ([0-9.]*, [0-9]*)/at (x, x)/g' expected/$f.out
		test -f "expected/$f.out" && sed -i "s/on '[0-9.]*'/on 'x'/g" expected/$f.out

		if test -f "expected/$f.out"; then
		    diff "$f.out" "expected/$f.out" >/dev/null
		    ret2="$?"
		    if test "$ret2" = 0; then
			echo "EXPECTED: same with expected output"
		    else
			echo "UNEXPECTED: Different with expected output"
			echo "======= diff content with expected output =========="
			diff "$f.out" "expected/$f.out"
		    fi
		else
		    echo "UNEXPECTED: no expected output for $f"
		fi
		sleep $seconds
	fi
done
