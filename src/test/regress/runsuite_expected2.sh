#! /bin/bash

seconds="${1:-10}"
listfile="${2:-serial_schedule}"
buildtype="${3:-release}"

cat $listfile | grep -v '^#' | sed '/^[ 	]*$/d' | awk '{print $2}' | while read f; do
	if test ! -f "sql/$f.sql"; then
		echo "sql/$f.sql : No such file or directory"
	else
		echo "Running sql/$f.sql ......"
		cat "sql/$f.sql" | grep -v DDL_STATEMENT > "$f.sql"
		cp -f "$f.sql" "sql/$f.sql"
		bash run_single.sh "sql/$f.sql"
		mv 1.out.p $f.out
		test "$LOCFROM" = "" || test "$LOCTO" = "" || sed -i "s#$LOCFROM#$LOCTO#g" $f.out
		expectedout="expected/$f.out"
		case "$buildtype" in
			*[Dd][Ee][Bb][Uu][Gg]* ) test -f "expected/$f.out.debug" && expectedout="expected/$f.out.debug" ;;
			* ) ;;
		esac
		# Preprocessing the output and expected output if necessary before comparing.
		sed -i 's/shard=[0-9]*/shard=1/g' $f.out
		sed -i 's/MySQL storage node ([0-9]*, [0-9]*)/MySQL storage node (1, 1)/g' $f.out
		sed -i 's/transaction [0-9]*-[0-9]*-[0-9]*/transaction x-x-x/g' $f.out
		test -f $expectedout && sed -i 's/transaction [0-9]*-[0-9]*-[0-9]*/transaction x-x-x/g' $expectedout
		sed -i 's/at ([0-9.]*, [0-9]*)/at (x, x)/g' $f.out
		sed -i "s/on '[0-9.]*'/on 'x'/g" $f.out
		test -f $expectedout && sed -i 's/at ([0-9.]*, [0-9]*)/at (x, x)/g' $expectedout
		test -f $expectedout && sed -i "s/on '[0-9.]*'/on 'x'/g" $expectedout

		if test -f $expectedout; then
		    dos2unix "$f.out" >/dev/null 2>/dev/null
		    dos2unix $expectedout >/dev/null 2>/dev/null
		    diff "$f.out" $expectedout >/dev/null
		    ret2="$?"
		    if test "$ret2" = 0; then
			echo "EXPECTED: same with expected output - $f.sql"
		    else
			echo "UNEXPECTED: Different with expected output - $f.sql"
			echo "======= diff content with expected output - $f.sql =========="
			diff "$f.out" $expectedout
		    fi
		else
		    echo "UNEXPECTED: no expected output for $f"
		fi
		sleep $seconds
	fi
done
