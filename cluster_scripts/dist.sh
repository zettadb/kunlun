#! /bin/bash
# Copyright (c) 2019 ZettaDB inc. All rights reserved.
# This source code is licensed under Apache 2.0 License,
# combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

script_dir="`dirname $0`"
. $script_dir/common.sh

# Use \$host to represent hostip or hostname value.

from="$1"
to="$2"

ffail=0
for hostitem in $hosts; do
	ufail=0
        host="${hostitem%:*}"
        hname="${hostitem#*:}"


	echo "=========== [`date`] transfer ($from) to $to on $host($hname) ==========="
        if `ping -c 2 $host >/dev/null 2>/dev/null`; then
                :
        else
                echo "Unable to connect $host($hname) !"
                continue
        fi

	if test "$SSHPASS" = ""; then
		eval scp -r $from $REMOTE_USER@$host:$to || ufail=1
	else
		eval sshpass -p "$REMOTE_PASSWORD" scp -r $from $REMOTE_USER@$host:$to || ufail=1
	fi

	if test "$ufail" = "1"; then
		ffail=1
		echo "!!!FAILURES!!!"
	fi

done

exit $ffail
