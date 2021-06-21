#! /bin/bash
# Copyright (c) 2019 ZettaDB inc. All rights reserved.
# This source code is licensed under Apache 2.0 License,
# combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

script_dir="`dirname $0`"
. $script_dir/common.sh

tmpscript="/tmp/tmprun_$$.sh"

hostitem="$1"
shift
cmd="$@"

ufail=0
ffail=0

host="${hostitem%:*}"
hname="${hostitem#*:}"

echo "=========== [`date`] execute ($cmd) on $host($hname) ==========="
if `ping -c 2 $host >/dev/null 2>/dev/null`; then
        :
else
        echo "Unable to connect $host($hname) !"
        exit 1
fi

echo "$cmd" > $tmpscript
sed -i "s#HOST_IP_ADDR#$host#g" $tmpscript
sed -i "s#HOST_NAME#$hname#g" "$tmpscript"

if test "$SSHPASS" = ""; then
	scp $tmpscript $REMOTE_USER@$host:/tmp
	ssh $REMOTE_USER@$host "bash $tmpscript" < /dev/null || ufail=1
	test "$clear" = "true" && ssh $REMOTE_USER@$host "rm -f $tmpscript"
else
	sshpass -p "$REMOTE_PASSWORD" scp $tmpscript $REMOTE_USER@$host:/tmp
	sshpass -p "$REMOTE_PASSWORD" ssh $REMOTE_USER@$host "bash $tmpscript" < /dev/null || ufail=1
	test "$clear" = "true" && sshpass -p "$REMOTE_PASSWORD" ssh $REMOTE_USER@$host "rm -f $tmpscript"
fi

if test "$ufail" = "1"; then
	ffail=1
	echo "!!!FAILURES!!!"
fi

rm -f $tmpscript
exit $ffail
