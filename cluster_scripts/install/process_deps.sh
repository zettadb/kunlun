#! /bin/bash
# Copyright (c) 2019 ZettaDB inc. All rights reserved.
# This source code is licensed under Apache 2.0 License,
# combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

tries=10
find ../bin -type f | while read f; do
	cur=0
	while test "$cur" -lt "$tries"; do
		ldd $f 2>/dev/null | grep 'not found' >&/dev/null || break
		ldd $f 2>/dev/null | grep 'not found' |  sed "s#$f:##g" | sed 's#: # #g' | awk '{print $1}' | while read libf; do
               		cp deps/`basename $libf` .
        	done
		let cur++
	done
done

