#! /bin/bash
# Copyright (c) 2019 ZettaDB inc. All rights reserved.
# This source code is licensed under Apache 2.0 License,
# combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

get_option_value() {
	str="$1"
	sub="$2"
	echo "$str" | sed "s,^$sub,,g"
}

#REMOTE_USER=""
#REMOTE_PASSWORD=""
hostfile="hosts"
blockfile="blockhosts"
hosts=""
blockhosts=""
SSHPASS="$SSHPASS"
isarg=1
parse_hosts="${parse_hosts:-1}"
parse_block="${parse_block:-1}"
clear=true
sleep=0
iteration=1000

while test "$isarg" = "1"; do
	isarg=1

	case "$1" in
		--user=*) REMOTE_USER=`get_option_value "$1" "--user="` ;;
		--password=*) REMOTE_PASSWORD=`get_option_value "$1" "--password="` ;;
		--hostfile=*) hostfile=`get_option_value "$1" "--hostfile="` ;;
		--blockfile=*) blockfile=`get_option_value "$1" "--blockfile="` ;;
		--hosts=*) hosts=`get_option_value "$1" "--hosts="` ;;
		--blockhosts=*) blockhosts=`get_option_value "$1" "--blockhosts="` ;;
		--sshpass*) SSHPASS="sshpass" ;;
		--noclear*) clear=false;;
		--sleep=*) sleep=`get_option_value "$1" "--sleep="` ;;
                --iteration=*) iteration=`get_option_value "$1" "--iteration="` ;;
		* ) isarg=0;;
	esac
	
	if test "$isarg" = "1"; then
		shift
	else
		break
	fi
done

test "$REMOTE_USER" = "" && REMOTE_USER="$LOGNAME"
test "$REMOTE_USER" = "" && REMOTE_USER="$USER"

case "$REMOTE_USER" in
	kunlun ) REMOTE_PASSWORD="kunlun";;
	* ) REMOTE_PASSWORD="Kunlun1#" ;;
esac

if test "$parse_hosts" = "1"; then
	if test "$hosts" = ""; then
		if test -f "$hostfile"; then
			hosts="`cat $hostfile|grep -v '#'`"
		else
			hosts=""
		fi
	else
		hosts="`echo $hosts | sed 's#,# #g'`"
	fi
fi

# Parsing blockhosts
if test "$parse_block" = "1"; then
	if test "$blockhosts" = ""; then
		if test -f "$blockfile"; then
			blockhosts="`cat $blockfile`"
		else
			blockhosts=""
		fi
	else
		blockhosts="`echo $blockhosts | sed 's#,# #g'`"
	fi
fi

showarg() {
	echo "REMOTE_USER: $REMOTE_USER"
	echo "REMOTE_PASSWORD: $REMOTE_PASSWORD"
	echo "hostfile: $hostfile"
	echo "blockfile: $blockfile"
	echo "hosts: $hosts"
	echo "blockhosts: $blockhosts"
	echo "SSHPASS: $SSHPASS"
	echo "other_arg: $@"
}

# showarg
