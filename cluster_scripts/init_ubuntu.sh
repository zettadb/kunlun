#! /bin/bash
# Copyright (c) 2019 ZettaDB inc. All rights reserved.
# This source code is licensed under Apache 2.0 License,
# combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

echo "deb http://archive.ubuntu.com/ubuntu/ xenial-updates main restricted" >> /etc/apt/sources.list

# Kunlun installation
apt-get update && apt-get install -y apt-utils python2 python2-dev libncurses5 libicu55 locales python-setuptools gcc g++

localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8 && locale

ln -s /usr/bin/python2 /usr/bin/python

echo "export LANG=en_US.utf8" >> /etc/profile

# For xtrabackup:
apt-get install -y curl lsb-release gnupg gnupg-l10n gnupg-utils

# For easy usage:
apt-get install -y net-tools iputils-ping sshpass

# For docker
apt-get install -y docker.io
