#! /bin/bash
# configure and build Kunlun computing node program.
# If you don't need to configure it, simply do make install in ./src.
# usage: build.sh <build-type> <install-dir>
# build-type: Release or Debug
# e.g. bash build.sh Release /home/dzw/mysql_installs/postgresql-11.5-rel
if test $# != 2 || test $1 != "Debug" -a $1 != "Release"; then
	echo "Usage: build.sh <build-type> <install-dir> \n\tbuild-type: Release or Debug"
	exit 1
fi

mkdir -p $2

export SRCROOT=`pwd`
cd $SRCROOT/resources
test -d mysql || tar -xzf mysql_client.tgz
ln -s ./libmariadb.so.3 libmariadb.so
cd $SRCROOT/src/include/sharding
ln -s $SRCROOT/resources/mysql mysql
cd $SRCROOT

export LIBS=-lmariadb
export LDFLAGS=-L$SRCROOT/resources

if test $1 = "Release"; then
	export CFLAGS="-O2 -ggdb3"
	export CXXFLAGS="-O2 -ggdb3"
	./configure --with-openssl --with-icu --prefix=$2 
	# ./configure --with-openssl --with-icu --with-llvm  --prefix=$2
elif test $1 = "Debug"; then

	export CFLAGS="-O0 -ggdb3 -DENABLE_DEBUG"
	export CXXFLAGS="-O0 -ggdb3 -DENABLE_DEBUG "
	./configure --with-openssl --with-icu --enable-debug --enable-cassert   --prefix=$2
	# ./configure --with-openssl --with-icu --with-llvm  --enable-debug --enable-cassert   --prefix=$2
	# in current ubuntu-20.04 latest llvm there is build error.
fi

cd $SRCROOT/src
make clean
make install -j8 2> err.txt
cd $SRCROOT
bash ./post-install.sh $2
