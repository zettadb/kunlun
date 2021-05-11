#! /bin/bash
# configure and build Kunlun computing node program.
# If you don't need to configure it, simply do make install.
# usage: build.sh <build-type> <prefix> | install
# build-type: Release or Debug
# e.g. bash build.sh Release /home/dzw/mysql_installs/postgresql-11.5-rel
export SRCROOT=`pwd`
action="$1"

usage(){
	echo "Usage: build.sh <build-type> <prefix> | install"
	echo "	build-type: Debug | Release"
	exit 1
}

init() {
	test "$1" = "" && usage
	cd $SRCROOT
	idir="$1"
	echo "$idir" > installdir
	cd $SRCROOT/resources
	test -d mysql || tar -xzf mysql_client.tgz
	# ln -s ./libmariadb.so.3 libmariadb.so
	cd $SRCROOT/src/include/sharding
	ln -s $SRCROOT/resources/mysql mysql
	cd $SRCROOT
}

build() {
	idir="$2"
	init "$idir"
	export LIBS=-lmariadb
	export LDFLAGS=-L$SRCROOT/resources
	if test "$1" = "Release"; then
		export CFLAGS="-O2 -ggdb3"
		export CXXFLAGS="-O2 -ggdb3"
		./configure --with-openssl --with-icu --prefix="$idir"
		# ./configure --with-openssl --with-icu --with-llvm  --prefix="$idir"
	elif test "$1" = "Debug"; then
		export CFLAGS="-O0 -ggdb3 -DENABLE_DEBUG"
		export CXXFLAGS="-O0 -ggdb3 -DENABLE_DEBUG "
		./configure --with-openssl --with-icu --enable-debug --enable-cassert   --prefix="$idir"
		# ./configure --with-openssl --with-icu --with-llvm  --enable-debug --enable-cassert   --prefix="$idir"
		# in current ubuntu-20.04 latest llvm there is build error.
	else #defensive
		usage
	fi
	make
}

install() {
	test -f installdir || {
		echo "installation directory is not set, call 'build.sh build' first!"
		usage
	}
	idir=`cat installdir`
	mkdir -p "$idir"
	make install
	bash ./post-install.sh "$idir"
}

if test "$action" = "Debug" -o "$action" = "Release"; then
	test "$#" != 2 && usage
	build "$1" "$2"
elif test "$action" = "install"; then
	test "$#" != 1 && usage
	install
else
	usage
fi
