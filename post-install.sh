if [ $# -ne 1 ]
then
    echo "usage: ./post-install.sh <installation-dir-full-path>"
    exit 0;
fi

cp -f ReleaseNotes.md INSTALL.kunlun.md README.md README_CN.md  $1
mkdir -p $1/scripts
cd scripts/
cp -f add_comp_nodes.py add_comp_self.py add_shards.py  bootstrap.py  common.py install_pg.py start_pg.py create_cluster.py \
	meta_inuse.sql comp-nodes.json shards-config.json  meta-shard.json $1/scripts
cp -fr ../resources $1
builddir="`pwd`"
cd $1
test ! -e lib && test -d lib64 && ln -s lib64 lib
test ! -e lib && test -d lib32 && ln -s lib32 lib
cd lib
cp -f $builddir/../resources/libmariadb.so.3 .
#cp -f $builddir/../resources/libssl.so.1.1 deps
#cp -f $builddir/../resources/libcrypto.so.1.1 deps
rm -f libmysqlclient.so && ln -s ./libmariadb.so.3  libmysqlclient.so
rm -f libmariadb.so && ln -s ./libmariadb.so.3  libmariadb.so

#copy dependent shared object libs to lib/deps
mkdir -p $1/lib/deps
export LD_LIBRARY_PATH=$1/lib:$LD_LIBRARY_PATH
cd $1/bin
rm -f ./prog-deps.txt
ls | grep -v 'prog-deps.txt' | xargs ldd >> ./prog-deps.txt 2>/dev/null
cat ./prog-deps.txt | sed -n '/^.* => .*$/p' | sed  's/^.* => \(.*\)(.*$/\1/g' | sort | uniq | sed /^.*postgresql-11\.5.*$/d | sed '/^ *$/d' |  while read f ; do
	echo "install $f to lib/deps"
	cp -f $f $1/lib/deps
done
rm ./prog-deps.txt

