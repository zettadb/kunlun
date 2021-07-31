# Installation Guides for Kunlun Distributed Database Cluster

This document helps users to install kunlun distributed DBMS cluster.
For more information, resources, documentation of Kunlun distributed RDBMS, please visit www.zettadb.com.

One could obtain kunlun software by building Kunlun modules from source or download the binaries or docker images from www.zettadb.com.

To build computing node program from source, use build.sh directly or refer to it for instructions.
To build kunlun-storage from source, see kunlun-storage/INSTALL.kunlun.md for instructions.
To build cluster_mgr from source, see cluster_mgr/README.md for instructions.

Unzip the downloaded Kunlun-xx.xx.tgz file into a path, which is named Kunlun in this doc, i.e. Kunlun=`pwd`. If the computing node is built from source, the 'Kunlun' path is the installation path. Then follow the steps below, one after another.

## I. Prerequisites

Install these software first. Below statement supposes this file is in $Kunlun. 
   
0. Linux running on a X86 architecture, and python2.7.
1. Kunlun-Storage and cluster_mgr which are both available in the binary Kunlun software package or can be built from source.
2. MySQL python connector, provided in $Kunlun/resources/mysql-connector-python-2.1.3.tar.gz
3. PostgreSQL python connector, provided in $Kunlun/resources/psycopg2-2.8.4.tar.gz
4. Set PATH to include kunlun-storage/bin so that the 'mysql' client program can be found by shell and python;
   and set LD_LIBRARY_PATH to include $Kunlun/lib: export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$Kunlun/lib

### Library dependencies

If you are using a kunlun computing node program built from source on the same Linux distribution and version as where it's being used, simply skip this step because there is no dependency issues.

All dynamic shared objects (*.so files) that programs in $Kunlun/bin depend on, are provided in $Kunlun/lib/deps directory. Try startup postgres (e.g. postgres --version) and see if your local Linux distro needs any of the provided *.so files. If so, copy the needed ones into $Kunlun/lib.

DO NOT copy everything in deps into lib at once, otherwise your linux OS or any software may not be able to work because of library version mismatches!

## II. Kunlun Installation Procedures

Follow below steps one by one to install a Kunlun distributed DBMS cluster.

### Installing Meta-data MySQL Cluster

A meta data cluster is a mysql binlog replication cluster that stores one or more Kunlun clusters' meta data. Users are required to use Kunlun-Storage for premium performance and reliability.

This step is only needed if you don't yet have a meta-data cluster to use. Multiple Kunlun clusters can share the same meta data cluster.

Install mysql instances of the meta-data cluster one after another using the install script and config template in Kunlun-Storage/dba_tools. And create a user for other components of the cluster to connect to the metadata cluster. The installation script in Kunlun-Storage already creates such a user 'pgx'. 

The meta data cluster must be running during the installation and when any Kunlun cluster is running.

Store the connection info of all mysql instances of the meta data cluster in a config file of the same format as the template file in $Kunlun/scripts/meta-shard.json. In this doc we name the config file 'my-meta.json'. The config file my-meta.json will be used in following steps.

#### Metadata Cluster Config File Explained

The connection info of the meta data cluster should be stored in a json file of below format. Its format and meaning is explained here. The json file contains an array of one or more config objects, each object contains basic info about a Kunlun-Storage database instance. All these instances form the metadata cluster.

[
   {
      "ip": "127.0.0.1",    IP of the computer server on which the db instance runs
      "port": 4001,         the db instance is listening on this port
      "user": "abc",        user name and password to connect to the db instance.
      "password":"abc"
   },
   more config objects for other db instances of the meta data cluster.
]

### Installing Storage Shards

Install storage shards of the distributed database cluster, and create a user in each shard for other components of the cluster to connect to each of the shards. The installation script in Kunlun-Storage already creates such a user 'pgx'. Store their connection info in a config file of same format as $Kunlun/scripts/shards-config.json. In this file we name such a file 'my-shards.json'.

A storage shard and a meta-data mysql binlog replication cluster consists of one mysql primary node and N mysql replica nodes. Users are required to use Kunlun's dedicated Kunlun-Storage component, which contains fixes to all known mysql-8.0 XA bugs. Without such fixes, Kunlun clusters will not be crash safe and may lose committed transactions in the event of various hardware/software/network failures. Also, kunlun-storage contains certain features required by Kunlun's computing nodes.

In Kunlun-Storage/dba_tools, there are scripts and configuration template file with recomended settings for users to install such an instance with premium configurations. Refer to the docs Kunlun-Storage/INSTALL.kunlun.md to install db instances.

All the storage shards listed in the config file must be running during the installation otherwise installation will fail.

#### Storage Shard Config File Explained

The connection info of a storage shard should be stored in a json file of below format. Its format and meaning is explained here. The json file contains an array of one or more shard config objects, each object contains one shard's configs. In a shard's config, there are config objects of one or more db instances of the shard. 

[
{
   "shard_name": "shard1",          name of the shard. storage shard names must be unique across a Kunlun cluster.
   "shard_nodes":
   [
       {
          "ip": "127.0.0.1",        IP of the computer server on which the db instance runs
          "port": 4101,             the db instance is listening on this port
          "user": "pgx",            user name and password to connect to the db instance.
          "password":"pgx_pwd"
       },
       more objects of db instances in shard1
   ]
}
, more shards' config objects
]


### Installing Computing Nodes

Install computing nodes of Kunlun distributed database cluster using "KunLun", i.e. this piece of software, using the script in $Kunlun/scripts/install_pg.py
One needs to prepare a config file using the template in $Kunlun/scripts/comp-nodes.json. In this file we name it 'my-comps.json', and it will be used in next steps too. When you fill my-comps.json properly, run install_pg.py using the filled config file:

`python install_pg.py config=my-comps.json install_ids=1,2,3`

Note that the `install_ids` argument specifies the computing nodes that one wants to install on current server, by its ID in the config file. It can be 'all', to install all computing nodes specified in the config file into current server; Or it can be a comma seperated list of computing IDs to only install such nodes here. If not specified, all computing nodes will be installed.

The install_pg.py will create the users you specified in the config file, this user will be used for cluster installation/uninstallation only. And you can create more users and assign proper priviledges to them. Refer to PostgreSQL documentation on how to do this.

To start a PostgreSQL or Kunlun process, use the script in $Kunlun/scripts/start_pg.py:

`python start_pg.py port=5401`

To shut it down, do:

`$Kunlun/bin/pg_ctl stop -D computing-node-datadir`

All the computing nodes listed in the config file must be running during the installation otherwise installation will fail.

#### Computing Node Config File Explained

The connection info of computing nodes should be stored in a json file of below format. Its format and meaning is explained here. The json file contains an array of one or more config objects, each object contains basic info about a computing node.

[
   {
      "id":1,               ID of the node. must be unique in a Kunlun cluster.
      "name":"comp1",       name of the node. must be unique in a Kunlun cluster.
      "ip":"127.0.0.1",     IP of the computer server on which the computing node runs
      "port":5401,          the computing node is listening on this port
      "user":"abc",         user name and password to connect to the computing node
      "password":"abc",
      "datadir":"/data/pg_data_dir1"    data directory of the node, the path must be a full path, i.e. starting with / . Only used when installing the computing node db instance. the target directory must be empty or non-existent.
   }
   , more config objects for other computing node of the cluster
]

 
## III. Bootstrapping

When meta data cluster, computing nodes and storage shards are all installed, we can bootstrap the meta data cluster, i.e. to create meta data tables and stored procedures in it.
Note that only bootstrap for a newly installed KunLun database cluster if its meta-data cluster is not initialized yet. The same meta data cluster can be used for multiple Kunlun clusters, and users only need to do this step once for the 1st Kunlun cluster.

`python bootstrap.py --config=./my-meta.json --bootstrap_sql=./meta_inuse.sql `


## IV. Initialize distrbuted database cluster

Note that the server where you run below script must be appended into the pg_hba.conf of every computing nodes first. See section VII. to do this.

Simply execute below command to do so.
`python create_cluster.py --shards_config ./my-shards.json --comps_config ./my-comps.json  --meta_config ./my-meta.json --cluster_name clust1 --cluster_owner abc --cluster_biz test `

Now the cluster is installed properly and ready for use. You can startup the "Kunlun" processes for each computing node, and the mysqld processes for each storage shards and meta-data cluster, and connect to one of the computing nodes to interact with the Kunlun distributed database cluster.

If some time later you want to add more storage shards or computing nodes to the cluster, do the following:


## V. Add more shards to an existing cluster:

Note that the server where you run below script must be appended into the pg_hba.conf of every computing nodes of current Kunlun cluster first. See section VII. to do this.

Install more storage shards as above step #2 of preparation phase, then do below to add them into the Kunlun cluster 'clust1'.

You can add the new computing nodes in the old config file to keep all configs in one file, and do somethig like:

`python add_shards.py --config ./my-shards.json --meta_config ./my-meta.json --cluster_name clust1 --targets=shard3,shard4`

Here targets=shard3,shard4 specifies the names of the new shards to install.

Or you can store new shards' configs in a new config file and do:

`python add_shards.py --config ./my-more-shards.json --meta_config ./my-meta.json --cluster_name clust1 `



## VI. Add more computing nodes to an existing cluster:

Note that the server where you run below script must be appended into the pg_hba.conf of the computing nodes to add first. See section VII. to do this.

Install computing nodes as above step #3 of preparation phase, then do below to add them into the Kunlun cluster 'clust1'.
You can add the new computing nodes in the old config file to keep all configs in one file, and do somethig like:

`python add_comp_nodes.py --config ./my-comps.json --meta_config ./my-meta.json --cluster_name clust1 --targets=3,4`

Here targets=3,4 specifies the IDs of the new computing nodes to install.

Or you can store new nodes' configs in a new config file and do:

`python add_comp_nodes.py --config ./my-more-comps.json --meta_config ./my-meta.json --cluster_name clust1 `

## VII. Connecting to computing  nodes from other servers

According to PostgreSQL, we have to add an entry for every computer server (ip-address, user-name, dbname) combination in order to connect to the PostgreSQL db instance from that server using the specified user name to the target database. After you finish the editing, do below command to make the changes effective.

`pg_ctl reload -D /db/datadir/path` 
