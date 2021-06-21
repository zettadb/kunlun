# Copyright (c) 2019 ZettaDB inc. All rights reserved.
# This source code is licensed under Apache 2.0 License,
# combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

昆仑数据库集群一键工具使用说明

本说明描述了如何使用一键工具来进行集群的安装，启动，停止，以及清理动作。该工具运行于一台
Linux机器，根据指定的配置，把数据库集群的各个节点(存储节点群，计算节点群，集群管理节点）
安装到指定的目标机器上，并且搭建好集群。该工具还能停止集群，启动整个集群，以及清理整个
集群。

基本要求:
1 所有节点所在机器须为Linux, 安装了bash, sed, gzip, python2, python2-dev等工具或者库。python 2可执行程序设置为python2
2 所有集群节点所在机器已经正确设置好用户，节点将以该用户启动，该用户能够运行sudo而不需要密码。
3 对于安装存储节点的机器，需要预先安装以下库(此处为ubuntu 20.04): libncurses5 libaio-dev
4 对于安装计算节点的机器，需要预先安装以下库(此处为ubuntu 20.04): libncurses5 libicu66 python-setuptools gcc
5 对于安装动作，需要预先将二进制发布包 ( percona-8.0.18-bin-rel.tgz, postgresql-11.5-rel.tgz,
   cluster_mgr_rel.tgz ) 放入当前目录. 此外，工具运行机器和节点所在机器间网络不能太慢，因为需要将发布包传递到这些机器上。

文件布局:
当前目录下主要有以下文件:
 - 对于安装动作，需要有发布包
   percona-8.0.18-bin-rel.tgz，postgresql-11.5-rel.tgz，cluster_mgr_rel.tgz
 - 配置文件(比如install.json),
   主要用于配置节点的详细信息，包含节点所有机器，安装节点所用的用户名，以及节点特有的信息。具体格式后面详细说明。
 - 其余为工具相关的文件，使用的基本流程是，先根据配置文件，产生实际运行的shell脚本，而后运行该脚本即可完成动作。

基本用法:
  python2 generate_scripts.py action=install|stop|start|clean config=config_file [defuser=user_to_be_used] [defbase=basedir_to_be_used]
  bash $action/commands.sh   # 其中$action=install|stop|start|clean

对于generate_scripts.py而言, defuser和defbase为可选参数。

示例:
1 安装集群:
  python2 generate_scripts.py action=install config=install.json defuser=kunlun
  bash install/commands.sh
2 停止集群:
  python2 generate_scripts.py action=stop config=install.json defbase=.
  bash stop/commands.sh
3 启动集群:
  python2 generate_scripts.py action=start config=install.json
  bash start/commands.sh
4 清理集群(停止集群，并删除所有安装的节点)
  python2 generate_scripts.py action=clean config=install.json
  bash clean/commands.sh

说明:
该工具集使用一个python脚本 'generate_scripts.py' 和一个json格式的配置文件来产生实际的安装命令序列(commands.sh),
而后运行这些命令序列即可以完成指定的动作。

参数 action=动作，指定需要指定的动作，为install, stop, start, clean四种之一

参数 config=文件，指定配置文件。

参数 'defuser=user_to_be_used'
设置节点所在机器所使用的默认用户名，如果机器设置(machines，见后)中没有用户名的设置，则该默认用户名将被使用。
如果该选项没有设置，则默认用户名为运行脚本所在机器的当前用户名。

参数 'defbase=basedir_to_be_used'
设置集群节点的所在机器的默认工作目录。如果工作目录没有在机器中设置(machines, 见后)的话，该默认目录将被使用。
如果该默认目录没有在命令行中指定，则'/kunlun'将作为默认目录。该目录将用于存放发布包，解压后的发布包，
以及一些配置文件和辅助脚本文件等。

配置文件说明:

对于不同的动作，可以允许配置文件内容有所不同，但一般都使用install动作的配置文件。由于目录结构的因素，要求start/stop/clean
操作的集群，也是使用该工具的install操作产生。

配置文件分为两大部分，可选的machines部分，和cluster部分。
* machines用来设置节点所在机器的信息，主要用来设置机器上的默认工作目录, 使用的默认用户名。
* cluster则用来设置集群的信息。集群信息分为五部分
  - name: 集群名字，一般使用字母和数字的组合
  - meta: 元数据集群的信息
  - comp: 计算节点集的信息
  - data: 数据节点集的信息
  - clustermgr: 管理节点的信息(只需要一个)
* 元数据集群为一个复制组，一主多备，内部含有3个或3个以上的存储节点。
* 数据节点集为多个复制组，一个复制组即为一个数据分片。每个复制组内部含有3个或3个以上存储节点。
* 计算节点集为一到多个计算节点，是客户端的接入点。具体数取决于需要的接入点数目。

对于每个存储节点，基于mysql-8.0.18开发， 一般需要以下信息:
* is_primary:是否为复制组中的初始主节点，仅install需要
* ip: 节点所在机器的ip
* port: mysql port
* xport: mysql xport，仅install需要
* mgr_port: 用于mysql group replication通信的节点，仅install需要
* innodb_buffer_pool_size: innodb的buffer pool大小，测试环境可以小一点，生产环境一般需要大一些。仅install需要
* data_dir_path: mysql数据目录，仅install需要
* log_dir_path: mysql binlog，服务器日志等的存放位置，仅install需要
* user: 运行mysql服务器进程的用户，一般应当与machines里面的对应条目使用相同的值，仅install需要
* election_weight: mysql group replication的选举权重。一般50即可，仅install需要

对于每个计算节点，基于postgresql-11.5开发，一般需要以下信息:
* id: 每个节点需不用，一般从1开始，仅install需要。
* name: 名称, 每个节点需不同，参照例子即可，仅install需要。
* ip: 节点所在机器的IP，用于客户端连接
* port: 端口，用于客户端连接
* user: 用户名，用于客户端连接，仅install需要。
* password: 密码，用于客户端连接，仅install需要。
* datadir: 节点的安装目录，用于存放节点数据。仅install需要。

对于集群管理节点，只需要一个信息:
* ip: 节点所在机器的IP

具体配置可以参照示例:install.json.

集群安装或者启动后，可以用以下方式来测试连接和冒烟测试:
kunlun@kunlun-test:cluster$ psql -f smokeTest.sql postgres://kunlun:Tx1Df2Mn#@192.168.0.199:5401/postgres
其中用户名，密码，ip，端口需要改为对应的内容。
