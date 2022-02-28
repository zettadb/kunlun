# Copyright (c) 2019 ZettaDB inc. All rights reserved.
# This source code is licensed under Apache 2.0 License,
# combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

import sys
import os
#arg: port
if __name__ == "__main__":
    #args = dict([arg.split('=') for arg in sys.argv[1:]])
    #port = args["port"]
    parser = argparse.ArgumentParser(description='install the computing node')
    parser.add_argument('--port', type=str, help="Port of the Computing node", required=True)
    args = parser.parse_args()
    port = args.port

    install_path = os.getcwd()[:-8] # cut off the /scripts tail

    # open the instances_list to look for target instance's datadir
    etc_path = install_path + "/etc"
    conf_list_file = etc_path+"/instances_list.txt"
    if not os.path.exists(conf_list_file):
        raise Exception("instance list file " + conf_list_file + " not exist!")

    # sequentially find target port's datadir from instances_list file
    fp_conf_list = open(conf_list_file, 'r')
    lines = fp_conf_list.readlines()
    datadir = ''
    for line in lines:
        if '==>' in line:
            cfg = line.split('==>')
            if cfg[0] == port:
                datadir = cfg[1][:-1]
                break

    fp_conf_list.close()


    pg_logfp = datadir + "/logfile-" + port

    cmd0 = "export LD_LIBRARY_PATH=" + install_path + "/lib:$LD_LIBRARY_PATH;"
    cmd1 = 'export LD_PRELOAD="' + install_path + '/resources/libjemalloc.so.3.6.0"; ulimit -c unlimited; '
    startup_cmd = cmd0 + cmd1 + install_path + '/bin/postgres -D ' + datadir + " > " + pg_logfp + " 2>&1 &"
    os.system(startup_cmd)
