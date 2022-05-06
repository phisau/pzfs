#!/usr/local/bin/python
import subprocess
import logging
import sys
from datetime import datetime  # strftime, datetime
import actions

now = datetime.now()
tmstmp = now.strftime("%Y%m%d%H%M")


# logger = logging.getLogger(__name__)
# config: Config


def main():
    """Run with python pzfs [send """

    if sys.platform == 'freebsd13':
        logging.basicConfig(
            format="%(asctime)s %(levelname)s %(message)s",
            filename="pzfs.log",
#            encoding="utf-8",
            level=logging.DEBUG,
            )
    else:
        logging.basicConfig(
            format="%(asctime)s %(levelname)s %(message)s",
            filename="pzfs.log",
            encoding="utf-8",
            level=logging.DEBUG,
        )
    logging.info("Started")

#    print(len(sys.argv))
#    print(str(sys.argv))

    check_cmds = ["--help", "activate", "-v", "--version", "--rc"]

    if "pzfs" in sys.argv[0] and len(sys.argv) == 1:
        skip_check = True
        print(f"use {check_cmds}")

    if "help" in sys.argv and len(sys.argv) == 2:
        #        cmd = sys.argv[sys.argv.index("help") -1]
        #        print(cmd)
        print("pzfs help function")

    if "version" in sys.argv and len(sys.argv) == 2:
        print(version())

    if sys.argv[1] == "last_snapshot":
        logging.debug("last_snapshot")
        host = sys.argv[2]
        dataset = sys.argv[3]
        print(f"Display last snapshot of {host} {dataset}")
        dataset, last_snap = actions.last_snapshot(host=host, dataset=dataset)
        #        print(last_snap)
        print(f"last_snapshot {dataset} {last_snap}")

        return f"last_snapshot {dataset} {last_snap}"

    if sys.argv[1] == "send":
        logging.debug("send start")
        host_from = sys.argv[2] 
        host_to = sys.argv[3]
        dataset_from = sys.argv[4]
        dataset_to = sys.argv[5]
        host_from_ssh = sys.argv[6]
        host_to_ssh = sys.argv[7]
        print(
                'host_from: ', host_from,
                '\n host_to: ', host_to,
                '\n dataset_from: ', dataset_from,
                '\n dataset_to: ', dataset_to,
                '\n host_from_ssh: ', host_from_ssh,
                '\n host_to_ssh: ', host_to_ssh)
        output = actions.zfs_send(
            host_from=host_from,
            host_to=host_to,
            dataset_from=dataset_from,
            dataset_to=dataset_to,
            host_from_ssh=host_from_ssh,
            host_to_ssh=host_to_ssh,
        )
        logging.debug("send end")

        return output 


    if sys.argv[1] == "create_snap":
        logging.debug("create snap start")
        host = sys.argv[2]
        dataset = sys.argv[3]
        output = actions.create_snap(host=host, dataset=dataset)
        logging.debug('create snap end')
        return output

    if sys.argv[1] == "create_dataset":
        try:
            host = sys.argv[2]
            #    dataset = 'riemann/test/test11'
            dataset = sys.argv[3]
            create_snap = actions.create_dataset(host=host, dataset=dataset)

            print(f"last_snapshot {create_snap}")
        except Exception:
            raise Exception("Error in ", host, dataset, create_snap.stderr)

    logging.info("End")


if __name__ == "__main__":
    main()
