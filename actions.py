#!/usr/local/bin/python
import subprocess
import logging
import sys
from datetime import datetime  # strftime, datetime

now = datetime.now()
tmstmp = now.strftime("%Y%m%d%H%M")


# logger = logging.getLogger(__name__)
# config: Config


def version():
    logging.debug("version check")
    return "Version 1"


def verify_output(output):
    if output.returncode != 0:
        logging.warning(f"{output}")
        raise Exception(output)
    logging.debug(f"{output}")
    return output

def create_snap(
    host: str = None, dataset: str = "riemann/test"
):  # -> Callable[[str],int, str, str]:
    """Creates a snapshot of the near side and takes the mapping table's first row"""
    snapshot = dataset + "@" + tmstmp

    if host is None or host == 'localhost':
        zfs_create = ["zfs", "snapshot", snapshot]
        output = subprocess.run(
            zfs_create,
            capture_output=True,
        )
    else:
        zfs_create = ["ssh", host, "zfs", "snapshot", snapshot]
        output = subprocess.run(
            zfs_create,
            capture_output=True,
        )
    verify_output(output)

    return output


def last_snapshot(host: str = None, dataset: str = None) -> (str, str):
    """returns last snapshots of dataset: (dataset, snapshot)"""

    if host is None or host == "localhost":
        zfs_last_snap = [
            "zfs",
            "list",
            "-t",
            "snapshot",
            "-o",
            "name",
            "-S",
            "creation",
            "-H",
            dataset,
        ]
    else:
        zfs_last_snap = [
            "ssh",
            host,
            "zfs",
            "list",
            "-t",
            "snapshot",
            "-o",
            "name",
            "-S",
            "creation",
            "-H",
            dataset,
        ]

    output = subprocess.run(zfs_last_snap, capture_output=True)
    output = verify_output(output)
    try:
        first_row = output.stdout.decode().split("\n")[0]
        dataset, snapshot = first_row.split("@")
        return dataset, snapshot
    except ValueError:  # if no snapshot exists, then return None for snapshot
        logging.INFO("ValueError in {host, dataset} has no snapshot")
        print(f"ValueError in {host, dataset} has no snapshot")
        return dataset, None
    else:
        raise Exception(f"Error in dataset {output}")


def create_dataset(
    host: str = "localhost", dataset: str = None
):  # -> Callable[[str],int, str, str]:
    """Creates a snapshot of the near side and takes the mapping table's first row"""

    if host is None or host == "localhost":
        zfs_create = ["zfs", "create", dataset]
        output = subprocess.run(
            zfs_create,
            capture_output=True,
        )
    else:
        zfs_create = ["ssh", host, "zfs", "create", dataset]
        output = subprocess.run(
            zfs_create,
            capture_output=True,
        )
    verify_output(output)
    print("You must mount your dataset! doas zfs mount -a should do it")
    return output



def zfs_send(
    host_from: str = None,
    host_to: str = None,
    dataset_from: str = None,
    dataset_to: str = None,
    host_from_ssh: bool = False,
    host_to_ssh: bool = False,
    create: bool = False,
):
    """Sends a snapshot from near to remote.
    Success, files transfered.."""

    dataset_from, last_snap_from = last_snapshot(host_from, dataset_from)
    dataset_to, last_snap_to = last_snapshot(host_to, dataset_to)

    dataset_from_snap = dataset_from + "@" + last_snap_from

    # First check if remote dataset/snapshot exists
#    if last_snap_from is None:
#        zfs_send_snap = [
#            "zfs",
#            "send",
#            dataset_from_snap,
#        ]


    snapshot_to = "@" + last_snap_to
    zfs_send_snap = [
        "zfs",
        "send",
        "-i",
        snapshot_to,
        dataset_from_snap,
    ]

    if host_from_ssh.lower() in ['true', '1', 'y']:
        zfs_send_snap = ["ssh", host_from] + zfs_send_snap

    zfs_recv = [
        "zfs",
        "recv",
        "-s",
        dataset_to,
    ]

    if host_to_ssh.lower() in ['true', '1', 'y']:
        zfs_recv = ["ssh", host_to] + zfs_recv

    output_send = subprocess.run(zfs_send_snap, stdout=subprocess.PIPE)

    output = subprocess.run(
        zfs_recv,
        input=output_send.stdout,
    )

    verify_output(output)

    return output

