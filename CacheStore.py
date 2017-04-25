from collections import defaultdict
import threading

class FileMeta(object):

    access_count_map = defaultdict(int)
    write_count_map = defaultdict(int)
    path_to_uuid_map = defaultdict(int)
    lock_map = defaultdict(threading.RLock)

    disk_to_path_map = {
        "io1" : "/mnt/io1",
        "gp2" : "/mnt/gp2",
        "st1" : "/mnt/st1",
        "sc1" : "/mnt/sc1"
    }

    DEFAULT_DISK = "sc1"
    USER_DIRECTORY = "" # is set by FuseWrapper
