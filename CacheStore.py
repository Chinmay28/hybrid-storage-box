from collections import defaultdict
import threading

class FileMeta(object):

    access_count_map = defaultdict(int)
    write_count_map = defaultdict(int)
    path_to_uuid_map = defaultdict(int)
    lock_map = defaultdict(threading.RLock)

    disk_to_path_map = {
        "io1" : "./io1",
        "gp2" : "./gp2",
        "st1" : "./st1",
        "sc1" : "./sc1"
    }

    DEFAULT_DISK = "st1"
