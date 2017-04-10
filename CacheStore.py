from collections import defaultdict
import threading


class FileMeta(object):
    
    access_count_map = defaultdict(int)
    write_count_map = defaultdict(int)
    path_to_uuid_map = defaultdict(int)
    lock_map = defaultdict(threading.RLock)
    