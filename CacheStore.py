from collections import defaultdict
import threading
import logging

main_logger = logging.getLogger('HYBRID_STORAGE_BOX')
hdlr = logging.FileHandler('/home/ubuntu/main.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
main_logger.addHandler(hdlr) 
main_logger.setLevel(logging.INFO)

db_logger = logging.getLogger('DB_LOG')
hdlr2 = logging.FileHandler('/home/ubuntu/db.log')
hdlr2.setFormatter(formatter)
db_logger.addHandler(hdlr2) 
db_logger.setLevel(logging.INFO)

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
