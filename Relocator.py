from __future__ import print_function
from shutil import copyfile
import os
import sys
from Util import DiskUtil


""" Only one instance of the class would be created 
    and all methods are static methods """
class TravelAgent(object):

    disk_to_path_map = {
        "io1" : "/mnt/io1/",
        "gp2" : "/mnt/gp2/",
        "st1" : "/mnt/st1/",
        "sc1" : "/mnt/sc1/" 
    }

    def __init__(self):
        pass

    @staticmethod
    def relocateFile(src_path, dst_path, metric):
        # 1. check if dst has enough space
        available = DiskUtil.get_available_space("/".join(dst_path.split("/")[:-1]))
        if available <= os.path.getsize(src_path):
            print("Available:", available, "File size:", os.path.getsize(src_path))
            # not enough space!
            # 2. call cleanup if not
        else:
            print("We are good! Available:", available, "File size:", os.path.getsize(src_path))

        # 3. if possible, call copy
        copyfile(src_path, dst_path)

        # 4. call symlink 
        os.remove(src_path)
        os.symlink(dst_path, src_path)

    @staticmethod
    def getVictimIter(disk_id, space_to_free, metric):
        victim_rows = DBUtil().getMatchingRows(disk_id=disk_id, metric=metric)
        totalSize = 0
        index = 0
        while totalSize < space_to_free:
            current = victim_rows[index]
            index += 1
            totalSize += current[4]
            yield current

    @staticmethod
    def cleanupDisk(disk_id, space_to_free, metric):

        for victim in getVictimIter(disk_id, space_to_free, metric):
            victim_path = victim[5]

            src_path = disk_to_path_map[disk_id] + victim_path
            if disk_id == "io1":
                # destination is gp2
                dst_path = disk_to_path_map["gp2"] + victim_path

            elif disk_id == "gp2":
                # destination is st1
                dst_path = disk_to_path_map["st1"] + victim_path

            elif disk_id == "st1":
                # destination is sc1
                dst_path = disk_to_path_map["sc1"] + victim_path

            else:
                # we are out of options!
                return # or fail :-D

            relocateFile(src_path, dst_path)



if __name__ == "__main__":
    TravelAgent.relocate_file(sys.argv[1], sys.argv[2])




