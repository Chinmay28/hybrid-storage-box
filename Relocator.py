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

        #Get the file id and take lock
        file_id = FileMeta.path_to_uuid_map[src_path]
        lock = FileMeta.lock_map[file_id]
        lock.acquire()
        # 3. if possible, call copy
        copyfile(src_path, dst_path)

        #Now remove the file as we have already copied it
        os.remove(src_path)

        #But we need to update the path to id map
        #ThreadIssue: If some other thread tries to access path_to_uuid_map at the same time for the same file then we are dead. Potential solution=one lock for this map.
        FileMeta.path_to_uuid_map.pop(src_path, None)
        FileMeta.path_to_uuid_map[dst_path] = file_id
        os.symlink(dst_path, src_path)
        #TODO: Update the DB entry - change files path from src_path to dst_path
        #Release lock only after creating symlink and updating db
        lock.release()

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




