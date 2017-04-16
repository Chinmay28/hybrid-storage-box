from __future__ import print_function
from shutil import copyfile
import os
import sys
from Util import DiskUtil, DBUtil
from CacheStore import FileMeta
import time
import shutil


""" Only one instance of the class would be created 
    and all methods are static methods """
class TravelAgent(object):

    disk_to_path_map = {
        "io1" : "/home/cmanjun/src/io1/",
        "gp2" : "/home/cmanjun/src/gp2/",
        "st1" : "/home/cmanjun/src/st1/",
        "sc1" : "/home/cmanjun/src/sc1/" 
    }

    def __init__(self):
        pass

    @staticmethod
    def relocateFile(disk_id, src_path, dst_path, metric):
        if not os.path.exists(src_path):
            print("How did we reach here?")
            # TODO clean stale DB entry!
            return

        # 1. check if dst has enough space
        available = DiskUtil.get_available_space("/".join(dst_path.split("/")[:-1]))
        if available <= os.path.getsize(src_path):
            print("Available:", available, "File size:", os.path.getsize(src_path))
            # not enough space! Free diff+1024 (just a number)
            TravelAgent.cleanupDisk(disk_id, os.path.getsize(src_path) - available + 1024, metric)
        else:
            print("We are good! Available:", available, "File size:", os.path.getsize(src_path))

        #Get the file id and take lock
        file_id = FileMeta.path_to_uuid_map[src_path]
        if not file_id:
            file_id = DBUtil().getFileId(src_path)
            print("File Id:", file_id)
        lock = FileMeta.lock_map[file_id]
        lock.acquire()
        # 3. move file
        shutil.move(src_path, dst_path)

        #But we need to update the path to id map
        FileMeta.path_to_uuid_map.pop(src_path, None)
        DBUtil().updateFilePath(file_id, dst_path)
        FileMeta.path_to_uuid_map[dst_path] = file_id

        # Remove the symlink
        #os.remove(src_path)

        #ThreadIssue: If some other thread tries to access path_to_uuid_map at 
        #the same time for the same file then we are dead. Potential solution=one lock for this map.
        FileMeta.path_to_uuid_map.pop(src_path, None)
        FileMeta.path_to_uuid_map[dst_path] = file_id
        os.symlink(dst_path, src_path)
        #Release lock
        lock.release()

    @staticmethod
    def getVictimIter(disk_id, space_to_free, metric):
        victim_rows = DBUtil().getColdRows(disk_id=disk_id, metric=metric)
        totalSize = 0
        index = 0
        while totalSize < space_to_free:
            current = victim_rows[index]
            index += 1
            totalSize += current[4]
            yield current

    @staticmethod
    def cleanupDisk(disk_id, space_to_free, metric):

        for victim in TravelAgent.getVictimIter(disk_id, space_to_free, metric):
            src_path = victim[5]

            new_disk_id = "io1"
            if disk_id == "io1":
                new_disk_id = "gp2"
            elif disk_id == "gp2":
                new_disk_id = "st1"
            elif disk_id == "st1":
                new_disk_id = "sc1"
            else:
                # Abort
                return None

            dst_path = TravelAgent.getRelocationPath(src_path, new_disk_id)
            TravelAgent.relocateFile(disk_id, src_path, dst_path, metric)

            return 0


    @staticmethod
    def getRelocationPath(old_path, new_disk_id):

        for key in TravelAgent.disk_to_path_map:
            if old_path.startswith(TravelAgent.disk_to_path_map[key]):
                return old_path.replace(TravelAgent.disk_to_path_map[key], \
                    TravelAgent.disk_to_path_map[new_disk_id])

        return None


    @staticmethod
    def runDaemon(frequency=2):

        disk_list = ["io1", "gp2", "st1", "sc1"]

        while True:
            time.sleep(frequency)

            for disk in disk_list[1:]:
                row = DBUtil().getHotRow(disk)
                if not row:
                    continue
                print("Row to relocate: ", row)
                TravelAgent.relocateFile(disk, row[0], TravelAgent.getRelocationPath(row[0], \
                    disk_list[disk_list.index(disk)-1]), row[2])



if __name__ == "__main__":
    TravelAgent.runDaemon()




