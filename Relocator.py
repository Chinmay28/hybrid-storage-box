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

    def __init__(self):
        pass

    @staticmethod
    def relocateFile(disk_id, src_path, dst_path, metric):
        if not os.path.exists(src_path):
            print("How did we reach here?")
            DBUtil().removeStaleEntry(src_path)
            return

        # 1. check if dst has enough space
        available = DiskUtil.get_available_space("/".join(dst_path.split("/")[:-1]))
        if available <= os.path.getsize(src_path):
            print("Available:", available, "File size:", os.path.getsize(src_path))
            # not enough space! Free diff+1024 (just a number)
            status = TravelAgent.cleanupDisk(disk_id, os.path.getsize(src_path) - available + 1024, metric)
            if status is None:
                return None
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

        #ThreadIssue: If some other thread tries to access path_to_uuid_map at 
        #the same time for the same file then we are dead. Potential solution=one lock for this map.
        #But we need to update the path to id map
        FileMeta.path_to_uuid_map.pop(src_path, None)
        DBUtil().updateFilePath(file_id, dst_path)
        FileMeta.path_to_uuid_map[dst_path] = file_id

        # Remove the symlink
        #get the symlink first
        symlinkname = src_path.replace(FileMeta.disk_to_path_map[disk_id], FileMeta.USER_DIRECTORY) 
        symlinkname += '/'

        os.unlink(real_path)
        os.symlink(dst_path, symlinkname)

        #os.symlink(dst_path, src_path)
        
        #Release lock
        lock.release()
        return 0


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
            if dst_path is None:
                return None
            TravelAgent.relocateFile(disk_id, src_path, dst_path, metric)

            return 0


    @staticmethod
    def getRelocationPath(old_path, new_disk_id):

        for key in FileMeta.disk_to_path_map:
            if old_path.startswith(FileMeta.disk_to_path_map[key]):
                return old_path.replace(FileMeta.disk_to_path_map[key], \
                    FileMeta.disk_to_path_map[new_disk_id])

        return None


    @staticmethod
    def runDaemon(frequency=2):

        disk_list = ["io1", "gp2", "st1", "sc1"]

        while True:
            time.sleep(frequency)

            #We will never move any thing from io1 in this daemon.
            #Files will only move out of it during clean up
            for disk in disk_list[1:]:
                row = DBUtil().getHotRow(disk)
                if not row:
                    continue
                print("Row to relocate: ", row)
                status = TravelAgent.relocateFile(disk, row[0], TravelAgent.getRelocationPath(row[0], \
                    disk_list[disk_list.index(disk)-1]), row[2])
                if status is None:
                    print("Daemon is vetoed. Abort!")
                    break
            else:
                break


if __name__ == "__main__":
    TravelAgent.runDaemon()
