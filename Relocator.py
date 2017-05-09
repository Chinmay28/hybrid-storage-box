from __future__ import print_function
from shutil import copyfile
import os
import sys
from Util import DiskUtil, DBUtil
from CacheStore import FileMeta
import time
import shutil
import uuid
from CacheStore import db_logger, main_logger
import Config


""" Only one instance of the class would be created 
    and all methods are static methods """
class TravelAgent(object):

    def __init__(self):
        pass

    @staticmethod
    def relocateFile(disk_id, src_path, dst_path, metric):
        if src_path == dst_path:
            main_logger.error("src_path = dst_path! Crazy. FIXME. Denying request.")
            return None
        main_logger.info("Trying to move " + src_path + " to " + dst_path + "...")
        if not os.path.exists(src_path):
            main_logger.info("Removing stale DB entry.")
            DBUtil().removeStaleEntry(src_path)
            return None

        # 1. check if dst has enough space
        available = DiskUtil.get_available_space(disk_id)
        if available <= os.path.getsize(src_path):
            main_logger.info("Available:"+ str(available)+ "File size:"+ str(os.path.getsize(src_path)))
            # not enough space! Free diff+1024 (just a number)
            status = TravelAgent.cleanupDisk(disk_id, os.path.getsize(src_path) - available + 1024, metric)
            if status is None:
                return None
        else:
            main_logger.info("We are good! Available:"+ str(available)+ "File size:"+ str(os.path.getsize(src_path)))

        #Get the file id and take lock
        file_id = FileMeta.path_to_uuid_map[src_path]
        if not file_id:
            file_id = DBUtil().getFileId(src_path)
            FileMeta.path_to_uuid_map[src_path] = file_id
        
        if file_id:
            #Aquire lock so that policy thread wont interfere. Unlock in release method
            lock = FileMeta.lock_map[file_id]           
        else:
            file_id = str(uuid.uuid1())
            FileMeta.path_to_uuid_map[src_path] = file_id
            lock = FileMeta.lock_map[file_id]
                    
        lock.acquire()
        # 3. move file
        try: 
            shutil.move(src_path, dst_path)
        except IOError:
            main_logger.info("Something went wrong. Retrying...")
            TravelAgent.relocateFile(disk_id, src_path, dst_path, metric)

        #But we need to update the path to id map
        FileMeta.path_to_uuid_map.pop(src_path, None)
        DBUtil().updateFilePath(file_id, dst_path)
        FileMeta.path_to_uuid_map[dst_path] = file_id

        # Remove the symlink
        #get the symlink first
        symlinkname = src_path.replace(FileMeta.disk_to_path_map[DiskUtil.getDiskId(src_path)], FileMeta.USER_DIRECTORY) 

        os.unlink(symlinkname)
        os.symlink(dst_path, symlinkname)

        #Release lock
        lock.release()
        main_logger.info("Move " + src_path + " to " + dst_path + " successful!")
        return 0


    @staticmethod
    def getVictimIter(disk_id, space_to_free, metric):
        victim_rows = DBUtil().getColdRows(disk_id=disk_id, metric=metric)
        totalSize = 0
        index = 0
        if not victim_rows:
            # No victims. Abort.
            return
        while totalSize < space_to_free:
            current = victim_rows[index]
            index += 1
            totalSize += current[4]
            yield current

    @staticmethod
    def cleanupDisk(disk_id, space_to_free, metric):
        main_logger.info("cleanupDisk, "+ disk_id+ ", " + str(space_to_free)+ ", "+ str(metric))

        for victim in TravelAgent.getVictimIter(disk_id, space_to_free, metric):
            main_logger.info("Victims: "+ str(victim))
            time.sleep(2)
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
                break

            dst_path = TravelAgent.getRelocationPath(src_path, disk_id, new_disk_id)
            main_logger.info("Destination path: "+ dst_path)
            if dst_path is None:
                return None
            status = TravelAgent.relocateFile(new_disk_id, src_path, dst_path, metric)
            if status is None:
                break

            return 0
        main_logger.info("No victims!")
        time.sleep(2)
        return None


    @staticmethod
    def getRelocationPath(old_path, old_disk_id, new_disk_id):

        return old_path.replace(FileMeta.disk_to_path_map[old_disk_id], \
                FileMeta.disk_to_path_map[new_disk_id])

    @staticmethod
    def runDaemon(frequency=5):

        disk_list = ["io1", "gp2", "st1", "sc1"]

        while True:
            time.sleep(frequency)
            DBUtil.writeToDB();
            
            if Config.RELOCATE:
                for disk in disk_list[1:]:
                    row = DBUtil().getHotRow(disk)
                    if not row:
                        continue
                    main_logger.info("Row to relocate: "+ str(row))
                    status = TravelAgent.relocateFile(disk_list[disk_list.index(disk)-1], \
                        row[0], TravelAgent.getRelocationPath(row[0], disk, disk_list[disk_list.index(disk)-1]), row[2])
                    if status is None:
                        main_logger.info("Daemon couldn't score! It will do some DB housekeeping now before continuing.")
                        DBUtil().cleanupStaleEntries()
                else:
                    # continue if inner loop didn't break
                    continue
                break


if __name__ == "__main__":
    TravelAgent.runDaemon()
