import psycopg2
import sys
import os
from collections import namedtuple
from CacheStore import FileMeta
import time
import uuid


class DBUtil(object):

    def __init__(self):
        self.connnection = psycopg2.connect("dbname=postgres user=postgres password=test")

    def insert(self, query):
        cursor = self.connnection.cursor()
        cursor.execute(query)
        cursor.close()
        self.connnection.commit()

    def query(self, query):
        cursor = self.connnection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        self.connnection.commit()
        return result
    
    def getCounts(self, file_id):
        cursor = self.connnection.cursor()
        cursor.execute("select access_count,write_count from file_meta \
        where file_id=\'" + str(file_id) + "\';")
        result = cursor.fetchone()
        cursor.close()
        self.connnection.commit()
        if result:
            return [int(result[0]), int(result[1])]
        else:
            return None        
    
    def __del__(self):
        self.connnection.close()
        
        
    @staticmethod
    def writeToDB(fuseObject, sleeptime):
        while True:
            #Wake up every sleeptime seconds
            time.sleep(sleeptime)
            #Write to DB only if any dictionary is not empty
            if len(FileMeta.access_count_map) > 0 or len(FileMeta.write_count_map) > 0:
                print("Writing file counts to DB (one by one)")
                #loop on all file locks
                for path in FileMeta.path_to_uuid_map:
                    realpath = fuseObject.getrealpath(path)
                    file_id = FileMeta.path_to_uuid_map[realpath]
                    lock = FileMeta.lock_map[file_id]
                    # get the old counts
                    if file_id is not 0:
                        old_counts = fuseObject.db_conn.getCounts(file_id)
                    else:
                        # no entry in the DB or Cache. lets create it!
                        file_id = str(uuid.uuid1())
                        FileMeta.path_to_uuid_map[realpath] = file_id
                        
                        last_update_time = last_move_time = create_time = str(time.time())
                        access_count = write_count = "0"
                        volume_info = "hdd_hot"
                        file_tag = "tada!"
                        query = "insert into file_meta values( \'" + str(file_id)+"\', \'" \
                        + realpath +"\', \'" + create_time+ "\', \'" + last_update_time+"\', \'" + last_move_time\
                        +"\', \'" + access_count+"\', \'" + write_count+"\', \'" + volume_info+"\', \'" + file_tag+"\');"
                        print("Executing: ", query)
                        fuseObject.db_conn.insert(query)
                        old_counts = [0, 0]
                        
                    lock.acquire()
                    #only write if atleast one count is non zero. (This condition should not occur)
                    if FileMeta.access_count_map[file_id] > 0 or FileMeta.write_count_map[file_id] > 0:
                        #write to DB
                        update_query = "update file_meta set access_count=\'" + \
                        str(FileMeta.access_count_map[file_id] + old_counts[0])+"\', write_count=\'" \
                        + str(FileMeta.write_count_map[file_id] + old_counts[0])+"\' where file_id=\'" + str(file_id)+ "\';"
                        print("Executing: ", update_query)
                        fuseObject.db_conn.insert(update_query)

                    lock.release()
                #Clear both count dictionaries. Note that we dont need to clear lock dictionary.
                FileMeta.access_count_map.clear()
                FileMeta.write_count_map.clear()



class DiskUtil(object):

    @staticmethod
    def get_available_space(path):
        """
        http://stackoverflow.com/questions/787776/find-free-disk-space-in-python-on-os-x
        """
        st = os.statvfs(path)
        return st.f_bavail * st.f_frsize


if __name__ == "__main__":
    DBUtil().getCounts("62a1edf4-142b-11e7-a41a-00505608aa24")
