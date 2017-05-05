import psycopg2
import sys
import os
from collections import namedtuple
from CacheStore import FileMeta
from CacheStore import db_logger, main_logger
import time
import uuid


class DBUtil(object):

    def __init__(self):
        self.connnection = psycopg2.connect("dbname=postgres user=postgres password=test")

    def insert(self, query):
        cursor = self.connnection.cursor()
        db_logger.info("executing: "+ query)
        cursor.execute(query)
        cursor.close()
        self.connnection.commit()

    def query(self, query):
        cursor = self.connnection.cursor()
        db_logger.info("executing: "+ query)
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

    def getColdRows(self, disk_id=None, metric=None):
        if disk_id and metric:
            cursor = self.connnection.cursor()
            cursor.execute("select file_id,volume_info,access_count,write_count,file_size,file_path from file_meta \
            where volume_info=\'" + disk_id + "\' and access_count < \'" + str(metric) + "\' \
            and write_count < \'" + str(metric) + "\' order by access_count;")
            result = cursor.fetchall()
            cursor.close()
            self.connnection.commit()
            return result

    def getHotRow(self, disk_id=None):
        if disk_id:
            query = "select file_path,file_size,access_count,volume_info from file_meta \
            where volume_info=\'"+disk_id+"\' order by access_count desc limit 1;"
            db_logger.info("getHotRow: "+query)
            cursor = self.connnection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            self.connnection.commit()
            return result

    def updateFilePath(self, file_id, dst_path):
         if file_id and dst_path:
            query = "update file_meta set file_path=\'"+dst_path+"\',\
            volume_info=\'"+DiskUtil.getDiskId(dst_path)+"\' where file_id=\'"+file_id+"\';"
            db_logger.info(query)
            cursor = self.connnection.cursor()
            cursor.execute(query)
            cursor.close()
            self.connnection.commit()   
         else:
            db_logger.error(str(file_id) + dst_path)       

    def getFileId(self, src_path):
        if src_path:
            query = "select file_id from file_meta where file_path=\'"+src_path+"\';"
            db_logger.info(query)
            cursor = self.connnection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            
            if result is None:
                file_id = str(uuid.uuid1())
                result = [file_id]
                FileMeta.path_to_uuid_map[src_path] = file_id
                
                last_update_time = last_move_time = create_time = str(time.time())
                access_count = write_count = "0"
                volume_info = DiskUtil.getDiskId(src_path)
                file_tag = "tada!"
                query = "insert into file_meta values( \'" + str(file_id)+"\', \'" \
                + src_path +"\', \'0\',\'" + create_time+ "\', \'" + last_update_time+"\', \'" + last_move_time\
                +"\', \'" + str(access_count)+"\', \'" + str(write_count)+"\', \'" + volume_info+"\', \'" + file_tag+"\');"
                db_logger.info("Executing: "+ query)
                self.insert(query)
            
            cursor.close()
            self.connnection.commit()
            return result[0]      

    def removeStaleEntry(self, src_path):
        if src_path:
            query = "delete from file_meta where file_path=\'"+src_path+"\';"
            db_logger.info(query)
            cursor = self.connnection.cursor()
            cursor.execute(query)
            cursor.close()
            self.connnection.commit()


    def updateSize(self, src_path, size):
        if src_path:
            query = "update file_meta set file_size=\'"+size+"\' where file_path=\'"+src_path+"\';"
            db_logger.info(query)
            cursor = self.connnection.cursor()
            cursor.execute(query)
            cursor.close()
            self.connnection.commit()    
            

    @staticmethod
    def writeToDB():
        #Write to DB only if any dictionary is not empty
        if len(FileMeta.access_count_map) > 0 or len(FileMeta.write_count_map) > 0:
            db_logger.info("Updating counts in DB...")
            db_conn = DBUtil()
            #loop on all file locks
            for path in FileMeta.path_to_uuid_map:
                realpath = os.path.realpath(path)
                file_id = FileMeta.path_to_uuid_map[realpath]
                
                if not file_id:
                    file_id = db_conn.getFileId(realpath) 
                    FileMeta.path_to_uuid_map[realpath] = file_id               
                
                if file_id:
                    #Aquire lock so that policy thread wont interfere. Unlock in release method
                    lock = FileMeta.lock_map[file_id]
                    lock.acquire()
                    realpath = os.path.realpath(path)
                    old_counts = db_conn.getCounts(file_id)     
                else:
                    file_id = str(uuid.uuid1())
                    FileMeta.path_to_uuid_map[realpath] = file_id
                    lock = FileMeta.lock_map[file_id]                                                                                            
                    lock.acquire()
                    realpath = os.path.realpath(path)                    
                    last_update_time = last_move_time = create_time = str(time.time())
                    access_count = write_count = "0"
                    volume_info = DiskUtil.getDiskId(realpath)
                    file_tag = "tada!"
                    query = "insert into file_meta values( \'" + str(file_id)+"\', \'" \
                    + realpath +"\', \'" + str(os.path.getsize(realpath)) + "\',\'" + create_time+ "\', \'" + last_update_time+"\', \'" + last_move_time\
                    +"\', \'" + str(access_count)+"\', \'" + str(write_count)+"\', \'" + volume_info+"\', \'" + file_tag+"\');"
                    db_logger.info("Executing: "+ query)
                    db_conn.insert(query)
                    old_counts = [0, 0]
                
                if not old_counts:
                    old_counts = [0, 0]
                    
                #only write if atleast one count is non zero. (This condition should not occur)
                if FileMeta.access_count_map[file_id] > 0 or FileMeta.write_count_map[file_id] > 0:
                    #write to DB
                    update_query = "update file_meta set access_count=\'" + \
                    str(FileMeta.access_count_map[file_id] + old_counts[0])+"\', write_count=\'" \
                    + str(FileMeta.write_count_map[file_id] + old_counts[0])+"\', file_size=\'" + str(os.path.getsize(realpath)) + "\' \
                    where file_id=\'" + str(file_id)+ "\';"
                    db_logger.info("Executing: "+ update_query)
                    db_conn.insert(update_query)

                lock.release()
            #Clear both count dictionaries. Note that we dont need to clear lock dictionary.
            FileMeta.access_count_map.clear()
            FileMeta.write_count_map.clear()



class DiskUtil(object):

    @staticmethod
    def get_available_space(disk_id):
        """
        http://stackoverflow.com/questions/787776/find-free-disk-space-in-python-on-os-x
        """
        st = os.statvfs(FileMeta.disk_to_path_map[disk_id])
        return st.f_bavail * st.f_frsize

    @staticmethod
    def getDiskId(path):
        for key in FileMeta.disk_to_path_map:
            if path.startswith(FileMeta.disk_to_path_map[key]):
                return key
        return None 

if __name__ == "__main__":
    print(DBUtil().getHotRow("io1"))


