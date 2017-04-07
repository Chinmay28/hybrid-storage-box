#!/usr/bin/env python

from __future__ import with_statement, print_function

import os
import sys
import errno

from collections import defaultdict
from fuse import FUSE, FuseOSError, Operations
from Util import DBUtil
import uuid
import time
import pprint
import threading
import thread
import time

#DB update thread
def WriteToDB(classobj, sleeptime):
    while True:
        #Wake up every sleeptime seconds
        time.sleep(sleeptime)
        print("Writing file counts to DB (one by one)")
        #Write to DB only if any dictionary is not empty
        if len(classobj.file_access_count) > 0 or len(classobj.file_write_count) > 0:
            #loop on all file locks
            for path in classobj.path_to_uuid:
                realpath = classobj.getrealpath(path)
                uuid = classobj.path_to_uuid[realpath]
                lock = classobj.file_lock[uuid]
                lock.acquire()
                #only write if atleast one count is non zero. (This condition should not occur)
                if classobj.file_access_count[uuid] > 0 or classobj.file_write_count[uuid] > 0:
                    #write to DB
                    update_query = "update file_meta set access_count=\'" + str(classobj.file_access_count[uuid])+"\', write_count=\'" \
                    + str(classobj.file_write_count[uuid])+"\' where file_id=\'" + str(uuid)+ "\';"
                    #print ("Updated", update_query)
                    classobj.db_conn.insert(update_query)

                lock.release()
            #Clear both count dictionaries. Note that we dont need to clear lock dictionary.
            classobj.file_access_count.clear()
            classobj.file_write_count.clear()

class Passthrough(Operations):
    def __init__(self, root):
        self.root = root
        self.db_conn = DBUtil()
        self.file_access_count = defaultdict(lambda:0)
        self.file_write_count = defaultdict(lambda:0)
        self.file_lock = defaultdict(threading.RLock)
        self.path_to_uuid = defaultdict(lambda:0)
        #Start DB update thread. Pass self pointer and sleep time.
        thread.start_new_thread(WriteToDB, (self, 10))

    # Helpers
    # =======

    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path

    # Filesystem methods
    # ==================

    def access(self, path, mode):
        full_path = self._full_path(path)
        print("LOG: access")
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        print("LOG: chmod")
        full_path = self._full_path(path)
        return os.chmod(full_path, mode)

    def chown(self, path, uid, gid):
        print("LOG: chown")
        full_path = self._full_path(path)
        return os.chown(full_path, uid, gid)

    def getattr(self, path, fh=None):
        print("LOG: getattr")
        full_path = self._full_path(path)
        st = os.lstat(full_path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                     'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    def readdir(self, path, fh):
        print("LOG: readdir")
        full_path = self._full_path(path)

        dirents = ['.', '..']
        if os.path.isdir(full_path):
            dirents.extend(os.listdir(full_path))
        for r in dirents:
            yield r

    def readlink(self, path):
        print("LOG: readlink")
        pathname = os.readlink(self._full_path(path))
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            return os.path.relpath(pathname, self.root)
        else:
            return pathname

    def mknod(self, path, mode, dev):
        print("LOG: mknode")
        return os.mknod(self._full_path(path), mode, dev)

    def rmdir(self, path):
        print("LOG: rmdir")
        full_path = self._full_path(path)
        return os.rmdir(full_path)

    def mkdir(self, path, mode):
        print("LOG: mkdir")
        return os.mkdir(self._full_path(path), mode)

    def statfs(self, path):
        print("LOG: statfs")
        full_path = self._full_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def unlink(self, path):
        print("LOG: unlink")
        return os.unlink(self._full_path(path))

    def symlink(self, name, target):
        print("LOG: symlink")
        return os.symlink(name, self._full_path(target))

    def rename(self, old, new):
        print("LOG: rename")
        realpath = self.getrealpath(self._full_path(old))

        #Aquire lock so that policy thread wont interfere
        lock = self.file_lock[self.path_to_uuid[realpath]]
        lock.acquire()
        retval = os.rename(self._full_path(old), self._full_path(new))
        #unlick now
        lock.release()
        return retval

    def link(self, target, name):
        print("LOG: link")
        return os.link(self._full_path(target), self._full_path(name))

    def utimens(self, path, times=None):
        print("LOG: utimes")
        return os.utime(self._full_path(path), times)

    # File methods
    # ============

    def open(self, path, flags):
        full_path = self._full_path(path)
        realpath = self.getrealpath(full_path)
        print("LOG: open ", realpath)
        #Aquire lock so that policy thread wont interfere. Unlock in release method
        lock = self.file_lock[self.path_to_uuid[realpath]]
        lock.acquire()
        return os.open(full_path, flags)

    def create(self, path, mode, fi=None):
        full_path = self._full_path(path)

        file_id = str(uuid.uuid1())

        #Aquire lock so that policy thread wont interfere. Unlock in release method
        lock = self.file_lock[file_id]
        lock.acquire()

        print("LOG create(): ", full_path)

        #Prepare query
        last_update_time = last_move_time = create_time = str(time.time())
        access_count = write_count = "1"
        volume_info = "hdd_hot"
        file_tag = "tada!"
        query = "insert into file_meta values( \'" + file_id+"\', \'" \
        + full_path+"\', \'" + create_time+ "\', \'" + last_update_time+"\', \'" + last_move_time\
        +"\', \'" + access_count+"\', \'" + write_count+"\', \'" + volume_info+"\', \'" + file_tag+"\');"
        self.db_conn.insert(query)

        #Add path and uuid to dictionary
        self.path_to_uuid[full_path] = file_id

        #update usage count
        self.file_access_count[file_id] += 1
        self.file_write_count[file_id] += 1


        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        realpath = self.getrealpath(self._full_path(path))
        print("LOG: read ", realpath)
        os.lseek(fh, offset, os.SEEK_SET)
        #update read count
        self.file_access_count[self.path_to_uuid[realpath]] += 1
        return os.read(fh, length)

    def write(self, path, buf, offset, fh):
        realpath = self.getrealpath(self._full_path(path))
        print("LOG: write ", realpath)
        os.lseek(fh, offset, os.SEEK_SET)
        #update read and write count
        self.file_access_count[self.path_to_uuid[realpath]] += 1
        self.file_write_count[self.path_to_uuid[realpath]] += 1
        return os.write(fh, buf)

    def truncate(self, path, length, fh=None):
        print("LOG: truncate ", path)
        full_path = self._full_path(path)
        with open(full_path, 'r+') as f:
            f.truncate(length)

    def flush(self, path, fh):
        print("LOG: flush ", path)
        return os.fsync(fh)

    def release(self, path, fh):
        realpath = self.getrealpath(self._full_path(path))
        print("LOG: release ", realpath)
        #Unlock the lock taken during open or create.
        lock = self.file_lock[self.path_to_uuid[realpath]]
        lock.release()
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        print("LOG: fsync ", path)
        return self.flush(path, fh)

    def getrealpath(self, path):
        #return original path
        return os.path.realpath(path)

def main(mountpoint, root):
    FUSE(Passthrough(root), mountpoint, nothreads=True, foreground=True)


if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])
