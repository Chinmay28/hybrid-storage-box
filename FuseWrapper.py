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
from CacheStore import FileMeta


class FuseSystem(Operations):

    def __init__(self, root):
        self.root = root
        self.db_conn = DBUtil()
        #Start DB update thread. Pass self pointer and sleep time.
        thread.start_new_thread(DBUtil.writeToDB, (self, 100))

    # Helpers
    # =======

    #Appends ./src
    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path
        
    # Filesystem methods
    # ==================

    #Do not use real path here. This function is appliicable fo symlinks as well
    def access(self, path, mode):
        full_path = self._full_path(path)
        print("LOG: access")
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    #Permissions of both symlink as well as the real file have to change
    def chmod(self, path, mode):
        print("LOG: chmod")
        full_path = self.getrealpath(self._full_path(path))
        os.chmod(seld._full_path(path), mode)
        return os.chmod(full_path, mode)

    #Same as chmod
    def chown(self, path, uid, gid):
        print("LOG: chown")
        full_path = self.getrealpath(self._full_path(path))
        os.chown(seld._full_path(path), uid, gid)
        return os.chown(full_path, uid, gid)

    #Get real files attributes. User will not know if we have created a symlink
    def getattr(self, path, fh=None):
        full_path = self.getrealpath(self._full_path(path))
        print("LOG: getattr ", path, self._full_path(path), full_path)
        st = os.lstat(full_path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                     'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

     #Always on real folder
    def readdir(self, path, fh):
        print("LOG: readdir")
        full_path = self.getrealpath(self._full_path(path))

        dirents = ['.', '..']
        if os.path.isdir(full_path):
            dirents.extend(os.listdir(full_path))
        for r in dirents:
            yield r

    #Applicable for links, so sont use real path
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
        return os.mknod(self.getrealpath(self._full_path(path)), mode, dev)

    def rmdir(self, path):
        print("LOG: rmdir")
        full_path = self.getrealpath(self._full_path(path))
        return os.rmdir(full_path)

    def mkdir(self, path, mode):
        print("LOG: mkdir")
        return os.mkdir(self.getrealpath(self._full_path(path)), mode)

    def statfs(self, path):
        print("LOG: statfs")
        full_path = self.getrealpath(self._full_path(path))
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    #When symlink gets deleted, we need to delete the real file as well as the symlink
    def unlink(self, path):
        real_path = self.getrealpath(self._full_path(path))
        print("LOG: unlink ", real_path)
        fid = FileMeta.path_to_uuid_map[real_path]
        lock = FileMeta.lock_map[fid]
        lock.acquire()
        os.unlink(self._full_path(path))
        #TODO: Update db about the deletion
        retval = os.unlink(real_path)
        #Threading Issue: While poping another thread tries to access this. Same as in relocatefile
        FileMeta.path_to_uuid_map.pop(real_path, None)
        FileMeta.write_count_map.pop(fid, None)
        FileMeta.access_count_map.pop(fid, None)
        lock = FileMeta.lock_map.pop(fid, None)
        lock.release()
        return retval

    def symlink(self, target, name):
        target = self.getrealpath(self._full_path(target))
        print("LOG: symlink", target, self._full_path(name))
        return os.symlink(target, self._full_path(name))
        
        
    #rename symlinks only
    def rename(self, old, new):
        print("LOG: rename")
        realpath = self.getrealpath(self._full_path(old))

        #Aquire lock so that policy thread wont interfere
        lock = FileMeta.lock_map[FileMeta.path_to_uuid_map[realpath]]
        lock.acquire()
        retval = os.rename(self._full_path(old), self._full_path(new))
        #unlick now
        lock.release()
        return retval

    #WE ARE DEAD IF THIS GETS CALLED
    def link(self, target, name):
        print("LOG: link")
        return os.link(self._full_path(target), self._full_path(name))

    def utimens(self, path, times=None):
        print("LOG: utimes")
        return os.utime(self.getrealpath(self._full_path(path)), times)
        
    # File methods
    # ============

    def open(self, path, flags):
        full_path = self._full_path(path)
        realpath = self.getrealpath(full_path)
        print("LOG: open ", realpath)
        #Aquire lock so that policy thread wont interfere. Unlock in release method
        lock = FileMeta.lock_map[FileMeta.path_to_uuid_map[realpath]]
        lock.acquire()
        return os.open(full_path, flags)

    def create(self, path, mode, fi=None):
        full_path = self._full_path(path)
        file_id = str(uuid.uuid1())
       # fh = os.open("./st1/yahoo.txt", os.O_WRONLY | os.O_CREAT, mode)
        #os.close(fh)

        #Aquire lock so that policy thread wont interfere. Unlock in release method
        lock = FileMeta.lock_map[file_id]
        lock.acquire()
        actual_path = os.path.abspath(FileMeta.disk_to_path_map[FileMeta.DEFAULT_DISK])
        actual_path += path
        print("Actually writing to = ", actual_path)

        print("LOG create(): ", full_path)

        #Prepare query
        last_update_time = last_move_time = create_time = str(time.time())
        access_count = write_count = "1"
        volume_info = FileMeta.DEFAULT_DISK
        file_tag = "tada!"
        query = "insert into file_meta values( \'" + file_id+"\', \'" \
        + actual_path +"\', \'0\', \'" + create_time+ "\', \'" + last_update_time+"\', \'" + last_move_time\
        +"\', \'" + str(access_count)+"\', \'" + str(write_count)+"\', \'" + volume_info+"\', \'" + file_tag+"\');"
        self.db_conn.insert(query)

        print("DB update finished.")
        #Add path and uuid to dictionary
        FileMeta.path_to_uuid_map[actual_path] = file_id

        print("update usage count...")
        FileMeta.access_count_map[file_id] += 1
        FileMeta.write_count_map[file_id] += 1

        os.symlink(actual_path, self._full_path(path))

        return os.open(actual_path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        realpath = self.getrealpath(self._full_path(path))
        print("LOG: read ", realpath)
        os.lseek(fh, offset, os.SEEK_SET)
        #update read count
        FileMeta.access_count_map[FileMeta.path_to_uuid_map[realpath]] += 1
        return os.read(fh, length)

    def write(self, path, buf, offset, fh):
        realpath = self.getrealpath(self._full_path(path))
        print("LOG: write ", realpath)
        os.lseek(fh, offset, os.SEEK_SET)
        #update read and write count
        FileMeta.access_count_map[FileMeta.path_to_uuid_map[realpath]] += 1
        FileMeta.write_count_map[FileMeta.path_to_uuid_map[realpath]] += 1
        return os.write(fh, buf)

    def truncate(self, path, length, fh=None):
        realpath = self.getrealpath(self._full_path(path))
        print("LOG: truncate ", realpath)
        with open(realpath, 'r+') as f:
            f.truncate(length)

    def flush(self, path, fh):
        print("LOG: flush ", self.getrealpath(self._full_path(path)))
        return os.fsync(fh)

    def release(self, path, fh):
        realpath = self.getrealpath(self._full_path(path))
        print("LOG: release ", realpath)
        val = os.close(fh)
        #Unlock the lock taken during open or create.        
        lock = FileMeta.lock_map[FileMeta.path_to_uuid_map[realpath]]
        lock.release()
        return val

    def fsync(self, path, fdatasync, fh):
        print("LOG: fsync ", path)
        return self.flush(path, fh)

    def getrealpath(self, path):
        #return original path
        return os.path.realpath(path)

def main(mountpoint, root):
    FUSE(FuseSystem(root), mountpoint, nothreads=True, foreground=True)


if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])
 
