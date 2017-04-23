#!/bin/bash
import os
from random import randint
import time
import datetime
import threading
import thread
from collections import defaultdict
import math
import subprocess
import sys

#media, doc, archive - zip & tar & iso

totalreadcount = 0
totalwritecount = 0
totaldeletecount = 0

music = ['.aif','.cda','.mid','.mp3','.mpa','.ogg','.wav','.wma']
video = ['.3gp','.avi','.flv','.h264','.m4v','.mkv','.mov','.mp4','.mpg','.rm','.swf','.vob','.wmv']
archive = ['.7z','.arj','.deb','.pkg','.rar','.rpm','.tar','.zip','.dmg','.iso','.toast','.vcd']
image = ['.ai','.bmp','.gif','.ico','.jpe','.png','.ps','.psd','.svg','.tif']
doc = ['.key','.odp','.pps','.ppt','.pptx','.c','.C','.class','.cpp','.cs','.h','.java','.sh','.swift','.vb','.py','.ods','.xlr','.xls','.xlsx','.doc','.odt','.pdf','.rtf','.tex','.txt','.wks','.wpd','.docx']

godarray = [music, video, archive, image, doc]
fnames = []
fname_lock = threading.Lock()
r_time = 0
w_time = 0
d_time = 0

def getts ():
    return datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H-%M-%S')

def func (fname, size, units, array):
    index = randint(0,len(array))
    i = index%len(array)
    filename = fname+array[i]
    cmd = "bash mkfile " + filename + " " + str(size) + units
    print cmd
    global w_time
    a = time.time()
    os.system(cmd)
    w_time += (time.time()-a)
    return filename

def Create (path):
    main_index = randint(0,4)
    global godarray
    global fnames
    global fname_lock
    ftype = godarray[main_index]
    findex = randint(0, len(ftype)-1)
    ext = ftype[findex]
    if findex == 0:
        #music
        size = randint(1, 10)
        name = func(path+"mfile_"+getts(), size, "M", ftype)
        fname_lock.acquire()
        fnames.append(name)
        fname_lock.release()
        return size
    elif findex == 1:
        #video
        size = randint(50, 5000)
        name = func(path+"vfile_"+getts(), size, "M", ftype)
        fname_lock.acquire()
        fnames.append(name)
        fname_lock.release()
        return size
    elif findex == 2:
        #archive
        size = randint(10, 1000)
        name = func(path+"afile_"+getts(), size, "M", ftype)
        fname_lock.acquire()
        fnames.append(name)
        fname_lock.release()
        return size
    elif findex == 3:
        #image
        size = randint(10, 5000)
        name = func(path+"ifile_"+getts(), size, "K", ftype)
        fname_lock.acquire()
        fnames.append(name)
        fname_lock.release()
        return size/1000
    elif findex == 4:
        #doc
        size = randint(1, 2000)
        name = func(path+"dfile_"+getts(), size, "K", ftype)
        fname_lock.acquire()
        fnames.append(name)
        fname_lock.release()
        return size/1000

    return 0

def Append(path):
    global fnames
    global fname_lock
    #Get an existing file
    fname_lock.acquire() 
    if len(fnames) > 1:
        index = randint(0, len(fnames)-1)
    elif len(fnames) == 1:
        index = 0
    elif len(fnames) == 0:
        Create(path)
        fname_lock.release()
        return

    name = fnames[index]
    lines = randint(0, 20)
    cmd = "tail -n "+str(lines)+" "+name+" >> "+name
    global w_time
    global totalwritecount
    print cmd
    a = time.time()
    totalwritecount += 1
    os.system(cmd)
    w_time += (time.time()-a)
    fname_lock.release()

def WritesThread (p, path, totalsize, etime):
    global fname_lock
    s = time.time()
    while True:
        global fnames
        total = 0
        rval = randint(0,100)
        if rval < p:
            #decide if we want to append or create
            choice = randint (0,3)
            global totalwritecount
            if choice == 0 or choice == 2:
                #Only create if size is not more than limit
                if total < totalsize:
                    totalwritecount += 1
                    val = Create(path)
                    total += val
            else:
                Append(path)
        if time.time() - s >= etime:
            return

def DeletesThread (p, path, etime):
    s = time.time()
    global fname_lock
    while True:        
        global fnames
        rval = randint(0,100)
        if rval < p:
            fname_lock.acquire()
            if len(fnames) > 0:
                global totaldeletecount
                global d_time
                if len(fnames) == 1:
                    fname_lock.release()
                    if time.time() - s >= etime:
                        return
                    else:
                        continue
                else:
                    index = randint(0, len(fnames)-1)
                name = fnames[index]
                cmd = "rm "+name
                print cmd
                totaldeletecount += 1
                a = time.time()
                os.system(cmd)
                d_time += (time.time()-a)
                del fnames[index]
            fname_lock.release()
        if time.time() - s >= etime:
            return

def Read (p, path, etime):
    s = time.time()
    global fname_lock
    while True:        
        global fnames
        rval = randint(0,100)
        if rval < p:
            fname_lock.acquire()
            if len(fnames) > 0:
                #print "reading"
                global totalreadcount
                global r_time
                totalreadcount += 1
                index = 0
                if len(fnames) > 1:
                    index = randint(0, len(fnames)-1)
                name = fnames[index]
                cmd = "cat "+name
                print cmd
                a = time.time()
                os.system(cmd)
                r_time += (time.time()-a)
            fname_lock.release()
        #else:
            #print rval
        #print "Time ran: ",time.time() - s
        if time.time() - s >= etime:
            return

def main(path, workload, etime=300, totalsize=10000):

    if totalsize < 0:
        totalsize = 9999999
    if workload == "Reads" or workload == "read" or workload == "reads" or workload == "Read":
        thread.start_new_thread (WritesThread, (20, path, totalsize, etime))
        thread.start_new_thread (DeletesThread, (5, path, etime))
        Read (80, path, etime)

    elif workload == "Writes" or workload == "writes" or workload == "Write" or workload == "write":
        thread.start_new_thread (WritesThread, (80, path, totalsize, etime))
        thread.start_new_thread (DeletesThread, (5, path, etime))
        Read (20, path, etime)

    elif workload == "Half" or workload == "half":
        thread.start_new_thread (WritesThread, (50, path, totalsize, etime))
        thread.start_new_thread (DeletesThread, (5, path, etime))
        Read (50, path, etime)
    else:
        return

    time.sleep(etime)
    print "\n------------------------------ Stats ------------------------------"
    print "\nWrite count = ", totalwritecount, ", time = ", w_time, " seconds"
    print "Read count = ", totalreadcount, ", time = ", r_time, " seconds"
    print "Delete count = ", totaldeletecount, ", time = ", d_time, " seconds\n"

if __name__ == '__main__':

    if len(sys.argv) == 2:
        if sys.argv[1] == "--help":
            print "\nUsage: python filegen.py [parameters] [optional parameters]"
            print "Parameters: out_path, workload"
            print "Optional Parameners: run_time, total_size"
            print "Note: All parameners are to be given in the same order as mentioned above"
            print "\t\tOR"
            print "python filegen.py --help"
            print "\nParameters"
            print "-------------------------------------------------------------------------------------"
            print "out_path: Path were operations are to be done"
            print "run_time: Run time of script in seconds, defaults to 5 min"
            print "          Script will run for this much time for sure"
            print "total_size: Total disk size that can be used by script in MB, defaults to 10GB"
            print "            If this value is less than or wqual to 0, then value = 999999999 (1000TB)"
            print "workload: One of read, write or half\n"
            print "Workloads"
            print "------------------------------"
            print "read:    80% reads, 20% writes"
            print "write:   20% reads, 80% writes"
            print "half:    50% reads, 50% writes"
            print "Note: All of the workloads include 0.05% deletes"
            print "------------------------------"
            print "\nType of files written"
            print "------------------------------"
            print "Video:   50 MB to 5000 MB"
            print "Music:   1 MB to 10 MB"
            print "Image:   10 KB to 5000 KB"
            print "Archive: 10 MB to 1000 MB"
            print "doc:     1 KB to 2000 KB\n"
            exit()
        else:
            print "\nInvalid usage: Use --help option for more details\n"
            exit()

    if len(sys.argv) < 3 or len(sys.argv) > 5:
        print "\nInvalid usage: Use --help option for more details\n"
        exit()

    if len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2])
    elif len(sys.argv) == 4:
        main(sys.argv[1], sys.argv[2], int(sys.argv[3]))
    elif len(sys.argv) == 5:
        main(sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]))
