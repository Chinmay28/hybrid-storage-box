#!/bin/bash
import os
from random import randint
import time
from collections import defaultdict
import math
import subprocess
import sys

#media, doc, archive - zip & tar & iso

music = ['.aif','.cda','.mid','.mp3','.mpa','.ogg','.wav','.wma']
video = ['.3gp','.avi','.flv','.h264','.m4v','.mkv','.mov','.mp4','.mpg','.rm','.swf','.vob','.wmv']
archive = ['.7z','.arj','.deb','.pkg','.rar','.rpm','.tar','.zip','.dmg','.iso','.toast','.vcd']
image = ['.ai','.bmp','.gif','.ico','.jpe','.png','.ps','.psd','.svg','.tif']
doc = ['.key','.odp','.pps','.ppt','.pptx','.c','.C','.class','.cpp','.cs','.h','.java','.sh','.swift','.vb','.py','.ods','.xlr','.xls','.xlsx','.doc','.odt','.pdf','.rtf','.tex','.txt','.wks','.wpd','.docx']

def func (fname, size, units, array):
    index = randint(0,len(array))
    i = index%len(array)
    filename = fname+array[i]
    cmd = "bash mkfile " + filename + " " + str(size) + units
    os.system(cmd)

def main(path, totalsize, workload):
    total = 0
    count = 0
    while True:
        if workload == "music" or workload == "Music":
            #music
            size = randint(1, 10)
            func(path+"mfile"+str(count), size, "M", music)
        elif workload == "text" or workload == "doc" or workload == "docs" or workload == "source":
            #doc
            size = randint(1, 2000)
            func(path+"dfile"+str(count), size, "K", doc)
        elif workload == "image" or workload == "Image":
            #image
            size = randint(10, 5000)
            func(path+"ifile"+str(count), size, "K", image)
        elif workload == "video" or workload == "Video":
            #video
            size = randint(50, 5000)
            func(path+"vfile"+str(count), size, "M", video)
        elif workload == "archive" or workload == "Archive" or workload == "backup" or workload == "compressed":
            #ziped
            size = randint(10, 1000)
            fname(path+"afile"+str(count), size, "M", archive)
        else:
            break

        count += 1
        total += size
        if total >= totalsize:
            break


if __name__ == '__main__':
    if len(sys.argv) < 4:
        print "\nProvide out path, total size (in KB or MB as per workload) and workload"
        print "\nWorkloads"
        print "-------------------------"
        print "Video:   50 MB to 5000 MB"
        print "Music:   1 MB to 10 MB"
        print "Image:   10 KB to 5000 KB"
        print "Archive: 10 MB to 1000 MB"
        print "doc:     1 KB to 2000 KB"
        exit()
    main(sys.argv[1], int(sys.argv[2]), sys.argv[3])
