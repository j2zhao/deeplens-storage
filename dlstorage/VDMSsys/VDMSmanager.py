"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

VDMSmanager.py defines the basic storage api in deeplens for storing in
VDMS, rather than the filesystem.
"""
from dlstorage.core import *
from dlstorage.constants import *
from dlstorage.stream import *
from dlstorage.filesystem.videoio import *
from dlstorage.header import *
from dlstorage.xform import *
from dlstorage.VDMSsys.vdmsio import *

import os
import math
import subprocess
import datetime
import time

class VDMSStorageManager(StorageManager):
    #NOTE: Here, size refers to the duration of the clip, 
    #and NOT the number of frames!
    #Another NOTE: We assume that if a VDMSStorageManager instance is used to 
    #put clips in VDMS, then the same instance must be used to get() the clips
    DEFAULT_ARGS = {'encoding': H264, 'size': -1, 'limit': -1, 'sample': 1.0}
    
    def __init__(self, content_tagger):
        self.content_tagger = content_tagger
        self.clip_headers = []
        self.totalFrames = -1
    
    def put(self, filename, target, args=DEFAULT_ARGS):
        """In this case, put() adds the file to VDMS, along with
        the header, which we might be able to send either as a long string
        of metadata, or as extra properties (which is still a lot of metadata)
        Note: we are going to suffer the performance penalty for acquiring
        header information in a frame-by-frame fashion
        Also Note: target is a dummy variable in this case, for the purposes
        of running the same benchmark
        """
        v = VideoStream(filename, args['limit'])
        v = v[Sample(args['sample'])]
        v = v[self.content_tagger]
        #find the size of the inputted file: if it's greater than 32MB, VDMS is going to have issues,
        #so we'll have to split the file into clips anyway
        fsize = os.path.getsize(filename) / 1000000.0
  
        if args['size'] == -1 and fsize <= 32.0:
            tf, headers = add_video(filename, v, args['encoding'], ObjectHeader())
            self.clip_headers = headers
            self.totalFrames = tf
        elif fsize > 32.0:
            #compute the number of clips we would need for each to be 32 MB
            numClips = math.ceil(fsize / 32.0)
            #find duration of video file to compute clip duration
            result = subprocess.Popen(['ffprobe', filename], \
                                      stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
            dur = [x for x in result.stdout.readlines() if "Duration" in x]
            #convert duration string to seconds
            inf = dur[0].split(',')
            dInfo = "".join(inf[0].split())
            acdur = dInfo[len("Duration:"):]
            x = time.strptime(acdur.split('.')[0], '%H:%M:%S')
            fdur = datetime.timedelta(hours=x.tm_hour,minutes=x.tm_min,seconds=x.tm_sec).total_seconds()
            idur = int(fdur)
            clip_size = idur / numClips
            #load the clips into VDMS
            tf, headers = add_video_clips(filename, v, args['encoding'], ObjectHeader(), clip_size)
            self.clip_headers = headers
            self.totalFrames = tf
        else:
            tf, headers = add_video_clips(filename, v, args['encoding'], ObjectHeader(), args['size'])
            self.clip_headers = headers
            self.totalFrames = tf
    
    def get(self, name, condition, clip_size):
        """
        get() retrieves all the clips with the given name that satisfy the given condition.
        """
        return find_video(name, condition, clip_size, self.clip_headers)
        
