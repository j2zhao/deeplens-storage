"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

tiered_file.py contains the basic file I/O methods in the deeplens storage manager. It
provies the basic routines for compressing and formatting video files on a tiered
storage system.
   
The basic file format is as follows:
* headers (labels/bounding boxes of objects in a particular frame), compressed 
in gzip or bzip2
* video (encoded in a supported encoding format)"""

import pickle
import string
import os 
import tarfile
import random
import cv2
from dlstorage.filesystem.file import *

#import all of the constants
from dlstorage.constants import *

def is_ref_name(name):
    ext_name = name.split('/')[-1].split('.')[1]
    if ext_name == 'ref':
        return True
    else:
        return False

#def get_ref_name(name):
#    base_name = name.split('/')[-1].split('.')[0]
#    ref_name = add_ext(name, '.ref')
 #   return ref_name

def write_ref_file(ref_file, file_name):
    f = open(ref_file, "w")
    f.write(file_name)
    f.close()

def read_ref_file(ref_file):
    with open(ref_file, "r") as f:
        ref = f.readline()
        return ref


def delete_ref_file(ref_file):
    if os.path.exists(ref_file):
        os.remove(ref_file)
        return True
    else:
        return False