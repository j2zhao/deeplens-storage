"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

storage_partition partitions the video clips in deeplens between internal
and external storage

"""

import numpy as np
from dlstorage.tieredsystem.tiered_manager import *
from dlstorage.tieredsystem.tiered_videoio import file_get
import copy
STORAGE_SIZE = 10
BLOCK_SIZE = 100000

#gets a header of a particular file


def _fetch_item_info(manager, item, seq):
    ''' Sets the value of the clips and 
    find the size
    '''
    base = manager.basedir
    size = 0
    physical_clip = os.path.join(base, item)
    header = {}
    failure = True
    try:
        file = add_ext(physical_clip, '.seq', seq) 
        size += sum(os.path.getsize(os.path.join(file,f)) for f in os.listdir(file))
        header, _ , _ , _= file_get(file)
        failure = False
    except FileNotFoundError:
        pass
    try:
        file = add_ext(physical_clip, '.ref', seq)
        header, ref, _ , _ = file_get(file)
        extern_dir = read_ref_file(ref)
        size += sum(os.path.getsize(os.path.join(extern_dir,f)) for f in os.listdir(extern_dir))
        failure = False
    except FileNotFoundError:
        pass
    if failure:
        return None
    val = header['access_frequency']
    return (int(val), int(size))


def _fetch_all_items(manager, size = BLOCK_SIZE):
    items  = []
    for item in manager.list():
        seq = 0
        while True:
            name = item
            info = _fetch_item_info(manager, item, seq) # note: using a different value here can change the value
            if info == None:
                break
            else:
                val, weight = info
            weight = (weight//size) + 1 
            items.append({'name': name, 'weight': weight, 'value': val, 'seq': seq})
            seq += 1
    return items

def _partition(items, W = STORAGE_SIZE):
    """ Implementations of dynamic programming solution
    to the knapsack problem 

    time complexity: O(nW)
    storage complexity: O(nW)
    (the storage requirement might be a huge limiting factor)

    Arguments:
    items: [{name: string, weight: int, value: int}]
    W: capacity
    """
    n = len(items)
    m = np.zeros((n + 1, W + 1))
    past_partition = [[] for x in range(n + 1)]
    for i in range(n + 1):
        past_partition[i] = [[] for x in range(W + 1)]
    for i in range(1, n + 1):
        for j in range(W + 1):
            item = items[i - 1]
            if item['weight'] > j:
                m[i, j] = m[i-1, j]
                past_partition[i][j] = past_partition[i- 1][j]
                
            else:
                v1 = m[i-1, j]
                v2 = m[i-1, j - item['weight']] + item['value']
                if v1 > v2:
                    m[i, j] = v1
                    past_partition[i][j] = past_partition[i- 1][j]
                else:
                    m[i, j] = v2
                    past_partition[i][j] = copy.deepcopy(past_partition[i-1][j - item['weight']])
                    past_partition[i][j].append([item['name'], item['seq']])
    return (m[n - 1, W], past_partition[n][W])

def _reassign(manager, partitioned_items):
    for item in manager.list():
        manager.moveToExtern(item, lambda hd: True)
    for item in partitioned_items:
        manager.moveFromExtern(item[0], lambda hd: hd['seq'] == item[1])

def storage_partition(storage_manager):
    """ Automatically partitions the files based on size
    and some value parameter

    Argument:
    storage_manager: instance of TieredStorageManager
    """
    items = _fetch_all_items(storage_manager, size = BLOCK_SIZE)
    print(items)
    val, partition = _partition(items)
    print(val)
    print(partition)
    _reassign(storage_manager, partition)