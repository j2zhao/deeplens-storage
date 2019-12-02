"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

storage_partition partitions the video clips in deeplens between internal
and external storage

"""

import numpy as np
from dlstorage.tieredsystem.tiered_manager import *

STORAGE_SIZE = 0
BLOCK_SIZE = 0


def _fetch_item_info(manager, item, seq):
    ''' Sets the value of the 
    '''
    base = manager.basedir
    size = 0
    physical_clip = os.path.join(base, name)
    header = {}
    failure = True
    try:
        file = add_ext(physical_clip, '.seq', seq) 
        size += sum(os.path.getsize(os.path.join(file,f)) for f in os.listdir(file))
        header = read_block(file)
        failure = False
    except FileNotFoundError:
        pass
    try:
        file = add_ext(physical_clip, '.ref', seq) 
        extern_dir = read_ref_file(file)
        size += sum(os.path.getsize(os.path.join(extern_dir,f)) for f in os.listdir(extern_dir))
        header = read_block(file)
        failure = False
    except FileNotFoundError:
        pass
    if failure:
        return None
    val = header['access_frequency']
    return (val, size)


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
            weight = (weight/size) + 1 
            items.append({'name': name, 'weight': weight, 'value': val, 'seq': seq})
            seq += 1
    return items

def _partition(items, W):
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
    m = np.zeros((n, W + 1))
    past_partition = [[] for x in range(n)]
    for i in range(n):
        past_partition[i] = [[] for x in range( W + 1)]
    for i in range(n):
        for j in range(W + 1):
            if items[i]['weight'] > j:
                m[i, j] = m[i-1, j]
                past_partition[i][j] = past_partition[i- 1][j]
            else:
                v1 = m[i-1, j]
                v2 = m[i-1, j - items[i]['weight']] + items[i]['value']
                if v_1 > v_2:
                    m[i, j] = v_1
                    past_partition[i][j] = past_partition[i- 1][j]
                else:
                    m[i, j] = v_2
                    past_partition[i][j] = past_partition[i-1][j - items[i]['weight']].append([items[i]['name'], items[i]['seq']])
    return (m[n - 1, W], past_partition[n - 1][W])

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
    items = _fetch_all_headers(storage_manager)
    val, partition = _partition(items, W)
    print(val)
    _reassign(storage_manager, partition)