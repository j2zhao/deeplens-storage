import intervaltree
import numpy as np

"""
a simple cost estimation function
We will implement a more sophisticated one later
"""
def estimate_Cost(i1, ilst, cl):
    """
    i1: interval to be added or merged
    ilst: list of intervals that were retrieved and merged before
    cl: the cost of merging these previous intervals
    """
    base_ccost = 10
    merge_cost = 20
    if len(ilst) == 0: #single interval, no merging to be done
        return 0
    if i1[0] - ilst[-1].end < 0:
        crop_cost = abs(i1[0] - ilst[-1].end) * base_ccost
    else:
        crop_cost = 0
    tot_cost = crop_cost + merge_cost + cl
    return tot_cost

#insert the clips into an interval tree
#and then find all intervals overlapping with the given one
def clips_in_range(clips, i1):
    """
    clips: a list of pairs representing intervals
    Later on, we'll want to replace this with the actual header + clip file
    structure, and create the intervals first.
    """
    t = IntervalTree(Interval(begin, end, "%d-%d" % (begin, end)) for begin, end in clips)
    tset = t.overlap(i1[0],i1[1])
    tlst = sorted(list(tset), key=lambda x: x.begin, reverse=True)
    return tlst

def find_min_cost(clips, i1):
    sclips = clips_in_range(clips, i1)
    
    #organize into hashmap
    H = {}
    minStart = int('inf')
    maxEnd = -1
    
    indexed = zip(range(len(sclips), -1, -1), sclips)
    for i,c in indexed:
        if c.begin < minStart:
            minStart = c.begin
        if c.end > maxEnd:
            maxEnd = c.end
        if not (c in H):
            H[c.begin] = list()
        H[c.begin].append((i,c.end))
    
    cliplst = []
    memo = np.zeros((len(sclips),len(sclips)))
    
    i = maxEnd
    clip_no = 0
    tot_cost = 0
    while i >= minStart:
        if H[i] == None or len(H[i]) == 0:
            continue
        else:
            clip_no += 1
            min_cost = int('inf')
            argmin_c = None
            for j,s in H[i]:
                m_cost = estimate_Cost([i,s], cliplst, tot_cost)
                tmp_cost = tot_cost + m_cost
                memo[j][clip_no] = tmp_cost
                if tmp_cost < min_cost:
                    min_cost = tmp_cost
                    argmin_c = j
            tot_cost += min_cost
            memo[argmin_c][clip_no] = tot_cost
            cliplst.append(sclips[argmin_c])
        i -= 1
    return tot_cost,cliplst

ivs = [(0,3),(3,7),(7,11),(11,15),(15,18),(18,20),(0,5),(5,7),(7,10),(10,14),(14,17),(17,20)]
i1 = [3,17]
tcost,tlst = find_min_cost(tcost,tlst)
print("Total Cost: " + str(tcost))
print("List of Intervals: ")
print(tlst)