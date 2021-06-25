#! /usr/bin/env python3

import sys, glob, time, os
from part import PartitionedList
from stats import Stats

Version = "1.0"


def cmp2dark(new_list="T2_US_Purdue_2021_06_18_02_28_D.list", old_list="T2_US_Purdue_2021_06_17_02_28_D.list", comm_list="out_D.list", stats_file="test_stats.json"):

    t0 = time.time()
    stats_key = "cms2dark"
    my_stats = stats = None
    op = "and"

    a_list = PartitionedList.open(files=[new_list])
    b_list = PartitionedList.open(files=[old_list])
    out_list = PartitionedList.create_file(comm_list)
        
    if stats_file is not None:
        stats = Stats(stats_file)
        my_stats= {
            "version": Version,
            "elapsed": None,
            "start_time": t0,
            "end_time": None,
            "new_list_files": 0,
            "old_list_files": 0,
            "join_list_files": 0,
            "operation":    op,
            "new_list": a_list.FileNames,
            "old_list": b_list.FileNames,
            "out_list": out_list.FileNames,
            "status": "started"
        }
        stats[stats_key] = my_stats

    n_a_files = 0
    n_b_files = 0
    n_out_files = 0
    
    for pa, pb in zip(a_list.parts(), b_list.parts()):
        b_set = set(pb)
        n_b_files += len(b_set)
        for f in pa:
            n_a_files += 1
            if op == "and":
                if f in b_set:
                    out_list.add(f)
                    n_out_files += 1
            elif op == "minus":
                if not f in b_set:
                    out_list.add(f)
                    n_out_files += 1
            elif op == "xor":
                if f in b_set:
                    b_set.remove(f)
                else:
                    out_list.add(f)
                    n_out_files += 1
            elif op == "or":
                if f in b_set:
                    b_set.remove(f)
                out_list.add(f)
                n_out_files += 1                
        if op in ("or", "xor"):
            for f in b_set:
                out_list.add(f)
                n_out_files += 1
                
    t1 = time.time()
    
    if stats_file:
        my_stats.update({
            "elapsed": t1-t0,
            "end_time": t1,
            "new_list_files": n_a_files,
            "old_list_files": n_b_files,
            "join_list_files": n_out_files,
            "status": "done"
        })
        stats[stats_key] = my_stats
        
