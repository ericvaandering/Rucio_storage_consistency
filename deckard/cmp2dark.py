#! /usr/bin/env python3

import sys, glob, time, os
from stats import Stats


def cmp2dark(new_list="T2_US_Purdue_2021_06_18_02_28_D.list", old_list="T2_US_Purdue_2021_06_17_02_28_D.list", comm_list="out_D.list", stats_file="test_stats.json"):

    t0 = time.time()
    stats_key = "cmp2dark"
    my_stats = stats = None
    op = "and"

    with open(new_list,"r") as a_list, open(old_list,"r") as b_list, open(comm_list,"w") as out_list:
        if stats_file is not None:
            stats = Stats(stats_file)
            my_stats= {
                "elapsed": None,
                "start_time": t0,
                "end_time": None,
                "new_list": new_list,
                "old_list": old_list,
                "out_list": out_list.name,
                "status": "started"
            }
            stats[stats_key] = my_stats

        a_set = set(line.strip() for line in a_list)
        b_set = set(line.strip() for line in b_list)
   

# The intersection of the two sets is what can be deleted
        out_set = a_set & b_set
#        print("\n\n\n\n out_set: \n\n\n", "\n".join( sorted(list(out_set)) ))

        out_list.writelines("\n".join(sorted(list(out_set))))

    t1 = time.time()

    if stats_file:
        my_stats.update({
            "elapsed": t1-t0,
            "end_time": t1,
            "status": "done"
        })
        stats[stats_key] = my_stats

