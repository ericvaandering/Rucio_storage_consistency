#! /usr/bin/env python3

import sys, glob, json, time, os, gzip
from datetime import datetime

Path = "/var/cache/consistency-dump/"

def parse_filename(fn):
    # filename looks like this:
    #
    #   <rse>_%Y_%m_%d_%H_%M_<type>.<extension>
    #
    fn, ext = fn.rsplit(".",1)
    parts = fn.split("_")
    typ = parts[-1]
    timestamp_parts = parts[-6:-1]
    timestamp = "_".join(timestamp_parts)
    rse = "_".join(parts[:-6])
    return rse, timestamp, typ, ext

def list_rses():
    files = glob.glob(f"{Path}/*_stats.json")
    rses = set()
    for path in files:
        fn = path.rsplit("/",1)[-1]
        rse, timestamp, typ, ext = parse_filename(fn)
        rses.add(rse)
    return sorted(list(rses))

def list_runs(rse, nlast=4):
    files = glob.glob(f"{Path}/{rse}_*_stats.json")
    print(files)
    runs = []
    for path in files:
        fn = path.rsplit("/",1)[-1]
        if os.stat(path).st_size > 0:
            r, timestamp, typ, ext = parse_filename(fn)
            if r == rse:
                # if the RSE was X, then rses like X_Y will appear in this list too, 
                # so double check that we get the right RSE
                runs.append(timestamp)
    return sorted(runs, reverse=True)[-nlast:]

print(list_rses())
print(list_runs('T2_US_Purdue',12))
