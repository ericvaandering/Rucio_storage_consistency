import sys, glob, json, time, os, gzip
from datetime import datetime
from config import Config

# Path = "/var/cache/consistency-dump/"
# Path = "/tmp/consistency-dump/"

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

def list_runs(rse, nlast=0):
    files = glob.glob(f"{Path}/{rse}_*_stats.json")
#    print(files)
    runs = []
    for path in files:
        fn = path.rsplit("/",1)[-1]
        if os.stat(path).st_size > 0:
            r, timestamp, typ, ext = parse_filename(fn)
            if r == rse:
                # if the RSE was X, then rses like X_Y will appear in this list too, 
                # so double check that we get the right RSE
#                runs.append(timestamp)
                runs.append(path)
    if nlast == 0:
        nlast = len(runs)
    return sorted(runs, reverse=True)[-nlast:]

def list_unprocessed_runs(rse, nlast=0):
    files = glob.glob(f"{Path}/{rse}_*_stats.json")
    #print(files)
    unproc_runs = []
    for path in files:
        fn = path.rsplit("/",1)[-1]
        if os.stat(path).st_size > 0:
            r, timestamp, typ, ext = parse_filename(fn)
            if r == rse:
                # if the RSE was X, then rses like X_Y will appear in this list too, 
                # so double check that we get the right RSE
                #print("was_cc_attempted for ",path)
                if not was_cc_attempted(path):
                    unproc_runs.append(timestamp)
    if nlast == 0:
        nlast = len(unproc_runs)
    return sorted(unproc_runs, reverse=True)[-nlast:]

def was_cc_attempted(stats_file):
    #print("get_data: input ",stats_file)
    try:
        f = open(stats_file, "r")
    except:
        print("get_data: error ",stats_file)
        return None
    stats = json.loads(f.read())
    cc_dark_status = ''
    cc_miss_status = ''
    if "cc_dark" in stats or "cc_miss" in stats:
        #print("CC processing was attempted for this run")
        return True
    else:
        #print("CC processing wasn't attempted yet for this run")
        return False

def was_cc_processed(stats_file):
    try:
        f = open(stats_file, "r")
    except:
        print("get_data: error ",stats_file)
        return None
    stats = json.loads(f.read())
    cc_dark_status = ''
    cc_miss_status = ''
    if "cc_dark" in stats:
        if "status" in stats['cc_dark']:
            cc_dark_status = stats['cc_dark']['status']
    if "cc_miss" in stats:
        if "status" in stats['cc_miss']:
            cc_miss_status = stats['cc_miss']['status']
    #print("CC_Dark Status is: ",cc_dark_status, "\nCC_Miss Status is: ",cc_miss_status)
    if cc_dark_status == 'done' or cc_miss_status == 'done':
        #print("Run was CC processed")
        return True
    else:
        #print("Run wasn't CC processed yet")
        return False

# print(list_rses())
# print(list_runs('T2_US_Purdue',12))



Usage = """
Usage: python3 deckard.py -c <config_file.yaml> [-f] -r <RSE> 
"""



if __name__ == "__main__":
    import sys, getopt

    #print("deckard.py: sys.argv:", sys.argv)

    try:
        opts, args = getopt.getopt(sys.argv[1:], "c:fr:")
    except getopt.GetoptError as err:
        print(err)
        print (Usage)
        sys.exit(2) 
    opts = dict(opts)
#    print("opts: ",opts)
#    [print(key,':',value) for key, value in opts.items()]

    if args:
        print("\n  don't know what to so with the extra arguments: ", args)
        print (Usage)
        sys.exit(2) 
    
#    config = opts.get("-c")
    config = Config(opts["-c"])

    rse = opts.get("-r")
    logging="-l" in opts
    debug=sys.stdout if "-d" in opts else None

    print("Starting deckard for RSE: ",rse," with config: ",config)

# read config parameters for this RSE from the yaml file
    #print("test minagedark",config.general_param(rse,"minagedark"))

    Path = config.general_param(rse,"scannerdir")
    minagedark = config.general_param(rse,"minagedark")
    maxdarkfraction = config.general_param(rse,"maxdarkfraction")
    maxmissfraction = config.general_param(rse,"maxmissfraction")
    print("\n Scanner Output Path: ",Path,"\n minagedark: ",minagedark,"\n maxdarkfraction: ",maxdarkfraction,"\n maxmissfraction: ",maxmissfraction,"\n")

    sys.stdout.flush()
    home = os.path.dirname(__file__) or "."

# Check if we have any scans available for that RSE
    if rse in list_rses():
        print("Found scans for RSE: ",rse)
        #[print(run) for run in (list_runs(rse))]

# Have any of them still not been processed? 
# (no CC_dark or CC-miss sections in _stats.json)
        #[print(run) for run in (list_unprocessed_runs(rse))]
        np_runs = list_unprocessed_runs(rse)
        print(len(np_runs)," unprocessed runs found for this RSE")

# Was the latest run attempted to be processed?

        latest_run = list_runs(rse,1)[0]
        #print("Was the latest run: ", latest_run, "\n attempted to be processed already? ", was_cc_attempted(latest_run))
        if was_cc_attempted(latest_run) is False:
         print("Will process")
        else:
         print("Nothing to do here")

    else:
        assert False, "no scans available for this RSE"
