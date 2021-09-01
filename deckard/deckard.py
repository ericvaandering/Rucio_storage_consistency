import sys, glob, json, time, os, gzip, re
from datetime import datetime
from config import Config
from cmp2dark import  cmp2dark
from stats import Stats


# import DeleteReplicas
import csv
from rucio.common import exception
from rucio.common.types import InternalAccount, InternalScope
from rucio.core.replica import __exists_replicas, update_replicas_states
from rucio.core.quarantined_replica import add_quarantined_replicas
from rucio.core.rse import get_rse_id
from rucio.db.sqla import models
from rucio.db.sqla.constants import (ReplicaState, BadFilesStatus)
from rucio.db.sqla.session import transactional_session
from sqlalchemy.exc import DatabaseError, IntegrityError
from sqlalchemy.orm.exc import FlushError

# Adapted from add_quarantined_replicas in core/quarantined_replica.py
# 'path' is not used by CMS, only 'scope' and 'name'

#import datetime

from sqlalchemy import and_, or_, exists, not_
from sqlalchemy.sql.expression import select, false

from rucio.common.utils import chunks
from rucio.db.sqla import models, filter_thread_work
from rucio.db.sqla.session import read_session, transactional_session

from rucio.rse.rsemanager import lfns2pfns, get_rse_info, parse_pfns
from rucio.core.rse import get_rse_protocols



# Adapted from __declare_bad_file_replicas in core/replica.py and removing the ATLAS PFN to LFN translation and the stuff for non-deterministic RSEs


@transactional_session
def declare_bad_file_replicas(dids, rse_id, reason, issuer, status=BadFilesStatus.BAD, scheme='srm', session=None):
    """
    Declare a list of bad replicas.

    :param dids: The list of DIDs.
    :param rse_id: The RSE id.
    :param reason: The reason of the loss.
    :param issuer: The issuer account.
    :param status: Either BAD or SUSPICIOUS.
    :param scheme: The scheme of the PFNs.
    :param session: The database session in use.
    """
    unknown_replicas = []
    replicas = []
    if True:
        for did in dids:
            scope = InternalScope(did['scope'], vo=issuer.vo)
            name = did['name']
            __exists, scope, name, already_declared, size = __exists_replicas(rse_id, scope, name, path=None,
                                                                              session=session)
            if __exists and ((str(status) == str(BadFilesStatus.BAD) and not already_declared) or str(status) == str(
                    BadFilesStatus.SUSPICIOUS)):
                replicas.append({'scope': scope, 'name': name, 'rse_id': rse_id, 'state': ReplicaState.BAD})
                new_bad_replica = models.BadReplicas(scope=scope, name=name, rse_id=rse_id, reason=reason, state=status,
                                                     account=issuer, bytes=size)
                new_bad_replica.save(session=session, flush=False)
                session.query(models.Source).filter_by(scope=scope, name=name, rse_id=rse_id).delete(
                    synchronize_session=False)
            else:
                if already_declared:
                    unknown_replicas.append('%s:%s %s' % (did['scope'], did['name'], 'Already declared'))
                else:
                    unknown_replicas.append('%s:%s %s' % (did['scope'], did['name'], 'Unknown replica'))
        if str(status) == str(BadFilesStatus.BAD):
            # For BAD file, we modify the replica state, not for suspicious
            try:
                # there shouldn't be any exceptions since all replicas exist
                update_replicas_states(replicas, session=session)
            except exception.UnsupportedOperation:
                raise exception.ReplicaNotFound("One or several replicas don't exist.")
    try:
        session.flush()
    except IntegrityError as error:
        raise exception.RucioException(error.args)
    except DatabaseError as error:
        raise exception.RucioException(error.args)
    except FlushError as error:
        raise exception.RucioException(error.args)

    return unknown_replicas



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


def list_runs_by_age(rse, reffile):
    files = glob.glob(f"{Path}/{rse}_*_stats.json")
    #print(files)
    r, reftimestamp, typ, ext = parse_filename(reffile)
    #print("reftimestamp", reftimestamp)
    reftime = datetime.strptime(reftimestamp,'%Y_%m_%d_%H_%M')
    #print("reftime", reftime)
    runs = {}
    for path in files:
        fn = path.rsplit("/",1)[-1]
        if os.stat(path).st_size > 0:
            r, timestamp, typ, ext = parse_filename(fn)
            #print("timestamp", timestamp)
            filetime = datetime.strptime(timestamp,'%Y_%m_%d_%H_%M')
            #print("filetime", filetime)
            fileagedays = (reftime - filetime).days
            #print("fileagedays ", fileagedays)
            if r == rse:
                # if the RSE was X, then rses like X_Y will appear in this list too,
                # so double check that we get the right RSE
                runs.update({path: fileagedays})
    
    #return sorted(runs.items(), reverse=True) 
    return {k:v for k,v in sorted(runs.items(), reverse=True)} 



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
#    return sorted(runs, reverse=True)[-nlast:]
    return sorted(runs, reverse=False)[-nlast:]

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
    #print("opts: ",opts)
    #[print(key,':',value) for key, value in opts.items()]

    if args:
        print("\n  don't know what to so with the extra arguments: ", args)
        print (Usage)
        sys.exit(2) 
    
    config = Config(opts["-c"])

    rse = opts.get("-r")
    logging="-l" in opts
    debug=sys.stdout if "-d" in opts else None
    force_proceed=True if "-f" in opts else False
    print("force_proceed is:",force_proceed)

    print("Starting deckard for RSE: ",rse," with config: ",config)

# Read config parameters for this RSE from the yaml file

    #print("test minagedark",config.general_param(rse,"minagedark"))

    Path = config.general_param(rse,"scannerdir")
    minagedark = config.general_param(rse,"minagedark")
    maxdarkfraction = config.general_param(rse,"maxdarkfraction")
    maxmissfraction = config.general_param(rse,"maxmissfraction")
    print("\n Scanner Output Path: ",Path,"\n minagedark: ",minagedark,"\n maxdarkfraction: ",maxdarkfraction,"\n maxmissfraction: ",maxmissfraction,"\n")

    sys.stdout.flush()
    home = os.path.dirname(__file__) or "."

###
# First, check that the RSE has been scanned at all
###

# Check if we have any scans available for that RSE
    if rse in list_rses():
        print("Found scans for RSE: ",rse)
        #[print(run) for run in (list_runs(rse))]

# Have any of them still not been processed? 
# (no CC_dark or CC-miss sections in _stats.json)
        #[print(run) for run in (list_unprocessed_runs(rse))]
        np_runs = list_unprocessed_runs(rse)
        print(len(np_runs)," unprocessed runs found for this RSE")

# Was the latest run ever attempted to be processed?

        latest_run = list_runs(rse,1)[0]
        print("Was the latest run", latest_run, "attempted to be processed already? ", was_cc_attempted(latest_run))
        if was_cc_attempted(latest_run) is False or force_proceed is True:
            print("Will try to process the run")
### 
# Address the Dark files first
###

# Is there another run, at least "minagedark" old, for this RSE?
            #print(list_runs_by_age(rse, latest_run))
            #print(type(list_runs_by_age(rse, latest_run)))
            d = list_runs_by_age(rse, latest_run)
            #print([(k,d[k]) for k in d if d[k] > minagedark])
            if len([k for k in d if d[k] > minagedark]) > 0:    # there is another dark run with appropriate age
                oldenough_run = [k for k in d if d[k] > minagedark][0]
                print("Found another run,",minagedark,"days older than the latest!\nWill compare the dark files in the two.") 
                print("The first",minagedark,"days older run is: ", oldenough_run)

# Create a cc_dark section in the stats file

                t0 = time.time()
                stats_key = "cc_dark"
                cc_stats = stats = None
                stats = Stats(latest_run)
                cc_stats= {
                    "start_time": t0,
                    "end_time": None,
                    "initial_dark_files": 0,
                    "confirmed_dark_files": 0,
                    "x-check_run": oldenough_run,
                    "status": "started"
                }
                stats[stats_key] = cc_stats

# Compare the two lists, and take only the dark files that are in both
                latest_dark = re.sub('_stats.json$', '_D.list', latest_run)
                oldenough_dark = re.sub('_stats.json$', '_D.list', oldenough_run)
                print("\nlatest_dark =",latest_dark)
                print("oldenough_dark =",oldenough_dark)
                csvfilename = "%s_DeletionList.csv" % latest_run
                csvfilename = re.sub('_stats.json$', '_DeletionList.csv', latest_run) 
                cmp2dark(new_list=latest_dark, old_list=oldenough_dark, comm_list=csvfilename, stats_file=latest_run)
                #cmp2dark(new_list=latest_dark, old_list=oldenough_dark, comm_list="out_D.list", stats_file="test_stats.json")
                #cmp2dark(new_list="T2_US_Purdue_2021_06_18_02_28_D.list", old_list="T2_US_Purdue_2021_06_17_02_28_D.list", comm_list="out_D.list", stats_file="test_stats.json")
###
#   SAFEGUARD
#   If a large fraction (larger than 'maxdarkfraction') of the files at a site are reported as 'dark', do NOT proceed with the deletion.
#   Instead, put a warning in the _stats.json file, so that an operator can have a look.
###          

# Get the number of files recorded by the scanner
                print("latest_run",latest_run)
                with open(latest_run, "r") as f:
                    fstats = json.loads(f.read())
                    if "scanner" in fstats:
                        scanner_stats = fstats["scanner"]
                        if "total_files" in scanner_stats:
                            scanner_files = scanner_stats["total_files"]
                        else:
                            scanner_files = 0
                            for root_info in scanner_stats["roots"]:
                                scanner_files += root_info["files"]
                    if "dbdump_before" in fstats:
                        dbdump_before_files = fstats["dbdump_before"]["files"]
                    if "dbdump_after" in fstats:
                        dbdump_after_files = fstats["dbdump_after"]["files"]
                max_files_at_site = max(scanner_files,dbdump_before_files,dbdump_after_files)
                print("\nscanner_files: ",scanner_files,"\ndbdump_before_files",dbdump_before_files,"\ndbdump_after_files",dbdump_after_files,"\nmax_files_at_site",max_files_at_site)        

                dark_files = sum(1 for line in open(latest_dark))
                print("\ndark_files",dark_files)
                print("dark_files/max_files_at_site = ",dark_files/max_files_at_site)
                print("maxdarkfraction configured for this RSE: ",maxdarkfraction)

                if dark_files/max_files_at_site < maxdarkfraction or force_proceed is True:
                    print("Can proceed with dark files deletion")

# Then, do the real deletion (code from DeleteReplicas.py)
# ref:
# https://github.com/rucio/rucio/blob/a4c05a1efd0525fef9bd9d9b1d9e9d2ad66d51cf/lib/rucio/core/quarantined_replica.py#L35
# https://github.com/rucio/rucio/blob/master/lib/rucio/daemons/auditor/__init__.py#L194

                    deleted_files = 0
                    issuer = InternalAccount('root')
                    #with open('dark_files.csv', 'r') as csvfile:
                    with open(csvfilename, 'r') as csvfile:
                        reader = csv.reader(csvfile)
                        dark_replicas = []
                        #for rse, scope, name, reason in reader:
                        scope = "cms"
                        reason = "deleteing dark file"
                        for name, in reader:
                            print("\n Processing dark file:\n RSE: ",rse," Scope: ",scope," Name: ",name)
                            rse_id = get_rse_id(rse=rse)
                            Intscope = InternalScope(scope=scope, vo=issuer.vo)
                            lfns = [{'scope': scope, 'name': name}]

                            attributes = get_rse_info(rse=rse)
                            pfns = lfns2pfns(rse_settings=attributes, lfns=lfns, operation='delete')
                            pfn_key = scope + ':' + name
                            url = pfns[pfn_key]
                            urls = [url]
                            paths = parse_pfns(attributes, urls, operation='delete')
                            replicas = [{'scope': Intscope, 'rse_id': rse_id, 'name': name, 'path': paths[url]['path']+paths[url]['name']}]
#                            replicas = [{'scope': Intscope, 'rse_id': rse_id, 'name': name, 'path': url}]
                            add_quarantined_replicas(rse_id, replicas, session=None)
                            deleted_files += 1

                    #Update the stats
                    t1 = time.time()

                    cc_stats.update({
                        "end_time": t1,
                        "initial_dark_files": dark_files,
                        "confirmed_dark_files": deleted_files,
                        "status": "done"
                    })
                    stats[stats_key] = cc_stats

                else:
                    darkperc = 100.*dark_files/max_files_at_site
                    print("\nWARNING: Too many DARK files! (%3.2f%%) \nStopping and asking for operator's help." % darkperc)

                    #Update the stats
                    t1 = time.time()

                    cc_stats.update({
                        "end_time": t1,
                        "initial_dark_files": dark_files,
                        "confirmed_dark_files": 0,
                        "status": "ABORTED",
                        "aborted_reason": "%3.2f%% dark" % darkperc,
                    })
                    stats[stats_key] = cc_stats

            else:
                print("There's no other run for this RSE at least",minagedark,"days older, so cannot safely proceed with dark files deleteion.")
###
#   Done with Dark Files processing
###

### 
# Finally, deal with the missing replicas
###

            latest_miss = re.sub('_stats.json$', '_M.list', latest_run)
            print("\n\nlatest_missing =",latest_miss)   

# Create a cc_miss section in the stats file

            t0 = time.time()
            stats_key = "cc_miss"
            cc_stats = stats = None
            stats = Stats(latest_run)
            cc_stats= {
                "start_time": t0,
                "end_time": None,
                "initial_miss_files": 0,
                "confirmed_miss_files": 0,
                "x-check_run": oldenough_run,
                "status": "started"
            }
            stats[stats_key] = cc_stats
###
#   SAFEGUARD
#   If a large fraction (larger than 'maxmissfraction') of the files at a site are reported as 'missing', do NOT proceed with the invalidation.
#   Instead, put a warning in the _stats.json file, so that an operator can have a look.
###

            miss_files = sum(1 for line in open(latest_miss))
            print("\nmiss_files",miss_files)
            print("miss_files/max_files_at_site = ",miss_files/max_files_at_site)
            print("maxmissfraction configured for this RSE: ",maxmissfraction)

            if miss_files/max_files_at_site < maxmissfraction or force_proceed is True:
                print("Can proceed with missing files retransfer")

                invalidated_files = 0
                issuer = InternalAccount('root')
                #with open('bad_replicas.csv', 'r') as csvfile:
                with open(latest_miss, 'r') as csvfile:
                    reader = csv.reader(csvfile)
                    #for rse, scope, name, reason in reader:
                    scope = "cms"
                    reason = "invalidating damaged/missing replica"
                    for name, in reader:
                        print("\n Processing invalid replica:\n RSE: ",rse," Scope: ",scope," Name: ",name,"\n")
                
                        rse_id = get_rse_id(rse=rse)
                        dids = [{'scope': scope, 'name': name}]
                        declare_bad_file_replicas(dids=dids, rse_id=rse_id, reason=reason, issuer=issuer)
                        invalidated_files += 1

                    #Update the stats
                    t1 = time.time()

                    cc_stats.update({
                        "end_time": t1,
                        "initial_miss_files": miss_files,
                        "confirmed_miss": invalidated_files,
                        "status": "done"
                    })
                    stats[stats_key] = cc_stats

            else:
                missperc = 100.*miss_files/max_files_at_site 
                print("\nWARNING: Too many MISS files (%3.2f%%)! \nStopping and asking for operator's help." % missperc)

                #Update the stats
                t1 = time.time()

                cc_stats.update({
                    "end_time": t1,
                    "initial_miss_files": miss_files,
                    "confirmed_miss_files": 0,
                    "status": "ABORTED",
                    "aborted_reason": "%3.2f%% miss" % missperc,
                })
                stats[stats_key] = cc_stats


###
#   Done with Missing Replicas processing
###

        else:
# This run was already processed
         print("Nothing to do here")

    else:
# No scans outputs are available for this RSE
        assert False, "no scans available for this RSE"
