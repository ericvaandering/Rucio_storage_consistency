## from __future__ import print_function

import csv

from rucio.common import exception
from rucio.common.types import InternalAccount, InternalScope
from rucio.core.replica import __exists_replicas, update_replicas_states
from rucio.core.rse import get_rse_id
from rucio.db.sqla import models
from rucio.db.sqla.constants import (ReplicaState, BadFilesStatus)
from rucio.db.sqla.session import transactional_session
from sqlalchemy.exc import DatabaseError, IntegrityError
from sqlalchemy.orm.exc import FlushError

# Adapted from add_quarantined_replicas in core/quarantined_replica.py
# 'path' is not used by CMS, only 'scope' and 'name'

import datetime

from sqlalchemy import and_, or_, exists, not_
from sqlalchemy.sql.expression import select, false

from rucio.common.utils import chunks
from rucio.db.sqla import models, filter_thread_work
from rucio.db.sqla.session import read_session, transactional_session

from rucio.rse.rsemanager import lfns2pfns, get_rse_info, parse_pfns
from rucio.core.rse import get_rse_protocols

@transactional_session
def add_quarantined_replicas(rse_id, replicas, session=None):
    """
    Bulk add quarantined file replicas.
    :param rse_id:      The rse id.
    :param replicas: A list of dicts with the replica information.
    :param session:  The database session in use.
    """

    #print("rse_id: ",rse_id)
    #print("replicas",replicas)
    #print("len(replicas)",len(replicas))

    for chunk in chunks(replicas, 100):
        #print("chunk: ",chunk)
        #print("len(chunk): ",len(chunk))
               
        # Exlude files that have a registered replica.  This is a
        # safeguard against potential issues in the Auditor.
        file_clause = []

        for replica in chunk:
            #print("replica's scope: ", replica.get('scope', None))
            #print("replica's name: ", replica.get('name', None))
            #print("replica's rse_id: ", replica.get('rse_id', None))
            #print("replica's path: ", replica.get('path', None))
            file_clause.append(and_(models.RSEFileAssociation.scope == replica.get('scope', None),
                                    models.RSEFileAssociation.name == replica.get('name', None),
                                    models.RSEFileAssociation.rse_id == rse_id))
            #print("file_clause: ",*file_clause)
        file_query = session.query(models.RSEFileAssociation.scope,
                                   models.RSEFileAssociation.name,
                                   models.RSEFileAssociation.rse_id).\
            with_hint(models.RSEFileAssociation, "index(REPLICAS REPLICAS_PK)", 'oracle').\
            filter(or_(*file_clause))
        #print("file_query: ",file_query)
        existing_replicas = [(scope, name, rseid) for scope, name, rseid in file_query]
        chunk = [replica for replica in chunk if (replica.get('scope', None), replica.get('name', None), rse_id) not in existing_replicas]

        # Exclude files that have already been added to the quarantined
        # replica table.
        quarantine_clause = []
        for replica in chunk:
            quarantine_clause.append(and_(models.QuarantinedReplica.path == replica['path'],
                                          models.QuarantinedReplica.rse_id == rse_id))
        quarantine_query = session.query(models.QuarantinedReplica.path,
                                         models.QuarantinedReplica.rse_id).\
            filter(or_(*quarantine_clause))
        quarantine_replicas = [(path, rseid) for path, rseid in quarantine_query]
        chunk = [replica for replica in chunk if (replica['path'], rse_id) not in quarantine_replicas]
            
        #print("chunk before insertion: ", chunk)
        session.bulk_insert_mappings(
            models.QuarantinedReplica,
            [{'rse_id': rse_id, 'path': file['path'],
              'scope': file.get('scope'), 'name': file.get('name'),
              'bytes': file.get('bytes')} for file in chunk])




issuer = InternalAccount('root')
with open('dark_files.csv', 'r') as csvfile:
    reader = csv.reader(csvfile)
    dark_replicas = []
    for rse, scope, name, reason in reader:
        print(type(name))
        print("\n Processing dark file:\n RSE: ",rse," Scope: ",scope," Name: ",name,"\n")
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
#        replicas = [{'scope': Intscope, 'rse_id': rse_id, 'name': name, 'path': url}]
        add_quarantined_replicas(rse_id, replicas, session=None)
