#!/usr/bin/env python3.6
# coding: utf8
"""
Basic endpoint wrappers for SPARQL et TPF
"""
#    Copyright (C) 2017 by
#    Emmanuel Desmontils <emmanuel.desmontils@univ-nantes.fr>
#    Patricia Serrano-Alvarado <patricia.serrano-alvarado@univ-nantes.fr>
#    All rights reserved.
#    GPL v 2.0 license.

import re
import json
import logging
import hashlib

import csv
import subprocess
import os.path

from SPARQLWrapper import SPARQLWrapper, JSON #, SPARQLWrapperException
from SPARQLWrapper.Wrapper import QueryResult, QueryBadFormed, EndPointNotFound, EndPointInternalError

#==================================================
#==================================================

class Endpoint:
    def __init__(self, service, cacheDir = '.') :
        self.engine = None
        self.service = service
        self.reLimit = re.compile(r'limit\s*\d+',re.IGNORECASE)
        self.setCacheDir(cacheDir)
        self.cache = dict()
        self.do_cache = False
        self.reSupCom=re.compile(r'#[^>].*$',re.IGNORECASE | re.MULTILINE)

    def loadCache(self):
        if os.path.isfile(self.cacheDir+'/be4dbp.csv') :
            logging.info('Reading cache file')
            with open(self.cacheDir+"/be4dbp.csv","r", encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    self.cache[row['qhash']] = row['nb'] == "True"

    def saveCache(self):
        if self.do_cache:
            logging.info('Writing cache file')
            with open(self.cacheDir+"/be4dbp.csv","w", encoding='utf-8') as f:
                fn=['nb','qhash']
                writer = csv.DictWriter(f,fieldnames=fn)
                writer.writeheader()
                for x in self.cache:
                    writer.writerow({'nb':self.cache[x],'qhash':x}) 

    def setCacheDir(self,cacheDir):
        self.cacheDir = cacheDir

    def caching(self, mode = True):
        if mode:
            self.loadCache()
        else:
            self.saveCache()
            self.cache.clear()
        self.do_cache = mode;

    def query(self, qstr):
        return []

    def is_answering(self, qstr):
        return False

    def hash(self,qstr):
        return hashlib.sha512(qstr.encode('utf-8')).hexdigest()

    def setLimit1(self,query):
        if self.reLimit.search(query):
            nquery = self.reLimit.sub('limit 1',query)
        else:
            nquery = query + ' limit 1 '
        return nquery

    def simplifyQuery(self,query) :
    	if self.reSupCom.search(query):
    		nquery = self.reSupCom.sub('',query)
    	else:
    		nquery = query
    	return ' '.join(nquery.split())

    def notEmpty(self,query):
        #On cherche d'abord dans le cache
        qhash = self.hash(query) 
        if qhash in self.cache:
            ok = self.cache[qhash]
            #nq = self.simplifyQuery(self.setLimit1(query))
            #nok = self.is_answering(nq)
            #assert ok == nok, 'pas égal\n %s \n %s' % (query,nq)
        else:
            ok = self.is_answering(self.setLimit1(query))
            self.cache[qhash] = ok
        return ok

#==================================================

class SPARQLEP (Endpoint): # "http://dbpedia.org/sparql" "http://172.16.9.15:8890/sparql"
    def __init__(self, service = 'http://172.16.9.15:8890/sparql', cacheDir = '.'):
        Endpoint.__init__(self, service, cacheDir)
        self.engine = SPARQLWrapper(self.service)
        self.engine.setReturnFormat(JSON)
        # self.sparql.setRequestMethod(POST)

    def query(self, qstr):
        self.engine.setQuery(qstr)
        return self.engine.query().convert()

    def is_answering(self, qstr):
        try:
            results = self.query(qstr)
            nb = len(results["results"]["bindings"])
        except QueryBadFormed as e:
            logging.info('Erreur QueryBadFormed : %s',e)
            print('QueryBadFormed',qstr)
            nb = 0
        except EndPointNotFound as e:
            logging.info('Erreur EndPointNotFound : %s',e)
            print('EndPointNotFound',qstr)
            nb = 0
        except EndPointInternalError as e:
            logging.info('Erreur EndPointInternalError : %s',e)
            print('EndPointInternalError',qstr)
            nb = 0
        except Exception as e:
            print('Erreur SPARQL EP ??? :',e,qstr)
            nb = 0
        return nb > 0

class DBPediaEP (SPARQLEP):
    def __init__(self, service = "http://dbpedia.org/sparql", cacheDir = '.'):
        SPARQLEP.__init__(self, service, cacheDir)

#==================================================

class TPFEP(Endpoint):
    def __init__(self,service = "http://172.16.9.3:5001/dbpedia_3_9", cacheDir = '.'):
        Endpoint.__init__(self,service, cacheDir)  

    def query(self, qstr):
        ret = subprocess.run(['ldf-client',self.service, qstr], 
                             stdout=subprocess.PIPE, encoding='utf-8', stderr=subprocess.PIPE, check=True)
        out = ret.stdout
        if out != '':
            return json.loads(out)
        else:
            #raise Exception('TPF Client error : %s' % ret.stderr)
            return []

    def is_answering(self, qstr):
        try:
            results = self.query(qstr)
            nb = len(results)
        except subprocess.CalledProcessError as e :
            logging.info('Erreur CalledProcessError : %s',e)
            print('CalledProcessError',qstr)
            nb = 0
        except json.JSONDecodeError as e:
            logging.info('Erreur JSONDecodeError : %s',e)
            print('JSONDecodeError :',e)
            nb = 0
        except Exception as e:
            print('Erreur TPF EP ??? :',e , qstr)
            nb = 0
        return nb > 0
