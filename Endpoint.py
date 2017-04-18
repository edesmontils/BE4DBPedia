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

class EndpointException(Exception):
	'''raise when the endpoint can't answer normally to the query (timeout...). Syntax errors are right answers'''

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
        raise EndpointException("There is no defined endpoint")
        return False

    def hash(self,qstr):
        return hashlib.sha512(qstr.encode('utf-8')).hexdigest()

    def setLimit1(self,query):
        if self.reLimit.search(query):
            nquery = self.reLimit.sub('limit 1',query)
        else:
            nquery = query + ' limit 1 '
        return nquery

    def notEmpty(self,query):
        #On cherche d'abord dans le cache
        qhash = self.hash(query) 
        if qhash in self.cache:
            ok = self.cache[qhash]
        else:
            try:
                ok = self.is_answering(self.setLimit1(query))
                self.cache[qhash] = ok
            except EndpointException as e:
                logging.info('Erreur EndpointException : %s',e)
                raise Exception('Endpoint error',e)
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
            return nb > 0
        except QueryBadFormed as e:
            #logging.info('Erreur QueryBadFormed : %s',e)
            print('QueryBadFormed',qstr)
            return False
        except EndPointNotFound as e:
            logging.info('Erreur EndPointNotFound : %s',e)
            #print('EndPointNotFound',qstr)
            raise EndpointException("SPARQL endpoint error (EndPointNotFound)",e,qstr)
        except EndPointInternalError as e:
            logging.info('Erreur EndPointInternalError : %s',e)
            #print('EndPointInternalError',qstr)
            raise EndpointException("SPARQL endpoint error (EndPointInternalError)",e,qstr)
        except Exception as e:
            logging.info('Erreur SPARQL EP ??? : %s',e)
            print('Erreur SPARQL EP ??? :',e,qstr)
            raise EndpointException("SPARQL endpoint error (???)",e,qstr)

#==================================================

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
            raise Exception('TPF Client error : %s' % ret.stderr)
            #return []

    def is_answering(self, qstr):
        try:
            results = self.query(qstr)
            nb = len(results)
            return nb > 0
        except subprocess.CalledProcessError as e :
            logging.info('Erreur CalledProcessError : %s',e)
            #print('CalledProcessError',qstr)
            raise EndpointException("TPF endpoint error (CalledProcessError)",e,qstr)
        except json.JSONDecodeError as e:
            logging.info('Erreur JSONDecodeError : %s',e)
            #print('JSONDecodeError :',e)
            raise EndpointException("TPF endpoint error (JSONDecodeError)",e,qstr)
        except Exception as e:
            logging.info('Erreur TPF EP ??? : %s',e)
            print('Erreur TPF EP ??? :',e , qstr)
            raise EndpointException("TPF endpoint error (???)",e,qstr)
