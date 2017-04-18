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

EP_QueryBadFormed = False
EP_QueryWellFormed = True

class Endpoint:
    def __init__(self, service, cacheType = '', cacheDir = '.') :
        self.engine = None
        self.service = service
        self.reLimit = re.compile(r'limit\s*\d+',re.IGNORECASE)
        self.setCacheDir(cacheDir)
        self.cache = dict()
        self.cacheType = cacheType
        self.do_cache = False
        self.reSupCom=re.compile(r'#[^>].*$',re.IGNORECASE | re.MULTILINE)

    def loadCache(self):
        if os.path.isfile(self.cacheDir+"/be4dbp-"+self.cacheType+".csv") :
            logging.info('Reading cache file')
            with open(self.cacheDir+"/be4dbp-"+self.cacheType+".csv","r", encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    self.cache[row['qhash']] = (row['ok'] == "True", row['wf'] == "True")

    def saveCache(self):
        if self.do_cache:
            logging.info('Writing cache file')
            with open(self.cacheDir+"/be4dbp-"+self.cacheType+".csv","w", encoding='utf-8') as f:
                fn=['ok','wf', 'qhash']
                writer = csv.DictWriter(f,fieldnames=fn)
                writer.writeheader()
                for x in self.cache:
                    (ok,wellFormed) = self.cache[x]
                    writer.writerow({'ok':ok, 'wf':wellFormed,'qhash':x}) 

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
        '''Test if the query replies at least one answer (first value) and if the query is well formed (second value)'''
        raise EndpointException("There is no defined endpoint")
        return (False, EP_QueryWellFormed)

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
            return self.cache[qhash]
        else:
            try:
                (ok,wf) = self.is_answering(self.setLimit1(query))
                self.cache[qhash] = (ok,wf)
                #---
                assert not(ok==True and wf==False), 'Bad response of is_answering'
                #---
                return (ok,wf)
            except EndpointException as e:
                logging.info('Erreur EndpointException : %s',e)
                raise Exception('Endpoint error',e)

#==================================================

class SPARQLEP (Endpoint): # "http://dbpedia.org/sparql" "http://172.16.9.15:8890/sparql"
    def __init__(self, service = 'http://172.16.9.15:8890/sparql', cacheDir = '.'):
        Endpoint.__init__(self, service, cacheType='SPARQL', cacheDir=cacheDir)
        self.engine = SPARQLWrapper(self.service)
        self.engine.setReturnFormat(JSON)
        # self.sparql.setRequestMethod(POST)

    def query(self, qstr):
        self.engine.setQuery(qstr)
        return self.engine.query().convert()

    def is_answering(self, qstr):
        try:
            print('Search in %s for %s'%(self.service,query.replace("\n", " ")))
            results = self.query(qstr)
            nb = len(results["results"]["bindings"])
            return (nb > 0, EP_QueryWellFormed)
        except QueryBadFormed as e:
            #logging.info('Erreur QueryBadFormed : %s',e)
            print('QueryBadFormed',qstr)
            return (False, EP_QueryBadFormed)
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
        SPARQLEP.__init__(self, service, cacheDir=cacheDir)

#==================================================

class TPFEP(Endpoint):
    def __init__(self,service = "http://172.16.9.3:5001/dbpedia_3_9", cacheDir = '.'):
        Endpoint.__init__(self,service, cacheType='TPF', cacheDir=cacheDir)  

    def query(self, qstr):
        # ret = subprocess.run(['ldf-client',self.service, qstr], 
        #                      stdout=subprocess.PIPE, encoding='utf-8', stderr=subprocess.PIPE, check=True)
        # out = ret.stdout
        # if out != '':
        #     return json.loads(out)
        # else:
        #     if ret.stderr.startswith('ERROR: Query execution could not start.\n\nSyntax error in query'):
        #         raise Exception('QueryBadFormed : %s' % ret.stderr)
        #     else:
        #         raise Exception('TPF Client error : %s' % ret.stderr)
        # 'run' n'existe que depuis python 3.5 !!! donc pas en 3.2 !!!!
        out = subprocess.check_output(['ldf-client',self.service, qstr]) #, encoding='utf-8') # ,stderr=subprocess.DEVNULL : python3.3
        #print('out=',out)
        if out != '':
            return json.loads(out)
        else: raise Exception('QueryBadFormed') #return []

    def is_answering(self, qstr):
        try:
            results = self.query(qstr)
            nb = len(results)
            return (nb > 0,EP_QueryWellFormed)
        except subprocess.CalledProcessError as e :
            logging.info('Erreur CalledProcessError : %s',e)
            print('CalledProcessError',e,qstr)
            raise EndpointException("TPF endpoint error (CalledProcessError)",e,qstr)
        # except json.JSONDecodeError as e: #Fonctionne pas en python 3.2... que depuis 3.5 !!!!
        #     logging.info('Erreur JSONDecodeError : %s',e)
        #     #print('JSONDecodeError :',e)
        #     raise EndpointException("TPF endpoint error (JSONDecodeError)",e,qstr)
        except Exception as e:
            message = e.__str__()
            if message.startswith('QueryBadFormed'):
                #print('QueryBadFormed',qstr)
                return (False,EP_QueryBadFormed)
            else:
                logging.info('Erreur TPF EP ??? : %s',e)
                print('Erreur TPF EP ??? :',e , qstr)
                raise EndpointException("TPF endpoint error (???)",e,qstr)

#==================================================
#==================================================
#==================================================

if __name__ == '__main__':
	logging.basicConfig(
	    format='%(levelname)s:%(asctime)s:%(message)s',
	    filename='scan.log',
	    filemode='w',
	    level=logging.DEBUG)

	print('main')

	ref = """
	    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
	    SELECT ?label
	    WHERE { <http://dbpedia.org/resource/Asturias> rdfs:label ?label }
	    LIMIT 10
	"""

	pb = """
	select DISTINCT ?zzzzzz where{  ?x ?y ?zzzzzz FILTER regex(?zzzzzz, <http://dbpedia.org/class/yago/PresidentsOfTheUnitedState>)} LIMIT 5 
	"""
	sp = DBPediaEP()
	sp.caching(True)
	try:
	  print(sp.notEmpty(ref))
	  sp.caching(False)
	except Exception as e:
	  print(e)

	q5 = """
	prefix : <http://www.example.org/lift2#> select ?s ?o where {?s :p3 "titi" . ?s :p1 ?o . ?s :p4 "tata"}
	"""

	q6 = """
	prefix : <http://www.example.org/lift2#>  #njvbjonbtrg

	#Q2
	select ?s ?o whre {
	  ?s :p2 "toto" . #kjgfjgj
	  # ?s ?p ?o .
	  #?s <http://machin.org/toto#bidule> ?o ## jhjhj
	} limit 10 offset 1000
	"""
	print('origin:',q6)

	 # http://localhost:5000/lift : serveur TPF LIFT (exemple du papier)
	sp = TPFEP(service = 'http://localhost:5000/lift')
	#sp.caching(True)
	try:
	  print('NotEmpty:',sp.notEmpty(q6))
	  #sp.saveCache()
	except Exception as e:
	  #print(e)
		pass