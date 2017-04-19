#!/usr/bin/env python3.6
# coding: utf8
"""
Basic query tools
"""
#    Copyright (C) 2017 by
#    Emmanuel Desmontils <emmanuel.desmontils@univ-nantes.fr>
#    Patricia Serrano-Alvarado <patricia.serrano-alvarado@univ-nantes.fr>
#    All rights reserved.
#    GPL v 2.0 license.

import re
import logging

import multiprocessing as mp

from rdflib.plugins.sparql.sparql import SPARQLError
from rdflib.plugins.sparql.algebra import translateQuery
from rdflib.plugins.sparql.parser import parseQuery

from bgp import *

#==================================================

# Possible SPARQL/SPARUL query type
SELECT     = "SELECT"
CONSTRUCT  = "CONSTRUCT"
ASK        = "ASK"
DESCRIBE   = "DESCRIBE"
INSERT     = "INSERT"
DELETE     = "DELETE"
CREATE     = "CREATE"
CLEAR      = "CLEAR"
DROP       = "DROP"
LOAD       = "LOAD"
COPY       = "COPY"
MOVE       = "MOVE"
ADD        = "ADD"
INSERTDATA = "INSERTDATA"
DELETEDATA = "DELETEDATA"
DELETEWHERE = "DELETEWHERE"

#==================================================

class QueryManager:
  def __init__(self, defaultPrefixes = None):
    self.comments_pattern = re.compile(r"(^|\n)\s*#.*?\n")

    self.typePattern = re.compile(r"""
        ((?P<baseDef>(\s*BASE\s*<.*?>)\s*)|(?P<prefixesDef>(\s*PREFIX\s+.+:\s*<.*?>)\s*))*
        (?P<type>(CONSTRUCT|SELECT|ASK|DESCRIBE|INSERT|DELETE|CREATE|CLEAR|DROP|LOAD|COPY|MOVE|ADD|INSERTDATA|DELETEDATA|DELETEWHERE))
        """, re.VERBOSE | re.IGNORECASE)

    self.reThumbnail = re.compile(r"\Wxsd\:date\s*\(", re.IGNORECASE | re.VERBOSE)
    self.reIsIRI = re.compile(r"\WisIRI\s*\(", re.IGNORECASE | re.VERBOSE)
    self.reRegex = re.compile(r"\Wregex\s*\(", re.IGNORECASE | re.VERBOSE)

    self.reUnion = re.compile(r'(\W+)union(\W+)', re.IGNORECASE)

    self.reLimit = re.compile(r'limit\s*\d+',re.IGNORECASE)
    self.reOffset = re.compile(r'offset\s*\d+',re.IGNORECASE)

    self.requestQueryTypes = {SELECT, CONSTRUCT, ASK, DESCRIBE}
    self.modificationQueryTypes = {INSERT, DELETE, CREATE, CLEAR, DROP, LOAD, COPY, MOVE, ADD, INSERTDATA, DELETEDATA, DELETEWHERE}
    self.allowedQueryTypes = self.requestQueryTypes | self.modificationQueryTypes

    self.mp_manager = mp.Manager()
    self.sem = self.mp_manager.dict() #self.mp_manager.Semaphore()
    self.stat = self.mp_manager.dict()
    for t in self.allowedQueryTypes:
      self.stat[t] = 0
      self.sem[t] = self.mp_manager.Semaphore()
    self.stat['None'] = 0
    self.sem['None'] = self.mp_manager.Semaphore()

    if defaultPrefixes == None:
      self.defaultPrefixes = dict()
      self.defaultPrefixes['ed'] = 'http://exemple.org/edamiral#'
      self.defaultPrefixes['rdf'] = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
      self.defaultPrefixes['rdfs'] = 'http://www.w3.org/2000/01/rdf-schema#'
      self.defaultPrefixes['owl'] = 'http://www.w3.org/2002/07/owl#'
      self.defaultPrefixes['xsd'] = 'http://www.w3.org/2001/XMLSchema#'
      self.defaultPrefixes['foaf'] = 'http://xmlns.com/foaf/0.1/'
      self.defaultPrefixes['dc'] = 'http://purl.org/dc/elements/1.1/'
      self.defaultPrefixes['skos'] = 'http://www.w3.org/2004/02/skos/core#'
      self.defaultPrefixes['dbpedia'] = 'http://dbpedia.org/'
      self.defaultPrefixes['dbpedia2'] = 'http://dbpedia.org/property/'
      self.defaultPrefixes['dbpedia3'] = 'http://dbpedia.org/resource/'
    else:
      self.defaultPrefixes = defaultPrefixes

  def printStats(self):
    print('Query Type Stats')
    for t in iter(self.stat.keys()):
      print('\t',t.ljust(12),'=',self.stat[t])

  def cleanCommentLines(self, query):
    return re.sub(self.comments_pattern, "\n" , query)

  def queryType(self,query):
    try:
      query = self.cleanCommentLines(query)
      r_queryType =  self.typePattern.search(query).group("type").upper()
    except AttributeError:
      r_queryType = None

    if r_queryType in self.allowedQueryTypes :
        with self.sem[r_queryType] :
            self.stat[r_queryType] += 1
        return r_queryType
    else :
        logging.warning("unknown query type (%s) for query '%s'" % (r_queryType,query.replace("\n", " ")))
        with self.sem['None'] :
          self.stat['None'] += 1
        return None # SELECT

  def isTPFCompatible(self, query):
    ok = True
    if self.reIsIRI.search(query) != None:
      return False
    elif self.reRegex.search(query) != None:
      return False
    elif self.reThumbnail.search(query) != None:
      return False
    else:
      return ok

  def containsUnion(self, query):
    return self.reUnion.search(query) is not None

  def simplifyQuery(self, query):
    nquery = self.cleanCommentLines(query)
    #nquery = re.sub(self.reLimit, "" , nquery)
    #nquery = re.sub(self.reOffset, "" , nquery)
    return nquery

  def extractBGP(self,query):
    try:
      tree = parseQuery(query)
      try:
        q = translateQuery(tree).algebra
        #---
        assert q is not None
        #---
        try:
          BGPSet = getBGP(q)
          if valid(BGPSet):
            return (BGPSet, query)
          else:
            raise BGPUnvalidException('BGP Not Valid')
        except ValueError as e:
          raise BGPException(e.args)    
      except SPARQLError as e:
        raise SPARQLException(e.args)
      except Exception as e:
        m = e.__str__().split(':')
        if (m[0] == 'Unknown namespace prefix '):
          pr = m[1].strip()
          if (pr in self.defaultPrefixes):
            n_query = 'PREFIX ' + pr + ': <' + self.defaultPrefixes[pr] + '> #ADD by BE4DBPedia \n' + query
            return self.extractBGP(n_query)
          else:
            raise NSException(e.args)
        else:
          raise TranslateQueryException(e.args)
    except Exception as e:
      raise ParseQueryException(e.args)

class QueryManagerException(Exception):
  def __init__(self, args):
    Exception.__init__(self,args)

class BGPException(QueryManagerException):
  def __init__(self, args):
    QueryManagerException.__init__(self,args)

class BGPUnvalidException(BGPException):
  def __init__(self, args):
    BGPException.__init__(self,args)

class TranslateQueryException(QueryManagerException):
  def __init__(self, args):
    QueryManagerException.__init__(self,args)

class SPARQLException(TranslateQueryException):
  def __init__(self, args):
    TranslateQueryException.__init__(self,args)

class NSException(TranslateQueryException):
  def __init__(self, args):
    TranslateQueryException.__init__(self,args)

class ParseQueryException(QueryManagerException):
  def __init__(self, args):
    QueryManagerException.__init__(self,args)

# reSelect = re.compile(r'(\W+)select(\s+)', re.IGNORECASE)

# r"""
# \Wregex\W*
# \(
#     ( [^,()"'<]+ | "[^"]*" | '[^']*' | <[^>]*> | \w* \( [^)]* \) )+
# ( , ( [^,()"'<]+ | "[^"]*" | '[^']*' | <[^>]*> | \w* \( [^)]* \) )+ ){2,}
# \)
# """

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

  #print(reLimit.sub('limit 1',ref))

  q5 = """
  prefix : <http://www.example.org/lift2#> select ?s ?o where {?s :p3 "titi" . ?s :p1 ?o . ?s :p4 "tata"}
  """

  q6 = """
  prefix : <http://www.example.org/lift2#>  #njvbjonbtrg
  #Q2
  select ?s ?o whre {
    ?s :p2 "toto" . #kjgfjgj
    ?s edm:type <http://exemple.org/MonConcept> .
    # ?s ?p ?o .
    #?s <http://machin.org/toto#bidule> ?o ## jhjhj
  } limit 10 offset 1000
  """

  # reSupCom=re.compile(r'#\w*[^>].*$',re.IGNORECASE | re.MULTILINE)

  # def simplyQuery(query) :
  #   if reSupCom.search(query):
  #     nquery = reSupCom.sub('',query)
  #   else:
  #     nquery = query
  #   return ' '.join(nquery.split())

  qe = QueryManager()
  #q = qe.simplifyQuery(q6)
  print('origin:',q6)
  #print('simplified',q)
  print('Select?',qe.queryType(q6) == SELECT)

  try:
    (bgp, query) = qe.extractBGP(q6)
    print(query)
    print(serializeBGP2str(bgp))
  except Exception as e:
    print(type(e))
    print(e)