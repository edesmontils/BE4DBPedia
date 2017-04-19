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
  def __init__(self):
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
    self.sem = self.mp_manager.Semaphore()
    self.stat = self.mp_manager.dict()
    for t in self.allowedQueryTypes:
      self.stat[t] = 0
    self.stat['None'] = 0

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
        with self.sem :
            self.stat[r_queryType] += 1
        return r_queryType
    else :
        logging.warning("unknown query type (%s) for query '%s'" % (r_queryType,query.replace("\n", " ")))
        with self.sem :
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


