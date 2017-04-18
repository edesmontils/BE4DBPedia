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

  def cleanCommentLines(self, query):
    return re.sub(self.comments_pattern, "\n" , query)

  def queryType(self,query):
    try:
      query = self.cleanCommentLines(query)
      r_queryType =  self.typePattern.search(query).group("type").upper()
    except AttributeError:
      logging.warning("not detected query type for query '%s'" % query.replace("\n", " "), RuntimeWarning)
      r_queryType = None

    if r_queryType in self.allowedQueryTypes :
        return r_queryType
    else :
        logging.warning("unknown query type '%s'" % r_queryType, RuntimeWarning)
        return SELECT

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
    nquery = re.sub(self.reLimit, "" , nquery)
    nquery = re.sub(self.reOffset, "" , nquery)
    return nquery

# reSelect = re.compile(r'(\W+)select(\s+)', re.IGNORECASE)

# r"""
# \Wregex\W*
# \(
#     ( [^,()"'<]+ | "[^"]*" | '[^']*' | <[^>]*> | \w* \( [^)]* \) )+
# ( , ( [^,()"'<]+ | "[^"]*" | '[^']*' | <[^>]*> | \w* \( [^)]* \) )+ ){2,}
# \)
# """