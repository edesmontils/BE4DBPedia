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

class QueryEvaluator:
  def __init__(self):
    self.comments_pattern = re.compile(r"(^|\n)\s*#.*?\n")
    self.pattern = re.compile(r"""
        ((?P<base>(\s*BASE\s*<.*?>)\s*)|(?P<prefixes>(\s*PREFIX\s+.+:\s*<.*?>)\s*))*
        (?P<queryType>(CONSTRUCT|SELECT|ASK|DESCRIBE|INSERT|DELETE|CREATE|CLEAR|DROP|LOAD|COPY|MOVE|ADD))
        """, re.VERBOSE | re.IGNORECASE)
    self.requestQueryTypes = {SELECT, CONSTRUCT, ASK, DESCRIBE}
    self.modificationQueryTypes = {INSERT, DELETE, CREATE, CLEAR, DROP, LOAD, COPY, MOVE, ADD}
    self.allowedQueryTypes = self.requestQueryTypes | self.modificationQueryTypes

  def cleanCommentLines(self, query):
    return re.sub(self.comments_pattern, "\n" , query)

  def queryType(self,query):
    try:
      query = self.cleanCommentLines(query)
      r_queryType =  self.pattern.search(query).group("queryType").upper()
    except AttributeError:
      warnings.warn("not detected query type for query '%s'" % query.replace("\n", " "), RuntimeWarning)
      r_queryType = None

    if r_queryType in self.allowedQueryTypes :
        return r_queryType
    else :
        #raise Exception("Illegal SPARQL Query; must be one of SELECT, ASK, DESCRIBE, or CONSTRUCT")
        warnings.warn("unknown query type '%s'" % r_queryType, RuntimeWarning)
        return SELECT