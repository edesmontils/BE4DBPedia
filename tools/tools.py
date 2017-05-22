#!/usr/bin/env python3.6
# coding: utf8
"""
Generic set of tools
"""
#    Copyright (C) 2017 by
#    Emmanuel Desmontils <emmanuel.desmontils@univ-nantes.fr>
#    Patricia Serrano-Alvarado <patricia.serrano-alvarado@univ-nantes.fr>
#    All rights reserved.
#    GPL v 2.0 license.

import datetime as dt
import os.path
# import iso8601 # https://pypi.python.org/pypi/iso8601/     http://pyiso8601.readthedocs.io/en/latest/
#==================================================
class Timezone(dt.tzinfo):
    def __init__(self, name="+0000"):
        self.name = name
        seconds = int(name[:-2]) * 3600 + int(name[-2:]) * 60
        self.offset = dt.timedelta(seconds=seconds)

    def utcoffset(self, dt):
        return self.offset

    def dst(self, dt):
        return timedelta(0)

    def tzname(self, dt):
        return self.name

# def t(u): 
#     try:
#         return iso8601.parse_date(u.attrib['t'])
#     except iso8601.ParseError:
#         return iso8601.parse_date(date.today().isoformat()+'T'+u.attrib['t'])
#     #return time.strptime(u.attrib['t'], "%Y-%m-%dT%H:%M:%S")

#==================================================
def existFile(f):
    return os.path.isfile(f)

#==================================================
def now():
	return dt.datetime.now()

#==================================================
def date2str(date):
	return date.__str__().replace(' ', 'T')[0:19]

#==================================================
def date2filename(date):
	return date.__str__().replace(' ', 'T').replace(':', '-').replace('+', '-')

#==================================================
def pprint_dict(d):
    length = max([len(str(i)) for i in d.keys()])
    for t in iter(d.keys()):
        print('\t',str(t).ljust(length),'=',d[t])

#==================================================
INVALID_URI_CHARS = '<>" {}|\\^`'
# ispired from the _is_valid_uri(uri) function from rdflib.term lib : 
# http://rdflib.readthedocs.io/en/stable/_modules/rdflib/term.html#URIRef
def isValidURI(uri):
    for c in INVALID_URI_CHARS:
        if c in uri: return False
    return True

#==================================================
#==================================================
#==================================================

if __name__ == '__main__':
    print('main',now())
