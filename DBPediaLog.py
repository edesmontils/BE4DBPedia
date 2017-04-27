#!/usr/bin/env python3.6
# coding: utf8
"""
Tools to manage the DBPedia log file
"""
#    Copyright (C) 2017 by
#    Emmanuel Desmontils <emmanuel.desmontils@univ-nantes.fr>
#    Patricia Serrano-Alvarado <patricia.serrano-alvarado@univ-nantes.fr>
#    All rights reserved.
#    GPL v 2.0 license.

import sys

import re
import time

from urllib.parse import urlparse, parse_qsl

from tools import *
import argparse
from Log import *

#==================================================

class DBPediaLog(Log):
    def __init__(self, file_name):
        Log.__init__(self, file_name)

    def makeLogPattern(self):
        parts = [
            r'(?P<host>\S+)',  # host %h
            r'\S+',  # indent %l (unused)
            r'(?P<user>\S+)',  # user %u
            r'\[(?P<time>.+)\]',  # time %t
            r'"(?P<request>.+)"',  # request "%r"
            r'(?P<status>[0-9]+)',  # status %>s
            r'(?P<size>\S+)',  # size %b (careful, can be '-')
            r'"(?P<referer>.*)"',  # referer "%{Referer}i"
            r'"(?P<code>.*)"',
            r'"(?P<agent>.*)"',  # user agent "%{User-agent}i"
        ]
        return re.compile(r'\s+'.join(parts) + r'\s*\Z')

    def extract(self,res):
        tt = time.strptime(res["time"][:-6], "%d/%b/%Y %H:%M:%S")
        tt = list(tt[:6]) + [0, Timezone(res["time"][-5:])]
        date = date2str(dt.datetime(*tt)) #.__str__().replace(' ', 'T')

        url = res['request'].split(' ')[1]
        param = url.split('?')[1]

        param_list = []
        query = ''
        for (p, q) in parse_qsl(param):
            if p == 'query':
                query = ' ' + q + ' '
            elif p == 'qtxt':
                query = ' ' + q + ' '
            else:
                param_list.append((p, q))
        return (query, date, param_list, res['host'])

#==================================================
#==================================================
#==================================================

if __name__ == '__main__':
    print('main de Log.py')

    parser = argparse.ArgumentParser(description='DBPedia log reading')
    parser.add_argument('file', metavar='file', help='log to analyse')
    args = parser.parse_args()

    file = args.file
    log = DBPediaLog(file)
    for (query, date, param_list, ip) in log:
        print(ip,query)

