#!/usr/bin/env python3.6
# coding: utf8
"""
Tools to manage the log file
"""
#    Copyright (C) 2017 by
#    Emmanuel Desmontils <emmanuel.desmontils@univ-nantes.fr>
#    Patricia Serrano-Alvarado <patricia.serrano-alvarado@univ-nantes.fr>
#    All rights reserved.
#    GPL v 2.0 license.

import sys
#import os
#import shutil

import re
import time

from urllib.parse import urlparse, parse_qsl

from tools import *
import argparse
import logging

#==================================================
class Log:

    def __init__(self, file_name):
        self.nb_lines = 0
        self.file_name = file_name
        self.pattern = self.makeLogPattern()
        if existFile(self.file_name):
            logging.info('Open "%s"' % self.file_name)
            self.f = open(self.file_name, 'r')
        else :
            logging.info('"%s" does\'nt exist' % self.file_name)
            print('Can\'t open file %s' % self.file_name )
            sys.exit()

    def __iter__(self):
        return self
 
    def __next__(self):
        ligne = self.f.readline()
        if len(ligne)==0:
            self.f.close()
            raise StopIteration
            #return None
        else:
            self.nb_lines += 1
            return self.extract(self.pattern.match(ligne).groupdict())

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
        pattern = re.compile(r'\s+'.join(parts) + r'\s*\Z')
        return pattern

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

    parser = argparse.ArgumentParser(description='Etude du ranking')
    parser.add_argument('files', metavar='file', nargs='+',help='files to analyse')
    args = parser.parse_args()

    file_set = args.files

    for file in file_set:
        log = Log(file)
        for (query, date, param_list, ip) in log:
            print(ip,query)

