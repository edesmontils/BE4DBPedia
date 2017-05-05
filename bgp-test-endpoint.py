#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
"""
Application to test request on SPARQL or TPF server
"""
#    Copyright (C) 2017 by
#    Emmanuel Desmontils <emmanuel.desmontils@univ-nantes.fr>
#    Patricia Serrano-Alvarado <patricia.serrano-alvarado@univ-nantes.fr>
#    All rights reserved.
#    GPL v 2.0 license.



import multiprocessing as mp
from queue import Empty

import datetime as dt

import logging
import argparse

from tools.tools import *
from tools.ProcessSet import *
from tools.Endpoint import *
from tools.Stat import *
from lib.beTestEPValid import *

from lxml import etree  # http://lxml.de/index.html#documentation

#==================================================

#==================================================

def manageLogging(logLevel, logfile = 'be4dbp.log'):
    if logLevel:
        # https://docs.python.org/3/library/logging.html?highlight=log#module-logging
        # https://docs.python.org/3/howto/logging.html#logging-basic-tutorial
        logging.basicConfig(
            format='%(levelname)s:%(asctime)s:%(message)s',
            filename=logfile,filemode='w',
            level=getattr(logging,logLevel))

#==================================================

parser = argparse.ArgumentParser(description='Etude des requêtes')
parser.add_argument('files', metavar='file', nargs='+',
                    help='files to analyse')
parser.add_argument("-l", "--log", dest="logLevel",
                        choices=[
                            'DEBUG',
                            'INFO',
                            'WARNING',
                            'ERROR',
                            'CRITICAL'],
                        help="Set the logging level", default='INFO')
parser.add_argument("-p", "--proc", type=int, default=mp.cpu_count(), dest="nb_processes",
                    help="Number of processes used (%d by default)" % mp.cpu_count())
parser.add_argument("-e","--empty", help="Request a SPARQL or a TPF endpoint to verify the query and test it returns at least one triple (%s by default)" % MODE_TE_TPF,
                choices=[MODE_TE_SPARQL,MODE_TE_TPF],dest="doEmpty",default=MODE_TE_TPF)
parser.add_argument("-ep","--endpoint", help="The endpoint requested for the '-e' ('--empty') option (for exemple '%s' for %s by default)" % (DEFAULT_TPF_EP,MODE_TE_TPF),
                dest="ep", default=DEFAULT_TPF_EP)
parser.add_argument("-to", "--timeout", type=int, default=60, dest="timeout",
                    help="Endpoint Time Out (%d by default)" % 60)
args = parser.parse_args()
emptyTest = args.doEmpty
manageLogging(args.logLevel, 'be4dbp-tests-'+emptyTest+'-'+date2filename(now())+'.log')

file_set = args.files

stat = Stat(Counter, ['valid', 'empty', 'bfq', 'to', 'other'] )

current_dir = os.getcwd()
resourcesDir = 'resources'
if emptyTest == MODE_TE_SPARQL:
    if args.ep == '':
        endpoint = SPARQLEP(cacheDir = current_dir+'/'+resourcesDir)
    else:
        endpoint = SPARQLEP(args.ep, cacheDir = current_dir+'/'+resourcesDir)
else:
    if args.ep == '':
        endpoint = TPFEP(cacheDir = current_dir+'/'+resourcesDir)
    else:
        endpoint = TPFEP(service = args.ep, cacheDir = current_dir+'/'+resourcesDir)

logging.info('Empty responses tests with %s' % endpoint)
endpoint.caching(True)
logging.info('Setting time out %s second(s)' % args.timeout)
endpoint.setTimeOut(args.timeout)

nb_processes = args.nb_processes
logging.info('Lancement des %d processus d\'analyse', nb_processes)
ps = ProcessSet(nb_processes, stat, TestAnalysis, endpoint, emptyTest)

# compute_queue = mp.Queue(nb_processes)
# process_list = [
#     mp.Process(target=analyse, args=(compute_queue, endpoint, emptyTest, stat))
#     for _ in range(nb_processes)
# ]
# for process in process_list:
#     process.start()

nbf = len(file_set)
tenpercent = max(int(nbf*0.1),2*nb_processes)
no = 0

for file in file_set:
    no +=1
    if no % tenpercent == 0: 
        print('Saving cache')
        endpoint.saveCache()
    if existFile(file):
        logging.debug('Analyse de "%s"', file)
        ps.put(file)

logging.info('Arrêt des processus d' 'analyse')
ps.stop()
# for process in process_list:
#     compute_queue.put(None)
# for process in process_list:
#     process.join()

logging.info('Fin')
endpoint.saveCache()
stat.stop(True)
logging.info('End')
