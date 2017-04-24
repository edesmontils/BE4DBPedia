#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
"""
Application to rank BGP according to frequency
"""
#    Copyright (C) 2017 by
#    Emmanuel Desmontils <emmanuel.desmontils@univ-nantes.fr>
#    Patricia Serrano-Alvarado <patricia.serrano-alvarado@univ-nantes.fr>
#    All rights reserved.
#    GPL v 2.0 license.

from beRanking import *

import multiprocessing as mp

import datetime as dt

import logging
import argparse
from tools import *

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

parser = argparse.ArgumentParser(description='Etude du ranking')
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
parser.add_argument("-t","--type", help="Request a SPARQL or a TPF endpoint to verify the query and test it returns at least one triple (%s by default)" % MODE_RA_NOTEMPTY,
                choices=[MODE_RA_NOTEMPTY,MODE_RA_VALID,MODE_RA_WF, MODE_RA_ALL],dest="mode",default=MODE_RA_NOTEMPTY)
args = parser.parse_args()
manageLogging(args.logLevel, 'be4dbp-ranking-'+date2filename(now())+'.log')

file_set = args.files
mode = args.mode
nb_processes = args.nb_processes
logging.info('Lancement des %d processus d\'analyse', nb_processes)
compute_queue = mp.Queue(nb_processes)
process_list = [
    mp.Process(target=analyse, args=(compute_queue, mode))
    for _ in range(nb_processes)
]
for process in process_list:
    process.start()

for file in file_set:
    if existFile(file):
        logging.debug('Analyse de "%s"', file)
        compute_queue.put(file)

logging.info('Arrêt des processus d' 'analyse')
for process in process_list:
    compute_queue.put(None)
for process in process_list:
    process.join()

logging.info('Fin')