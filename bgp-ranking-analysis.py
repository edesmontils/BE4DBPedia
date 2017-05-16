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

from lib.beRanking import *

import multiprocessing as mp

import datetime as dt

import logging
import argparse

from tools.tools import *
from tools.Stat import *
from tools.ProcessSet import *

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
parser.add_argument("-t","--type", help="How to take into account the validation by a SPARQL or a TPF endpoint (%s by default)" % MODE_RA_NOTEMPTY,
                choices=[MODE_RA_NOTEMPTY,MODE_RA_VALID,MODE_RA_WF, MODE_RA_ALL],dest="mode",default=MODE_RA_NOTEMPTY)
args = parser.parse_args()
now = date2filename(now())
logname = 'be4dbp-ranking-'+now+'.log'
csvname = 'be4dbp-ranking-'+now+'.csv'
manageLogging(args.logLevel, logname)

file_set = args.files
mode = args.mode

stat = Stat(Counter, ['file','cut'+str(MODE_CUTE),'rank','entry-rank','occurrences','self','s-s', 'o-o','s-o','o-s', 'sp-sp', 'po-po', 'sp-po', 'po-sp'] )
ps = ProcessSet(args.nb_processes, rankAnalysis ,mode)
ps.setStat(stat)
ps.start()

for file in file_set:
    if existFile(file):
        logging.debug('Analyse de "%s"', file)
        ps.put(file)

logging.info('Arrêt des processus d' 'analyse')
ps.stop()

stat.stop(True)
stat.saveCSV(csvname)
logging.info('Fin')
