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

from beLib import *
from beLibProcesses import *

import os.path
import argparse

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
args = parser.parse_args()
startDate = dt.datetime.now().__str__().replace(' ', 'T').replace(':', '-')[0:19]
manageLogging(args.logLevel, 'be4dbp-ranking-'+startDate+'.log')

file_set = args.files

nb_processes = args.nb_processes
logging.info('Lancement des %d processus d\'analyse', nb_processes)
compute_queue = mp.Queue(nb_processes)
process_list = [
    mp.Process(target=analyse, args=(compute_queue, ))
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