#!/usr/bin/env python3.6
# coding: utf8
"""
Multi-processing application to extract BGP from a DBPedia log.
"""
#    Copyright (C) 2017 by
#    Emmanuel Desmontils <emmanuel.desmontils@univ-nantes.fr>
#    Patricia Serrano-Alvarado <patricia.serrano-alvarado@univ-nantes.fr>
#    All rights reserved.
#    GPL v 2.0 license.

import time
import datetime
#from pprint import pprint
import os
import os.path

from multiprocessing import Process, Lock, Queue, BoundedSemaphore, Semaphore
import multiprocessing as mp
from queue import Empty

import logging

from bgp import *
from beLib import *
from beLibProcesses import *

#==================================================


def compute(sem, in_queue, stat, default_prefixes):
    logging.debug('Start compute worker "%s"', os.getpid())
    while True:
        try:
            mess = in_queue.get()
            if mess is None:
                break
            else:
                (query, param_list, host, file, date, line) = mess
                logging.debug('Treat mess in %s %s', os.getpid(), host)
                (ok, nquery, bgp) = validate(
                    ParallelCounter(stat, date), line, host, query,
                    default_prefixes)
                logging.debug('Analyse "%s" pour %s', ok, host)
                if ok:
                    s = buildXMLBGP(nquery, param_list, bgp, host, date, line)
                    if s is not None:
                        with sem:
                            saveEntry(file, s, host)
        except Empty as e:
            print('empty!')
        except Exception as e:
            print(e)
            break
    logging.debug('Stop compute worker "%s"', os.getpid())


#==================================================
#==================================================
#==================================================

# Traitement de la ligne de commande
# time python3.6 scan-2.py -p 4 -t '2015-11-03T02:00:00+01:00'

parser = setStdArgs('Parallel BGP Extractor for DBPedia log.')
parser.add_argument("-p", "--proc", type=int, default=mp.cpu_count(), dest="nb_processes",
                    help="Number of processes used (%d by default)" % mp.cpu_count())
args = parser.parse_args()
(refDate, baseDir, f_in) = manageStdArgs(args)

logging.info('Lecture des préfixes par défaut')
default_prefixes = loadPrefixes()

logging.info('Initialisations')
pattern = makeLogPattern()
old_date = ''
nb_lines = 0
file_set = set()

nb_processes = args.nb_processes
logging.info('Lancement des %d processus de traitement', nb_processes)
sem = Lock()
stat = Stat()
compute_queue = mp.Queue()
process_list = [
    mp.Process(
        target=compute, args=(sem, compute_queue, stat, default_prefixes))
    for _ in range(nb_processes)
]
for process in process_list:
    process.start()

cpt = ParallelCounter(stat)

logging.info('Lancement du traitement')
for line in f_in:
    nb_lines += 1
    m = pattern.match(line)
    (query, date, param_list, ip) = extract(m.groupdict())

    if (date != old_date):
        dateOk = date.startswith(refDate)
        if dateOk:
            logging.info('%d - Traitement de %s', nb_lines, date)
        else:
            logging.info('%d - passage de %s', nb_lines, date)

        old_date = date
        rep = newDir(baseDir, date)
        cpt = ParallelCounter(stat, date)

    if nb_lines % 1000 == 0:
        logging.info('%d ligne(s) vues', nb_lines)

    cpt.line()
    if dateOk:  # and (nb_lines < 100):
        if (query != ''):
            file = rep + ip + '-be4dbp.xml'
            compute_queue.put((query, param_list, ip, file, date, nb_lines))
            file_set.add(file)
        else:
            logging.debug('(%d) No query for %s', nb_lines, ip)
            cpt.autre()

logging.info('Fermeture de "%s"' % args.file)
f_in.close()

logging.info('Arrêt des processus de traitement')
for process in process_list:
    compute_queue.put(None)
for process in process_list:
    process.join()

stat.stop()

logging.info('Terminaison des fichiers')
for file in file_set:
    if os.path.isfile(file):
        closeLog(file)

# logging.info('Lancement des %d processus d\'analyse', nb_processes)
# process_list = [
#     mp.Process(target=analyse, args=(compute_queue, ))
#     for _ in range(nb_processes)
# ]
# for process in process_list:
#     process.start()

# for file in file_set:
#     if os.path.isfile(file):
#         logging.debug('Analyse de "%s"', file)
#         compute_queue.put(file)

# logging.info('Arrêt des processus d' 'analyse')
# for process in process_list:
#     compute_queue.put(None)
# for process in process_list:
#     process.join()

logging.info('Fin')
