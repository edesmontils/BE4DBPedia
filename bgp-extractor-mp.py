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


def compute(idp, tab_date, sem, in_queue, stat, default_prefixes, doTPFC):
    logging.debug('(%d) Start compute worker "%s"' %(idp, os.getpid()) )
    while True:
        try:
            mess = in_queue.get(timeout=5)
            if mess is None:
                break
            else:
                (query, param_list, host, file, date, line) = mess
                logging.debug('Treat mess in %s %s', os.getpid(), host)
                if date != tab_date[idp]:
                    tab_date[idp] = date
                (ok, nquery, bgp) = validate(
                    ParallelCounter(stat, date), line, host, query,
                    default_prefixes, doTPFC)
                logging.debug('Analyse "%s" pour %s', ok, host)
                if ok:
                    s = buildXMLBGP(nquery, param_list, bgp, host, date, line)
                    if s is not None:
                        with sem:
                            saveEntry(file, s, host)
        except Empty as e:
            logging.info('%d - %s empty!' %(idp, os.getpid()) )
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
max_processes = mp.cpu_count()
nb_processes_default = min(4, max_processes / 2)
parser.add_argument("-p", "--proc", type=int, default=nb_processes_default, dest="nb_processes",
                    help="Number of processes used to extract (%d by default) over %d usuable processes)" % (nb_processes_default,max_processes))
args = parser.parse_args()
(refDate, baseDir, f_in, doRanking, doTPFC) = manageStdArgs(args)

logging.info('Lecture des préfixes par défaut')
default_prefixes = loadPrefixes()

logging.info('Initialisations')
pattern = makeLogPattern()
old_date = ''
nb_lines = 0
file_set = dict()

nb_processes = min(args.nb_processes,max_processes)
logging.info('Lancement des %d processus de traitement', nb_processes)
sem = Lock()
stat = Stat()
manager = mp.Manager()
tab_date = manager.dict()
for i in range(nb_processes) :
    tab_date[i]=''
compute_queue = mp.Queue()
process_list = [
    mp.Process(
        target=compute, args=(i, tab_date, sem, compute_queue, stat, default_prefixes, doTPFC))
    for i in range(nb_processes)
]
for process in process_list:
    process.start()

cpt = ParallelCounter(stat)

if doRanking:
    logging.info('Lancement des %d processus d\'analyse', nb_processes)
    ranking_queue = mp.Queue()
    ranking_list = [
        mp.Process(target=analyse, args=(ranking_queue, ))
        for _ in range(nb_processes)
    ]
    for process in ranking_list:
        process.start()

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
        file_set[date] = set()
        rep = newDir(baseDir, date)
        cpt = ParallelCounter(stat, date)

    if nb_lines % 1000 == 0:
        logging.info('%d ligne(s) vues', nb_lines)
        for d in file_set:
            if len(file_set[d]) > 0 :
                i=0
                for n in range(nb_processes):
                    if tab_date[n] > d:
                        i += 1
                if i == nb_processes:
                    logging.info('cloture pour %s' % d)
                    for file in file_set[d]:
                        if os.path.isfile(file):
                            closeLog(file)
                            if doRanking:
                                logging.info('Analyse de "%s"', file)
                                ranking_queue.put(file)
                    file_set[d].clear()                  

    cpt.line()
    if dateOk:  # and (nb_lines < 100):
        if (query != ''):
            file = rep + ip + '-be4dbp.xml'
            compute_queue.put((query, param_list, ip, file, date, nb_lines))
            file_set[date].add(file)
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
for d in file_set:
    for file in file_set[d]:
        if os.path.isfile(file):
            closeLog(file)
            if doRanking:
                logging.info('Analyse de "%s"', file)
                ranking_queue.put(file)

if doRanking:
    logging.info('Arrêt des processus d' 'analyse')
    for process in ranking_list:
        ranking_queue.put(None)
    for process in ranking_list:
        process.join()

logging.info('Fin')
