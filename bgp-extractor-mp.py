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

from multiprocessing import Process, Lock, Queue, BoundedSemaphore, Semaphore
import multiprocessing as mp
from queue import Empty

import logging

from bgp import *
from beLib import *
from Stat import *
from Context import *
from Counter import *

#==================================================


def compute(idp, tab_date, sem, in_queue, stat, ctx):
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
                    ParallelCounter(stat, date), line, host, query, ctx)
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

ctx = ParallelContext('Parallel BGP Extractor for DBPedia log.')

logging.info('Initialisations')
pattern = makeLogPattern()
old_date = ''
file_set = dict()

logging.info('Lancement des %d processus de traitement', ctx.nb_processes)
sem = Lock()
stat = Stat()
manager = mp.Manager()
tab_date = manager.dict()
for i in range(ctx.nb_processes) :
    tab_date[i]=''
compute_queue = mp.Queue(ctx.nb_processes)
process_list = [
    mp.Process(
        target=compute, args=(i, tab_date, sem, compute_queue, stat, ctx))
    for i in range(ctx.nb_processes)
]
for process in process_list:
    process.start()

cpt = ParallelCounter(stat)

if ctx.doRanking:
    logging.info('Lancement des %d processus d\'analyse', ctx.nb_processes)
    ranking_queue = mp.Queue()
    ranking_list = [
        mp.Process(target=analyse, args=(ranking_queue, ))
        for _ in range(ctx.nb_processes)
    ]
    for process in ranking_list:
        process.start()

logging.info('Lancement du traitement')
for line in ctx.file():
    ctx.newLine()
    m = pattern.match(line)
    (query, date, param_list, ip) = extract(m.groupdict())

    if (date != old_date):
        dateOk = date.startswith(ctx.refDate)
        if dateOk:
            logging.info('%d - Study of %s', ctx.lines(), date)
        else:
            logging.info('%d - Pass %s', ctx.lines(), date)

        old_date = date
        ctx.newDate(date)
        file_set[date] = set()
        rep = ctx.newDir(date)
        cpt = ParallelCounter(stat, date)

    if ctx.lines() % 1000 == 0:
        logging.info('%d line(s) viewed', ctx.lines())
        ctx.save()
        for d in file_set:
            if len(file_set[d]) > 0 :
                i=0
                for n in range(ctx.nb_processes):
                    if tab_date[n] > d:
                        i += 1
                if i == ctx.nb_processes:
                    logging.info('Close %s' % d)
                    for file in file_set[d]:
                        if existFile(file):
                            closeLog(file)
                            if ctx.doRanking:
                                logging.info('Study of "%s"', file)
                                ranking_queue.put(file)
                    file_set[d].clear()                  

    cpt.line()
    if dateOk:  # and (ctx.lines() < 100):
        if (query != ''):
            file = rep + ip + '-be4dbp.xml'
            compute_queue.put( (query, param_list, ip, file, date, ctx.lines()) )
            file_set[date].add(file)
        else:
            logging.debug('(%d) No query for %s', ctx.lines(), ip)
            cpt.autre()

logging.info('Arrêt des processus de traitement')
for process in process_list:
    compute_queue.put(None)
for process in process_list:
    process.join()

stat.stop()

logging.info('Terminaison des fichiers')
for d in file_set:
    for file in file_set[d]:
        if existFile(file):
            closeLog(file)
            if ctx.doRanking:
                logging.info('Analyse de "%s"', file)
                ranking_queue.put(file)

if ctx.doRanking:
    logging.info('Arrêt des processus d' 'analyse')
    for process in ranking_list:
        ranking_queue.put(None)
    for process in ranking_list:
        process.join()

ctx.close()
