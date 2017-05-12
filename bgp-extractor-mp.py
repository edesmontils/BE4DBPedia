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

from lib.bgp import *
from lib.beLib import *
from tools.Stat import *
from lib.Context import *
from lib.beRanking import *

from tools.ProcessSet import *

#==================================================

def compute(idp, mess, tab_date, ctx):
    (query, param_list, host, file, date, line) = mess
    logging.debug('Treat mess in %s %s', os.getpid(), host)
    if date != tab_date[idp]:
        tab_date[idp] = date
    (ok, nquery, bgp, qlt) = validate(date,line, host, query, ctx)
    logging.debug('Analyse "%s" pour %s', ok, host)
    if ok:
        s = buildXMLBGP(nquery, param_list, bgp, host, date, line, qlt)
        if s is not None:
            with ctx.sem:
                saveEntry(file, s, host)

#==================================================
#==================================================
#==================================================

# Traitement de la ligne de commande

ctx = ParallelContext('Parallel BGP Extractor for DBPedia log.')

logging.info('Initialisations')
old_date = ''
file_set = dict()

logging.info('Lancement des %d processus de traitement', ctx.nb_processes)

manager = mp.Manager()
tab_date = manager.dict()
for i in range(ctx.nb_processes) :
    tab_date[i]=''

psExtractor = ProcessSet(ctx.nb_processes, compute, tab_date, ctx)
psExtractor.start()

if ctx.doRanking:
    logging.info('Lancement des %d processus d\'analyse', ctx.nb_processes)
    psRanking = ProcessSet(ctx.nb_processes, rankAnalysis, MODE_RA_ALL)
    statRank = Stat(Counter, ['file','cut'+str(MODE_CUTE),'rank','entry-rank','occurrences'] )
    psRanking.setStat(statRank)
    psRanking.start()

logging.info('Lancement du traitement')
for (query, date, param_list, ip) in ctx.file():
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
                                psRanking.put(file)
                    file_set[d].clear()                  

    ctx.stat.put(date,'line')#line()
    if dateOk:  # and (ctx.lines() < 100):
        file = rep + ip + '-be4dbp.xml'
        psExtractor.put( (query, param_list, ip, file, date, ctx.lines()) )
        file_set[date].add(file)

logging.info('Arrêt des processus de traitement')
psExtractor.stop()

logging.info('Terminaison des fichiers')
for d in file_set:
    for file in file_set[d]:
        if existFile(file):
            closeLog(file)
            if ctx.doRanking:
                logging.info('Analyse de "%s"', file)
                psRanking.put(file)

if ctx.doRanking:
    logging.info('Arrêt des processus d\'analyse')
    psRanking.stop()
    
    statRank.stop(True)
    csvname = 'be4dbp-ranking-'+date2filename(now())+'.csv'
    statRank.saveCSV(csvname)

ctx.close()
