#!/usr/bin/env python3.6
# coding: utf8
"""
Application to extract BGP from a DBPedia log.
"""
#    Copyright (C) 2017 by
#    Emmanuel Desmontils <emmanuel.desmontils@univ-nantes.fr>
#    Patricia Serrano-Alvarado <patricia.serrano-alvarado@univ-nantes.fr>
#    All rights reserved.
#    GPL v 2.0 license.

import time
import datetime
from pprint import pprint
import logging

from bgp import *
from beLib import *

#==================================================


def compute(cpt, line, file, date, host, query, param_list, rep, ctx):
    (ok, nquery, bgp) = validate(cpt, line, host, query, ctx)
    if ok:
        logging.debug('ok (%d) for %s' % (line, query))
        entry = buildXMLBGP(nquery, param_list, bgp, host, date, line)
        if entry is not None:
            saveEntry(file, entry, host)


#==================================================
#==================================================
#==================================================

# Traitement de la ligne de commande
# https://docs.python.org/3/library/argparse.html
# https://docs.python.org/3/howto/argparse.html

ctx = Context('BGP Extractor for DBPedia log.')

logging.info('Initialisations')
pattern = makeLogPattern()
users = dict()
cpt = dict()
old_date = ''

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
        ctx.newDate(date)
        users[date] = dict()
        old_date = date
        rep = ctx.newDir(date)
        cpt[date] = Counter(date)
        cur_cpt = cpt[date]

    cur_cpt.line()
    if ctx.lines() % 1000 == 0:
        logging.info('%d line(s) viewed (%d for the current date)', ctx.lines(), cur_cpt.getLine())
        ctx.save()

    if dateOk:
        if (query != ''):
            file = rep + ip + '-be4dbp.xml'
            users[date][ip] = file
            compute(cur_cpt, ctx.lines(), file, date, ip, query, param_list, rep, ctx)
        else:
            logging.debug('(%d) No query for %s', ctx.lines(), ip)
            cur_cpt.autre()


logging.info('Fermeture des fichiers')
for d in users:
    for f in users[d]:
        file = users[d][f]
        if existFile(file):
            closeLog(file)
            if ctx.doRanking: 
                rankAnalysis(file) 

total = Counter()
for d in cpt:
    total.join(cpt[d])
    cpt[d].print()
print('=========== total =============')
total.print()
        
ctx.close()
