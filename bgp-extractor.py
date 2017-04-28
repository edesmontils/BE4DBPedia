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

from lib.bgp import *
from lib.beLib import *
from lib.Context import *
from tools.Counter import *
from lib.beRanking import *

#==================================================

def compute(line, file, date, host, query, param_list, rep, ctx):
    (ok, nquery, bgp, qlt) = validate(date, line, host, query, ctx)
    if ok:
        logging.debug('ok (%d) for %s' % (line, nquery))
        entry = buildXMLBGP(nquery, param_list, bgp, host, date, line, qlt)
        if entry is not None:
            saveEntry(file, entry, host)

#==================================================

# Traitement de la ligne de commande
# https://docs.python.org/3/library/argparse.html
# https://docs.python.org/3/howto/argparse.html

ctx = Context('BGP Extractor for DBPedia log.')

logging.info('Initialisations')
users = dict()
old_date = ''

logging.info('Lancement du traitement')
for (query, date, param_list, ip) in ctx.file():
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

    ctx.stat.put(date,'line')
    if ctx.lines() % 1000 == 0:
        logging.info('%d line(s) viewed', ctx.lines())
        ctx.save()

    if dateOk:
        file = rep + ip + '-be4dbp.xml'
        users[date][ip] = file
        compute(ctx.lines(), file, date, ip, query, param_list, rep, ctx)

logging.info('Fermeture des fichiers')
for d in users:
    for f in users[d]:
        file = users[d][f]
        if existFile(file):
            closeLog(file)
            if ctx.doRanking: 
                rankAnalysis(file)

ctx.close()
