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

parser = setStdArgs('BGP Extractor for DBPedia log.')
args = parser.parse_args()
ctx = Context(args)

logging.info('Initialisations')
pattern = makeLogPattern()
users = dict()
cpt = dict()
old_date = ''
nb_lines = 0
nb_dates = 0

logging.info('Lancement du traitement')
for line in ctx.file():
    nb_lines += 1
    m = pattern.match(line)
    (query, date, param_list, ip) = extract(m.groupdict())

    if (date != old_date):
        dateOk = date.startswith(ctx.refDate)
        if dateOk:
            logging.info('%d - Study of %s', nb_lines, date)
        else:
            logging.info('%d - Pass %s', nb_lines, date)
        nb_dates += 1
        users[date] = dict()
        old_date = date
        rep = newDir(ctx.baseDir, date)
        cpt[date] = Counter(date)
        cur_cpt = cpt[date]

    cur_cpt.line()
    if nb_lines % 1000 == 0:
        logging.info('%d line(s) viewed (%d for the current date)', nb_lines,
                     cur_cpt.getLine())

    if dateOk:
        if (query != ''):
            file = rep + ip + '-be4dbp.xml'
            users[date][ip] = file
            compute(cur_cpt, nb_lines, file, date, ip, query, param_list, rep, ctx)
        else:
            logging.debug('(%d) No query for %s', nb_lines, ip)
            cur_cpt.autre()

ctx.close()

logging.info('Fermeture des fichiers')
for d in users:
    for f in users[d]:
        file = users[d][f]
        if existFile(file):
            closeLog(file)
            if ctx.doRanking: 
                rankAnalysis(file) 

logging.info('Fin')

print('Nb line(s) : ', nb_lines)
print('Nb date(s) : ', nb_dates)
total = Counter()
for d in cpt:
    total.join(cpt[d])
    cpt[d].print()
print('=========== total =============')
total.print()
