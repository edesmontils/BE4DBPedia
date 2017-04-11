#!/usr/bin/env python3.6
# coding: utf8
#from __future__ import unicode_literals

import time
import datetime
from pprint import pprint
import logging

from bgp import *
from beLib import *

#==================================================


def compute(cpt, line, file, date, host, query, param_list, default_prefixes,
            rep):
    (ok, nquery, bgp) = validate(cpt, line, host, query, default_prefixes)
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
(refDate, baseDir, f_in) = manageStdArgs(args)

logging.info('Initialisations')
pattern = makeLogPattern()
users = dict()
cpt = dict()
old_date = ''
nb_lines = 0
nb_dates = 0

logging.info('Lecture des préfixes par défaut')
default_prefixes = loadPrefixes()

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
        nb_dates += 1
        users[date] = dict()
        old_date = date
        rep = newDir(baseDir, date)
        cpt[date] = Counter(date)
        cur_cpt = cpt[date]

    cur_cpt.line()
    if nb_lines % 1000 == 0:
        logging.info('%d ligne(s) vues (%d pour la date courante)', nb_lines,
                     cur_cpt.getLine())

    if dateOk:
        if (query != ''):
            file = rep + ip + '-be4dbp.xml'
            users[date][ip] = file
            compute(cur_cpt, nb_lines, file, date, ip, query, param_list,
                    default_prefixes, rep)
        else:
            logging.debug('(%d) No query for %s', nb_lines, ip)
            cur_cpt.autre()

logging.info('Fermeture des fichiers')
f_in.close()

# for d in users:
#     for f in users[d]:
#         file = users[d][f]
#         if os.path.isfile(file):
#             closeLog(file)
#             rankAnalysis(file)

logging.info('Fin')
print('Nb line(s) : ', nb_lines)
print('Nb date(s) : ', nb_dates)
total = Counter()
for d in cpt:
    total.join(cpt[d])
    cpt[d].print()
print('=========== total =============')
total.print()
