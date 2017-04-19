#!/usr/bin/env python3.6
# coding: utf8
"""
Tools to manage log extraction
"""
#    Copyright (C) 2017 by
#    Emmanuel Desmontils <emmanuel.desmontils@univ-nantes.fr>
#    Patricia Serrano-Alvarado <patricia.serrano-alvarado@univ-nantes.fr>
#    All rights reserved.
#    GPL v 2.0 license.

from pprint import pprint

import os
import logging

from bgp import *

from operator import itemgetter
from lxml import etree  # http://lxml.de/index.html#documentation

#==================================================

def analyse(in_queue):
    logging.debug('Start analyse worker "%s"', os.getpid())
    while True:
        try:
            mess = in_queue.get()
            if mess is None:
                break
            else:
                logging.debug('Treat mess in %s %s', os.getpid(), mess)
                rankAnalysis(mess)
        except Empty as e:
            print('empty!')
        except Exception as e:
            print(mess, e)
            break
    logging.debug('Stop analyse worker "%s"', os.getpid())

#==================================================


def addBGP2Rank(bgp, line, ranking):
    ok = False
    for (i, (d, n, ll)) in enumerate(ranking):
        if bgp == d:
            # if equals(bgp,d) :
            ok = True
            break
    if ok:
        ll.add(line)
        ranking[i] = (d, n+1, ll)
    else:
        ranking.append( (bgp, 1 , {line}) )


#==================================================


def rankAnalysis(file):
    logging.debug('rankAnalysis for %s' % file)
    parser = etree.XMLParser(recover=True, strip_cdata=True)
    tree = etree.parse(file, parser)
    #---
    assert etree.DTD('./resources/log.dtd').validate(tree), '%s non valide au chargement : %s' % (
        file, dtd.error_log.filter_from_errors()[0])
    #---
    ranking = []
    nbe = 0
    for entry in tree.getroot():
        nbe += 1
        ide = entry.get('logline')
        bgp = unSerializeBGP(entry.find('bgp'))
        cbgp = canonicalize_sparql_bgp(bgp)
        addBGP2Rank(cbgp, ide, ranking)
    ranking.sort(key=itemgetter(1), reverse=True)
    node_tree_ranking = etree.Element('ranking')
    node_tree_ranking.set('ip', tree.getroot().get('ip'))
    rank = 0
    old_freq = 0;
    for (i, (bgp, freq, lines)) in enumerate(ranking):
        if freq != old_freq:
            rank += 1
            old_freq = freq
        f = freq / nbe
        node_r = etree.SubElement(
            node_tree_ranking,
            'entry-rank',
            attrib={
                'frequence': '{:04.3f}'.format(f),
                'nb-occurrences': '{:d}'.format(freq),
                'rank':'{:d}'.format(rank),
                'lines':'{:s}'.format(" ".join(x for x in lines))
                }
        )
        node_b = serializeBGP(bgp)
        node_r.append(node_b)
    try:
        file_ranking = file[:-4]+'-ranking.xml'
        logging.debug('Ecriture de "%s"', file_ranking)
        tosave = etree.tostring(
            node_tree_ranking,
            encoding="UTF-8",
            xml_declaration=True,
            pretty_print=True,
            doctype='<!DOCTYPE ranking SYSTEM "ranking.dtd">')
        try:
            f = open(file_ranking, 'w')
            f.write(tosave.decode('utf-8'))
        except Exception as e:
            logging.error(
                'PB Rank Analysis saving %s : %s',
                file,
                e.__str__())
        finally:
            f.close()
    except etree.DocumentInvalid as e:
        logging.warning('PB Rank Analysis, %s not validated : %s' % (file, e))
