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
from queue import Empty
import os
import logging

from lib.bgp import *

from operator import itemgetter
from lxml import etree  # http://lxml.de/index.html#documentation

#==================================================

MODE_RA_NOTEMPTY = 'NotEmpty'
MODE_RA_VALID = 'Valid'
MODE_RA_WF = 'WellFormed'
MODE_RA_ALL = 'All'

#==================================================

def analyse(in_queue, mode, stat):
    logging.debug('Start analyse worker "%s"', os.getpid())
    while True:
        try:
            mess = in_queue.get()
            if mess is None:
                break
            else:
                logging.debug('Treat mess in %s %s', os.getpid(), mess)
                rankAnalysis(mess, mode, stat)
        except Empty as e:
            print('empty!')
        except Exception as e:
            print(mess, e)
            break
    logging.debug('Stop analyse worker "%s"', os.getpid())

#==================================================


def addBGP2Rank(bgp, nquery, line, ranking):
    ok = False
    for (i, (d, n, query, ll)) in enumerate(ranking):
        if bgp == d:
            # if equals(bgp,d) :
            ok = True
            break
    if ok:
        ll.add(line)
        ranking[i] = (d, n+1, query, ll)
    else:
        ranking.append( (bgp, 1 , nquery, {line}) )


#==================================================

def entryOk(entry, mode):
    #ok = True
    valid = entry.get("valid")
    #print('Vérif pour %s' % valid)
    if  valid == None:
        return mode == MODE_RA_ALL
    else:
        if valid.startswith('Empty'):
            #print('is Empty')
            return mode == {MODE_RA_VALID,MODE_RA_WF,MODE_RA_ALL}
        elif valid.startswith('QBF'):
            #print('is QBF')
            return mode == MODE_RA_ALL
        elif valid.startswith('TO'):
            #print('is TO')
            return mode in {MODE_RA_ALL,MODE_RA_WF}
        elif valid == 'NotTested':
            #print('Not tested')
            return mode == MODE_RA_ALL
        else: # TPF ou SPARQL
            #print('Other')
            return mode in {MODE_RA_NOTEMPTY,MODE_RA_VALID,MODE_RA_WF,MODE_RA_ALL}

#==================================================

def rankAnalysis(file, mode, stat):
    logging.debug('rankAnalysis for %s' % file)
    #print('Traitement de %s' % file)
    parser = etree.XMLParser(recover=True, strip_cdata=True)
    tree = etree.parse(file, parser)
    #---
    dtd = etree.DTD('./resources/log.dtd')
    assert dtd.validate(tree), '%s non valide au chargement : %s' % (
        file, dtd.error_log.filter_from_errors()[0])
    #---

    ranking = []
    nbe = 0
    date = ''
    for entry in tree.getroot():
        if entryOk(entry,mode):
            #print('entry ok')
            nbe += 1
            if nbe == 1:
                date = entry.get('datetime')
            ide = entry.get('logline')
            bgp = unSerializeBGP(entry.find('bgp'))
            cbgp = canonicalize_sparql_bgp(bgp)
            query = entry.find('request').text
            addBGP2Rank(cbgp, query, ide, ranking)
    ranking.sort(key=itemgetter(1), reverse=True)
    node_tree_ranking = etree.Element('ranking')
    node_tree_ranking.set('ip', tree.getroot().get('ip'))
    rank = 0
    old_freq = 0;
    stat.put(date,'file')
    nb = 0
    for (i, (bgp, freq, query, lines)) in enumerate(ranking):
        nb +=1
        if freq != old_freq:
            stat.put(date,'rank')
            rank += 1
            old_freq = freq
        f = freq / nbe
        stat.mput(date,'occurrences',freq)
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
        request_node = etree.SubElement(node_r, 'request')
        request_node.text = query
    if nb > 200:
        stat.put(date,'cut200')
    stat.mput(date,'entry-rank',nb)
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

#==================================================
#==================================================
#==================================================

if __name__ == '__main__':
    print('main')
