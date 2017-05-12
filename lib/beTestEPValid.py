#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
"""
Application to test request on SPARQL or TPF server
"""
#    Copyright (C) 2017 by
#    Emmanuel Desmontils <emmanuel.desmontils@univ-nantes.fr>
#    Patricia Serrano-Alvarado <patricia.serrano-alvarado@univ-nantes.fr>
#    All rights reserved.
#    GPL v 2.0 license.



import multiprocessing as mp
from queue import Empty

import datetime as dt

import logging
import argparse

from tools.tools import *

from tools.Endpoint import *
from tools.Stat import *

from lxml import etree  # http://lxml.de/index.html#documentation

#==================================================
#==================================================

reTimeout = re.compile(r'TimeoutExpired')

#==================================================

def testQuery(qr,endpoint, cacheTO):
    """
    test if the query has at least one response
    """
    try:
        hq = endpoint.hash(qr)
        if hq in cacheTO:
            return (False, 'TO')
        else:
            (ok, wellFormed) = endpoint.notEmpty(qr)
            if ok:
                return (ok, 'NotEmpty')
            elif wellFormed:
                return (False, 'Empty')
            else:
                return (False,'QBF')
    except Exception as e:
        message = e.__str__()
        if message.startswith('QueryBadFormed'):
            return (False, 'QBF')
        elif reTimeout.search(e.__str__()):
            cacheTO.add(hq)
            return (False, 'TO')
        else:
            return (False, 'autre')


def test(ip,date,endpoint, entry, stat,emptyTest, cacheTO):
    ide = entry.get('logline')
    query = entry.find('request').text
    (ok, mss) = testQuery(query,endpoint,cacheTO)
    if ok:
        entry.set('valid',emptyTest)
        stat.put(date,'valid')
        stat.put(ip,'valid')
    elif mss == 'Empty':
        entry.set('valid','Empty'+emptyTest)
        stat.put(date,'empty')
        stat.put(ip,'empty')
    elif mss == 'QBF':
        stat.put(date,'bfq')
        stat.put(ip,'bfq')
        entry.set('valid','QBF'+emptyTest)
    elif mss == 'TO':
        stat.put(date,'to')
        stat.put(ip,'to')
        entry.set('valid','TO'+emptyTest)
    else:
        print('PB error for:',ide)
        stat.put(date,'other')
        stat.put(ip,'other')

def TestAnalysis(idp, file, stat, endpoint,emptyTest):
    logging.debug('testAnalysis for %s' % file)
    print('testAnalysis for %s' % file)
    file_tested = file[:-4]+'-tested-'+emptyTest+'.xml'
    if existFile(file_tested):
        print('%s already tested' % file_tested)
        parser = etree.XMLParser(recover=True, strip_cdata=True)
        tree = etree.parse(file_tested, parser)
    else:
        parser = etree.XMLParser(recover=True, strip_cdata=True)
        tree = etree.parse(file, parser)
    dtd = etree.DTD('http://documents.ls2n.fr/be4dbp/log.dtd')#'./resources/log.dtd')
    #---
    assert dtd.validate(tree), '%s non valide au chargement : %s' % (
        file, dtd.error_log.filter_from_errors()[0])
    #---
    nbe = 0
    date='no-date'
    cacheTO = set()
    ip = 'ip-'+tree.getroot().get('ip').split('-')[0]
    for entry in tree.getroot():
        nbe += 1
        if nbe == 1:
            date = entry.get('datetime')
        valid = entry.get("valid")
        ide = entry.get('logline')
        if  valid == None:
            test(ip,date,endpoint,entry,stat,emptyTest, cacheTO)
        else:
            if valid == emptyTest: #in [MODE_TE_SPARQL, MODE_TE_TPF]:
                stat.put(date,'valid')
                stat.put(ip,'valid')
                #print('Always OK for',ide)
            elif valid == 'Empty'+emptyTest: #in ['Empty'+MODE_TE_SPARQL, 'Empty'+MODE_TE_TPF]:
                stat.put(date,'empty')
                stat.put(ip,'empty')
                #print('Always Empty for',ide)
            elif valid == 'QBF'+emptyTest: #in ['QBF'+MODE_TE_SPARQL, 'QBF'+MODE_TE_TPF]:
                #print('Always QBF for:',ide)
                stat.put(date,'bfq')
                stat.put(ip,'bfq')
            elif valid == 'TO'+emptyTest: #in ['TO'+MODE_TE_SPARQL, 'TO'+MODE_TE_TPF]:
                if (endpoint.getTimeOut() > 0):
                    print('Old Timeout, redo:',ide)
                    test(ip,date,endpoint,entry,stat,emptyTest,cacheTO)
                else:
                    stat.put(date,'to')
                    stat.put(ip,'to')
            else:
                #print('Tested for the first time')
                test(ip,date,endpoint,entry,stat,emptyTest,cacheTO)
    try:
        #file_tested = file[:-4]+'-tested.xml'
        logging.debug('Ecriture de "%s"', file_tested)
        tosave = etree.tostring(
            tree,
            encoding="UTF-8",
            xml_declaration=True,
            pretty_print=True,
            doctype='<!DOCTYPE log SYSTEM "http://documents.ls2n.fr/be4dbp/log.dtd">')
        try:
            f = open(file_tested, 'w')
            f.write(tosave.decode('utf-8'))
        except Exception as e:
            logging.error(
                'PB Test Analysis saving %s : %s',
                file,
                e.__str__())
        finally:
            f.close()
    except etree.DocumentInvalid as e:
        logging.warning('PB Test Analysis, %s not validated : %s' % (file, e))


#==================================================
#==================================================
#==================================================

if __name__ == '__main__':
    print('main')
