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

from tools import *

from Endpoint import *
from Stat import *

from lxml import etree  # http://lxml.de/index.html#documentation

#==================================================
#==================================================

def analyse(in_queue, endpoint,emptyTest, stat):
    logging.debug('Start analyse worker "%s"', os.getpid())
    while True:
        try:
            mess = in_queue.get()
            if mess is None:
                break
            else:
                logging.debug('Treat mess in %s %s', os.getpid(), mess)
                TestAnalysis(mess, endpoint,emptyTest, stat)
        except Empty as e:
            print('empty!')
        except Exception as e:
            print(mess, e)
            break
    logging.debug('Stop analyse worker "%s"', os.getpid())

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


def test(endpoint, entry, stat,emptyTest, cacheTO):
    ide = entry.get('logline')
    query = entry.find('request').text
    (ok, mss) = testQuery(query,endpoint,cacheTO)
    if ok:
        entry.set('valid',emptyTest)
        stat.stdput('valid')
    elif mss == 'Empty':
        entry.set('valid','Empty'+emptyTest)
        stat.stdput('empty')
    elif mss == 'QBF':
        stat.stdput('bfq')
        entry.set('valid','QBF'+emptyTest)
    elif mss == 'TO':
        stat.stdput('to')
        entry.set('valid','TO'+emptyTest)
    else:
        print('PB error for:',ide)
        stat.stdput('other')


# def test(endpoint, entry, stat,emptyTest, cacheTO):
#     ide = entry.get('logline')
#     query = entry.find('request').text
#     hq = endpoint.hash(query)
#     try:
#         if hq in cacheTO:
#             stat.stdput('to')
#             print('Another Timeout for',ide)
#             entry.set('valid','TO'+emptyTest)
#         else:            
#             (ok, wf) = endpoint.notEmpty(query)
#             if ok:
#                 entry.set('valid',emptyTest)
#                 stat.stdput('valid')
#                 #print('OK for',ide)
#             elif wf:
#                 #print('Empty for',ide)
#                 entry.set('valid','Empty'+emptyTest)
#                 stat.stdput('empty')
#             else:
#                 #print('PB QBF for:',ide)
#                 stat.stdput('bfq')
#                 entry.set('valid','QBF'+emptyTest)
#     except Exception as e:
#         if reTimeout.search(e.__str__()):
#             stat.stdput('to')
#             print('Timeout for',ide)
#             entry.set('valid','TO'+emptyTest)
#             cacheTO.add(hq)
#         else:
#             print('PB error for:',ide)
#             print(e)
#             stat.stdput('other')

#==================================================

def TestAnalysis(file, endpoint,emptyTest,stat):
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
    dtd = etree.DTD('./resources/log.dtd')
    #---
    assert dtd.validate(tree), '%s non valide au chargement : %s' % (
        file, dtd.error_log.filter_from_errors()[0])
    #---
    nbe = 0
    cacheTO = set()
    for entry in tree.getroot():
        nbe += 1
        valid = entry.get("valid")
        ide = entry.get('logline')
        if  valid == None:
            test(endpoint,entry,stat,emptyTest, cacheTO)
        else:
            if valid == emptyTest: #in [MODE_TE_SPARQL, MODE_TE_TPF]:
                stat.stdput('valid')
                #print('Always OK for',ide)
            elif valid == 'Empty'+emptyTest: #in ['Empty'+MODE_TE_SPARQL, 'Empty'+MODE_TE_TPF]:
                stat.stdput('empty')
                #print('Always Empty for',ide)
            elif valid == 'QBF'+emptyTest: #in ['QBF'+MODE_TE_SPARQL, 'QBF'+MODE_TE_TPF]:
                #print('Always QBF for:',ide)
                stat.stdput('bfq')
            elif valid == 'TO'+emptyTest: #in ['TO'+MODE_TE_SPARQL, 'TO'+MODE_TE_TPF]:
                print('Old Timeout, redo:',ide)
                test(endpoint,entry,stat,emptyTest,cacheTO)
            else:
                #print('Tested for the first time')
                test(endpoint,entry,stat,emptyTest,cacheTO)
    try:
        #file_tested = file[:-4]+'-tested.xml'
        logging.debug('Ecriture de "%s"', file_tested)
        tosave = etree.tostring(
            tree,
            encoding="UTF-8",
            xml_declaration=True,
            pretty_print=True,
            doctype='<!DOCTYPE log SYSTEM "log.dtd">')
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
