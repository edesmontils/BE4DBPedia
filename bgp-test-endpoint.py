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

MODE_SPARQL = 'SPARQL'
MODE_TPF = 'TPF'
DEFAULT_EP = 'http://localhost:5001/dbpedia_3_9'

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

def test(endpoint, query, entry, stat,emptyTest):
    try:
        (ok, wf) = endpoint.notEmpty(query)
        if ok:
            entry.set('valid',emptyTest)
            stat.put('','valid')
            print('OK for',ide)
        elif wf:
            print('Empty for',ide)
            entry.set('valid','Empty'+emptyTest)
            stat.put('','empty')
        else:
            print('PB  wf for:',ide)
            stat.put('','bfq')
            entry.set('valid','QBF'+emptyTest)
    except Exception as e:
        print('PB error for:',ide)
        stat.put('','other')
    # return entry


def TestAnalysis(file, endpoint,emptyTest):
    logging.debug('testAnalysis for %s' % file)
    print('testAnalysis for %s' % file)
    file_tested = file[:-4]+'-tested.xml'
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
    for entry in tree.getroot():
        nbe += 1
        ide = entry.get('logline')
        query = entry.find('request').text
        valid = entry.get("valid")
        if  valid == None:
            test(endpoint,query,entry,stat,emptyTest)
        else:
            if valid in [MODE_SPARQL, MODE_TPF]:
                stat.put('','valid')
                print('Always OK for',ide)
            elif valid in ['Empty'+MODE_SPARQL, 'Empty'+MODE_TPF]:
                stat.put('','empty')
                print('Always Empty for',ide)
            elif valid  in ['QBF'+MODE_SPARQL, 'QBF'+MODE_TPF]:
                print('Always wf for:',ide)
                stat.put('','bfq')
            else:
                test(endpoint,query,entry,stat,emptyTest)
                # stat.put('','other')
    try:
        file_tested = file[:-4]+'-tested.xml'
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

def manageLogging(logLevel, logfile = 'be4dbp.log'):
    if logLevel:
        # https://docs.python.org/3/library/logging.html?highlight=log#module-logging
        # https://docs.python.org/3/howto/logging.html#logging-basic-tutorial
        logging.basicConfig(
            format='%(levelname)s:%(asctime)s:%(message)s',
            filename=logfile,filemode='w',
            level=getattr(logging,logLevel))

#==================================================

parser = argparse.ArgumentParser(description='Etude des requêtes')
parser.add_argument('files', metavar='file', nargs='+',
                    help='files to analyse')
parser.add_argument("-l", "--log", dest="logLevel",
                        choices=[
                            'DEBUG',
                            'INFO',
                            'WARNING',
                            'ERROR',
                            'CRITICAL'],
                        help="Set the logging level", default='INFO')
parser.add_argument("-p", "--proc", type=int, default=mp.cpu_count(), dest="nb_processes",
                    help="Number of processes used (%d by default)" % mp.cpu_count())
parser.add_argument("-e","--empty", help="Request a SPARQL or a TPF endpoint to verify the query and test it returns at least one triple (%s by default)" % MODE_TPF,
                choices=[MODE_SPARQL,MODE_TPF],dest="doEmpty",default=MODE_TPF)
parser.add_argument("-ep","--endpoint", help="The endpoint requested for the '-e' ('--empty') option (for exemple '%s' for %s by default)" % (DEFAULT_EP,MODE_TPF),
                dest="ep", default=DEFAULT_EP)
args = parser.parse_args()
manageLogging(args.logLevel, 'be4dbp-tests-'+date2filename(now())+'.log')

file_set = args.files

stat = AbstractStat(AbstractCounter, ['valid', 'empty', 'bfq', 'other'] )

current_dir = os.getcwd()
resourcesDir = 'resources'
emptyTest = args.doEmpty
if emptyTest == MODE_SPARQL:
    if args.ep == '':
        endpoint = SPARQLEP(cacheDir = current_dir+'/'+resourcesDir)
    else:
        endpoint = SPARQLEP(args.ep, cacheDir = current_dir+'/'+resourcesDir)
else:
    if args.ep == '':
        endpoint = TPFEP(cacheDir = current_dir+'/'+resourcesDir)
    else:
        endpoint = TPFEP(service = args.ep, cacheDir = current_dir+'/'+resourcesDir)
    endpoint.setEngine('/Users/desmontils-e/Programmation/TPF/Client.js-master/bin/ldf-client')
logging.info('Empty responses tests with %s' % endpoint)
endpoint.caching(True)
endpoint.setTimeOut(60)

nb_processes = args.nb_processes
logging.info('Lancement des %d processus d\'analyse', nb_processes)
compute_queue = mp.Queue(ctx.nb_processes * 6)
process_list = [
    mp.Process(target=analyse, args=(compute_queue, endpoint, emptyTest, stat))
    for _ in range(nb_processes)
]
for process in process_list:
    process.start()

nbf = len(file_set)
dixpercent = int(nbf*0.1)
print (dixpercent)
no = 0
sys.exit()

for file in file_set:
    no +=1
    if no % dixpercent == 0: endpoint.saveCache()
    if existFile(file):
        logging.debug('Analyse de "%s"', file)
        compute_queue.put(file)

logging.info('Arrêt des processus d' 'analyse')
for process in process_list:
    compute_queue.put(None)
for process in process_list:
    process.join()

logging.info('Fin')
endpoint.saveCache()
stat.stop(True)
logging.info('End')
