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

import re
import time
import datetime as dt
from urllib.parse import urlparse, parse_qsl
import csv
import sys
import os
import os.path
import shutil

import logging
import argparse

import rdflib
from rdflib.query import Processor, Result, UpdateProcessor
from rdflib.plugins.sparql.sparql import Query, SPARQLError
from rdflib.plugins.sparql.parser import parseQuery, parseUpdate
from rdflib.plugins.sparql.algebra import translateQuery, translateUpdate, pprintAlgebra
from rdflib.plugins.sparql.evaluate import evalQuery
from rdflib.plugins.sparql.update import evalUpdate
from rdflib import Graph, Literal, BNode, Namespace, RDF, URIRef, Variable

from bgp import *
from Endpoint import *
from queryTools import *

from operator import itemgetter
# import xml.etree.ElementTree as ET
from lxml import etree  # http://lxml.de/index.html#documentation


#==================================================


def extract(res):
    # if res["user"] == "-": res["user"] = None

    # res["status"] = int(res["status"])

    # if res["size"] == "-":
    #     res["size"] = 0
    # else:
    #     res["size"] = int(res["size"])

    # if res["referer"] == "-":
    #     res["referer"] = None

    tt = time.strptime(res["time"][:-6], "%d/%b/%Y %H:%M:%S")
    tt = list(tt[:6]) + [0, Timezone(res["time"][-5:])]
    # res["time"] = dt.datetime(*tt)
    date = dt.datetime(*tt).__str__().replace(' ', 'T')

    url = res['request'].split(' ')[1]
    # res['request'] = url
    param = url.split('?')[1]
    # param_list = parse_qsl(param)

    param_list = []
    query = ''
    for (p, q) in parse_qsl(param):
        if p == 'query':
            query = ' ' + q + ' '
        elif p == 'qtxt':
            query = ' ' + q + ' '
        else:
            # .replace('"', '\'').replace('<', '').replace('>', '')))
            param_list.append((p, q))
    #query = ' '.join(query.split())
    return (query, date, param_list, res['host'])

#==================================================


def translate(cpt, line, ip, query, tree, ctx):
    try:
        q = translateQuery(tree).algebra

        #---
        assert q is not None
        #---

        try:
        	bgp = getBGP(q)
        	return (True, query, bgp)
        except ValueError as e:
	        logging.debug('PB URI in BGP (%d) : %s\n%s', line, e, query)
	        cpt.err_qr()
	        return (False, None, None)
    except SPARQLError as e:
        logging.debug('PB SPARQLError (%d) : %s\n%s', line, e, query)
        cpt.err_qr()
        return (False, None, None)
    except Exception as e:
        m = e.__str__().split(':')
        if (m[0] == 'Unknown namespace prefix '):
            pr = m[1].strip()
            if (pr in ctx.default_prefixes):
                n_query = 'PREFIX ' + pr + ': <' + \
                    dp[pr] + '> #ADD by SCAN \n' + query
                return translate(cpt, line, ip, n_query, parseQuery(n_query), ctx)
            else:
                logging.debug('PB NS (%d) : %s\n%s', line, e, query)
                cpt.err_ns()
                return (False, None, None)
        else:
            logging.debug('PB translate (%d) : %s\n%s', line, e, query)
            cpt.err_qr()
            return (False, None, None)


#==================================================

reSelect = re.compile(r'(\W+)select(\s+)', re.IGNORECASE)

reUnion = re.compile(r'(\W+)union(\W+)', re.IGNORECASE)

reIsIRI = re.compile(r"""
\WisIRI\s*\(
""", re.IGNORECASE | re.VERBOSE)

reRegex = re.compile(r"""
\Wregex\s*\(
"""
# r"""
# \Wregex\W*
# \(
#     ( [^,()"'<]+ | "[^"]*" | '[^']*' | <[^>]*> | \w* \( [^)]* \) )+
# ( , ( [^,()"'<]+ | "[^"]*" | '[^']*' | <[^>]*> | \w* \( [^)]* \) )+ ){2,}
# \)
# """
, re.IGNORECASE | re.VERBOSE)

reThumbnail = re.compile(r"""
\Wxsd\:date\s*\(
""", re.IGNORECASE | re.VERBOSE)

def isTPFCompatible(query):
  ok = True
  if reIsIRI.search(query) != None:
    return False
  elif reRegex.search(query) != None:
    return False
  elif reThumbnail.search(query) != None:
    return False
  else:
    return ok

def existDBPEDIA(line,query,ctx):
    """
    test if the query has at least one response
    """
    try:
        ok = ctx.endpoint.notEmpty(query)
        return (ok, 'empty')
    except Exception as e:
        message = e.__str__()
        print('Erreur existDBPEDIA:',line, message, query)
        if message.startswith('QueryBadFormed'):
            logging.warning('PB SPARQL Endpoint (QueryBadFormed):%s',e)
            return (False, 'QBF')
        else:
            logging.warning('PB SPARQL Endpoint (autre):%s',e)
            return (False, 'autre')

def validate(cpt, line, ip, query, ctx):
    if (reSelect.search(query) is not None):
        cpt.select()
        if (reUnion.search(query) is None):
            try:
                tree = parseQuery(query)
                (ok, n_query, bgp) = translate(cpt, line, ip, query, tree, ctx)
                if ok:
                    if not(valid(bgp)):
                        cpt.bgp_not_valid()
                        return (False, None, None)
                    if ctx.doTPFC: 
                        if not(isTPFCompatible(n_query)):
                            logging.debug('PB TPF Client (%d) : %s', line, n_query)
                            cpt.err_tpf()
                            return (False, None, None)
                    if ctx.emptyTest is not None :
                        (done, mss) = existDBPEDIA(line,n_query,ctx)
                        if not(done):
                            if mss=='empty':
                                logging.debug('Empty Query (%d) : %s', line, query)
                                cpt.emptyQuery()
                            elif mss=='QBF':
                                cpt.err_qr()
                            else:
                                cpt.err_endpoint()
                            return (False, None, None)
                    cpt.ok()
                    return (True, n_query, bgp)
                else:
                    return (False, None, None)
            except Exception as e:
                logging.debug('PB parseQuery (%d) : %s\n%s', line, e, query)
                cpt.err_qr()
                return (False, None, None)
        else:
            logging.debug('Union (%d) : %s', line, query)
            cpt.union()
            return (False, None, None)
    else:
        cpt.autre()
        return (False, None, None)


#==================================================


def makeLogPattern():
    parts = [
        r'(?P<host>\S+)',  # host %h
        r'\S+',  # indent %l (unused)
        r'(?P<user>\S+)',  # user %u
        r'\[(?P<time>.+)\]',  # time %t
        r'"(?P<request>.+)"',  # request "%r"
        r'(?P<status>[0-9]+)',  # status %>s
        r'(?P<size>\S+)',  # size %b (careful, can be '-')
        r'"(?P<referer>.*)"',  # referer "%{Referer}i"
        r'"(?P<code>.*)"',
        r'"(?P<agent>.*)"',  # user agent "%{User-agent}i"
    ]
    pattern = re.compile(r'\s+'.join(parts) + r'\s*\Z')
    return pattern


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
    dtd = etree.DTD('./resources/log.dtd')

    #---
    assert dtd.validate(tree), '%s non valide au chargement : %s' % (
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

#==================================================


def buildXMLBGP(nquery, param_list, bgp, host, date, line):
    try:
        entry_node = etree.Element('entry')
        # entry_node.set('ip', 'ip-%s' % host)
        entry_node.set('datetime', '%s' % date)
        entry_node.set('logline', '%d' % line)
        request_node = etree.SubElement(entry_node, 'request')
        request_node.text = nquery  # '<![CDATA[%s]]>' % nquery

        for (param, val) in param_list:
            param_node = etree.SubElement(entry_node, 'param')
            param_node.set('name', '%s' % param)
            param_node.set('val', '%s' % val)

        if not(bgp is None):
            try:
                bgp_node = serializeBGP(bgp)
                entry_node.insert(1, bgp_node)
            except Exception as e:
                logging.error('(%s) PB serialize BGP : %s\n%s\n%s',
                              host, e.__str__(), nquery, bgp)

        return entry_node
    except ValueError as e:
        return None


#==================================================


def existFile(f):
    return os.path.isfile(f)


def saveEntry(file, s, host, test=existFile):
    #---
    assert s is not None
    #---
    try:
        xml_entry = etree.tostring(s,encoding="UTF-8",pretty_print=True)
        if test(file):  # not(os.path.isfile(file)):
            logging.debug('MàJ de "%s"', file)
            # tree = etree.parse(file)
            # log_node = tree.getroot()
            # log_node.append(s)
            # tosave = etree.tostring(
            #     tree,
            #     encoding="UTF-8",
            #     xml_declaration=True,
            #     pretty_print=True,
            #     doctype='<!DOCTYPE log SYSTEM "log.dtd">')
            f_out = open(file, 'a')
        else:
            logging.debug('Création de "%s"', file)
            # log_node = etree.Element('log')
            # log_node.set('ip', host)
            # log_node.append(s)
            # tosave = etree.tostring(
            #     log_node,
            #     encoding="UTF-8",
            #     xml_declaration=True,
            #     pretty_print=True,
            #     doctype='<!DOCTYPE log SYSTEM "log.dtd">')
            xml_str = '<?xml version="1.0" encoding="utf-8"?>\n'
            xml_str += '<!DOCTYPE log SYSTEM "log.dtd">\n'
            xml_str += '<log ip="%s" date="%s">\n' % (host,dt.datetime.now())
            f_out = open(file, 'w')
            f_out.write(xml_str)
        f_out.write(xml_entry.decode('utf-8'))
    except Exception as e:
        logging.error('PB Save Entry : %s', e.__str__())
    finally:
        f_out.close()

def closeLog(file, test=existFile):
    if test(file):
        try:
            logging.debug('Close "%s"', file)
            f_out = open(file, 'a')
            f_out.write('</log>')
        finally:
            f_out.close()

#==================================================
#==================================================

class Context:
    def __init__(self,description):
        self.setArgs(description)
        self.args = self.parser.parse_args()
        self.startDate = dt.datetime.now().__str__().replace(' ', 'T').replace(':', '-')[0:19]
        self.manageLogging(self.args.logLevel, 'be4dbp-'+self.startDate+'.log')

        self.refDate = self.manageDT(self.args.refdate)

        self.baseDir = self.manageDirectories(self.args.baseDir)
        self.resourcesDir = './resources'
        self.resourceSet = {'log.dtd', 'bgp.dtd', 'ranking.dtd'}

        self.loadPrefixes()

        if self.args.doR:
            self.doRanking = True
            logging.info('Ranking activated')
        else:
            self.doRanking = False

        if self.args.doTPFC:
            logging.info('TPFC constraints activated')
            self.doTPFC = True
        else:
            self.doTPFC = False

        if self.args.doEmpty != 'None':
            self.emptyTest = self.args.doEmpty
            if self.emptyTest == 'SPARQL':
                if self.args.ep == '':
                    self.endpoint = SPARQLEP(cacheDir = self.resourcesDir)
                else:
                    self.endpoint = SPARQLEP(self.args.ep, cacheDir = self.resourcesDir)
            else:
                if self.args.ep == '':
                    self.endpoint = TPFEP(cacheDir = self.resourcesDir)
                else:
                    self.endpoint = TPFEP(service = self.args.ep, cacheDir = self.resourcesDir)
            logging.info('Empty responses tests with %s' % self.endpoint)
            self.endpoint.caching(True)
        else:
            self.emptyTest = None

        self.file_name = self.args.file
        if existFile(self.file_name):
            logging.info('Open "%s"' % self.file_name)
            self.f_in = open(self.file_name, 'r')
        else :
            logging.info('"%s" does\'nt exist' % self.file_name)
            print('Can\'t open file %s' % self.file_name )
            sys.exit()

        self.nb_lines = 0
        self.nb_dates = 0
        self.date_set= set()

    def save(self):
        if self.emptyTest is not None:
            self.endpoint.saveCache()

    def close(self):
        logging.info('Close "%s"' % self.file_name)
        self.f_in.close()
        print('Nb line(s) : ', self.lines())
        print('Nb date(s) : ', self.nbDates())
        if self.emptyTest is not None:
            self.endpoint.saveCache()
        logging.info('End')

    def setArgs(self,exp):
        # https://docs.python.org/3/library/argparse.html
        # https://docs.python.org/3/howto/argparse.html
        self.parser = argparse.ArgumentParser(description=exp)
        self.parser.add_argument("-l", "--log", dest="logLevel",
                            choices=['DEBUG','INFO','WARNING','ERROR','CRITICAL'],
                            help="Set the logging level (INFO by default)", default='INFO')

        #self.parser.add_argument("-f", "--file", dest="file", help="Set the file to study")
        self.parser.add_argument("file", help="Set the file to study")

        self.parser.add_argument("-t","--datetime",dest="refdate",help="Set the date-time to study in the log",default='')
        self.parser.add_argument("-d", "--dir", dest="baseDir",
                            help="Set the directory for results ('./logs' by default)", default='./logs')
        self.parser.add_argument("-r","--ranking", help="do ranking after extraction",
                        action="store_true",dest="doR")
        self.parser.add_argument("--tpfc", help="filter some query the TPF Client does'nt treat",
                        action="store_true",dest="doTPFC")
        # self.parser.add_argument("-e","--empty", help="Request a SPARQL or a TPF endpoint to verify the query and test it returns at least one triple",
        #                 action="store_true",dest="doEmpty")
        self.parser.add_argument("-e","--empty", help="Request a SPARQL or a TPF endpoint to verify the query and test it returns at least one triple",
                        choices=['SPARQL','TPF', 'None'],dest="doEmpty",default='None')
        self.parser.add_argument("-ep","--endpoint", help="The endpoint requested for the '-e' ('--empty') option ( for exemple 'http://dbpedia.org/sparql' for SPARQL)",
                        dest="ep", default='')

    def loadPrefixes(self):
        logging.info('Reading of default prefixes')
        self.default_prefixes = dict()
        with open(self.resourcesDir+'/PrefixDBPedia.txt', 'r') as f:
            reader = csv.DictReader(f, fieldnames=['prefix', 'uri'], delimiter='\t')
            try:
                for row in reader:
                    self.default_prefixes[row['prefix']] = row['uri']
            except csv.Error as e:
                sys.exit('file %s, line %d: %s' % (f, reader.line_num, e))

    def newDir(self, date):
        rep = self.baseDir + date.replace('-', '').replace(':', '').replace('+', '-')
        if not (os.path.isdir(rep)):
            logging.info('Creation of "%s"', rep)
            os.makedirs(rep)
            for x in self.resourceSet:
                shutil.copyfile(self.resourcesDir+'/'+x, rep + '/'+x)
        rep = rep + '/'
        return rep

    def manageLogging(self,logLevel, logfile = 'be4dbp.log'):
        if logLevel:
            # https://docs.python.org/3/library/logging.html?highlight=log#module-logging
            # https://docs.python.org/3/howto/logging.html#logging-basic-tutorial
            logging.basicConfig(
                format='%(levelname)s:%(asctime)s:%(message)s',
                filename=logfile,filemode='w',
                level=getattr(logging,logLevel))

    def manageDT(self,refDate):
        if refDate != '':
            logging.info('Extracting "%s"', refDate)
        else:
            logging.info('Extracting all the file')
        return refDate


    def manageDirectories(self,d):
        logging.info('Results in "%s"', d)
        if os.path.isdir(d):
            dirList = os.listdir(d)
            for f in dirList:
                if os.path.isdir(d + '/' + f):
                    shutil.rmtree(d + '/' + f)
        else:
            os.makedirs(d)
        return d + '/'

    def newLine(self):
        self.nb_lines += 1

    def lines(self):
        return self.nb_lines

    def newDate(self,date):
        self.nb_dates += 1
        self.date_set.add(date)

    def nbDates(self):
        return self.nb_dates

    def dates(self):
        return self.date_set

    def file(self):
        return self.f_in

#==================================================
#==================================================

class Counter:
    def __init__(self, date=''):
        self.setDate(date)
        self.cpt = {
            'line': 0,
            'err_qr': 0,
            'err_ns': 0,
            'ok': 0,
            'emptyQuery':0,
            'select': 0,
            'autre': 0,
            'union': 0,
            'bgp_not_valid': 0,
            'err_tpf': 0,
            'err_endpoint':0
        }

    def setDate(self, date):
        self.date = date

    def line(self):
        self.cpt['line'] += 1

    def getLine(self):
        return self.cpt['line']

    def err_qr(self):
        self.cpt['err_qr'] += 1

    def err_endpoint(self):
        self.cpt['err_endpoint'] += 1

    def err_ns(self):
        self.cpt['err_ns'] += 1

    def emptyQuery(self):
        self.cpt['emptyQuery'] += 1

    def ok(self):
        self.cpt['ok'] += 1

    def select(self):
        self.cpt['select'] += 1

    def autre(self):
        self.cpt['autre'] += 1

    def union(self):
        self.cpt['union'] += 1

    def bgp_not_valid(self):
        self.cpt['bgp_not_valid'] += 1

    def err_tpf(self):
        self.cpt['err_tpf'] += 1

    def join(self, c):
        for x in c.cpt:
            self.cpt[x] += c.cpt[x]

    def print(self):
        if (self.date != ''):
            print('=========== ', self.date, '=============')
        # else:
        #   print('=========== ','xxxxxxxx','=============')
        pprint(self.cpt)

#==================================================
#==================================================

class Timezone(dt.tzinfo):
    def __init__(self, name="+0000"):
        self.name = name
        seconds = int(name[:-2]) * 3600 + int(name[-2:]) * 60
        self.offset = dt.timedelta(seconds=seconds)

    def utcoffset(self, dt):
        return self.offset

    def dst(self, dt):
        return timedelta(0)

    def tzname(self, dt):
        return self.name


#==================================================
#==================================================
#==================================================

if __name__ == '__main__':
    logging.basicConfig(
        format='%(levelname)s:%(asctime)s:%(message)s',
        filename='scan.log',
        filemode='w',
        level=logging.DEBUG)
    print('main')
