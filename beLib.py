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

# from SPARQLWrapper import SPARQLWrapper

from bgp import *

from operator import itemgetter
# import xml.etree.ElementTree as ET
from lxml import etree  # http://lxml.de/index.html#documentation

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

    return (query, date, param_list, res['host'])


#==================================================


def loadPrefixes():
    default_prefix = dict()
    with open('./resources/PrefixDBPedia.txt', 'r') as f:
        reader = csv.DictReader(
            f, fieldnames=['prefix', 'uri'], delimiter='\t')
        try:
            for row in reader:
                default_prefix[row['prefix']] = row['uri']
            return default_prefix
        except csv.Error as e:
            sys.exit('file %s, line %d: %s' % (f, reader.line_num, e))


#==================================================
# sparql = SPARQLWrapper("http://dbpedia.org/sparql")

#==================================================


class Counter:
    def __init__(self, date=''):
        self.setDate(date)
        self.cpt = {
            'line': 0,
            'err_qr': 0,
            'err_ns': 0,
            'ok': 0,
            'select': 0,
            'autre': 0,
            'union': 0,
            'bgp_not_valid': 0,
            'err_tpf': 0
        }

    def setDate(self, date):
        self.date = date

    def line(self):
        self.cpt['line'] += 1

    def getLine(self):
        return self.cpt['line']

    def err_qr(self):
        self.cpt['err_qr'] += 1

    def err_ns(self):
        self.cpt['err_ns'] += 1

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
        #	print('=========== ','xxxxxxxx','=============')
        pprint(self.cpt)


#==================================================


def translate(cpt, line, ip, query, tree, dp):
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
            if (pr in dp):
                n_query = 'PREFIX ' + pr + ': <' + \
                    dp[pr] + '> #ADD by SCAN \n' + query
                return translate(cpt, line, ip, n_query,
                                 parseQuery(n_query), dp)
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
\Wregex\W*
\(
    ( [^,()"'<]+ | "[^"]*" | '[^']*' | <[^>]*> | \w* \( [^)]* \) )+
( , ( [^,()"'<]+ | "[^"]*" | '[^']*' | <[^>]*> | \w* \( [^)]* \) )+ ){2,}
\)
""", re.IGNORECASE | re.VERBOSE)

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


def validate(cpt, line, ip, query, dp):
	# if (re.search('(\s+)select(\s+)', query.lower()) is not None):
	if (reSelect.search(query) is not None):
	    cpt.select()
	    # if (re.search('(\s+)union(\s+)', query.lower()) is None):
	    if (reUnion.search(query) is None):
	        if isTPFCompatible(query):
	            try:
	                tree = parseQuery(query)
	                (ok, n_query, bgp) = translate(cpt, line, ip, query,
	                                               tree, dp)
	                if ok:
	                    if valid(bgp):
	                        cpt.ok()
	                        return (True, n_query, bgp)
	                    else:
	                        cpt.bgp_not_valid()
	                        return (False, None, None)
	                else:
	                    return (False, None, None)
	            except Exception as e:
	                logging.debug('PB parseQuery (%d) : %s\n%s', line, e, query)
	                cpt.err_qr()
	                return (False, None, None)
	        else:
	            logging.debug('PB TPF Client (%d) : %s', line, query)
	            cpt.err_tpf()
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


def addBGP2Rank(bgp, query, line, ranking):
    ok = False
    for (i, (d, p, n, ll)) in enumerate(ranking):
        if bgp == d:
            # if equals(bgp,d) :
            ok = True
            break
    if ok:
        ll.add(line)
        ranking[i] = (d, p, n+1, ll)
    else:
        ranking.append( (bgp, query, 1 , {line}) )


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
        addBGP2Rank(
            cbgp,
            entry.find('request').text, ide, ranking)
    ranking.sort(key=itemgetter(2, 1), reverse=True)
    node_tree_ranking = etree.Element('ranking')
    node_tree_ranking.set('ip', tree.getroot().get('ip'))
    for (i, (bgp, query, freq, lines)) in enumerate(ranking):
        s = " ".join(x for x in lines)
        f = freq / nbe
        node_r = etree.SubElement(
            node_tree_ranking,
            'entry-rank',
            attrib={
                'frequence': '{:04.3f}'.format(f),
                'nb-occurrences': '{:d}'.format(freq),
                'rank':'{:d}'.format(i+1),'lines':'{:s}'.format(s)
                }
        )
        node_b = serializeBGP(bgp)
        node_r.append(node_b)
        #node_q = etree.SubElement(node_r, 'request')
        #node_q.text = query

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
            logging.debug('Fermeture de "%s"', file)
            f_out = open(file, 'a')
            f_out.write('</log>')
        finally:
            f_out.close()


#==================================================


def manageLogging(logLevel, logfile = 'be4dbp.log'):
    if logLevel:
        # https://docs.python.org/3/library/logging.html?highlight=log#module-logging
        # https://docs.python.org/3/howto/logging.html#logging-basic-tutorial
        logging.basicConfig(
            format='%(levelname)s:%(asctime)s:%(message)s',
            filename=logfile,
            filemode='w',
            level=getattr(
                logging,
                logLevel))


def manageDT(refDate):
    if refDate != '':
        logging.info('Etude du "%s"', refDate)
    else:
        logging.info('Etude de tout le fichier')
    return refDate


def manageDirectories(d):
    logging.info('Résultat dans "%s"', d)
    if os.path.isdir(d):
        dirList = os.listdir(d)
        for f in dirList:
            if os.path.isdir(d + '/' + f):
                shutil.rmtree(d + '/' + f)
    else:
        os.makedirs(d)
    return d + '/'


def newDir(baseDir, date):
    rep = baseDir + date.replace('-', '').replace(':', '').replace('+', '-')
    if not (os.path.isdir(rep)):
        logging.debug('Création de "%s"', rep)
        os.makedirs(rep)
        shutil.copyfile('./resources/log.dtd', rep + '/log.dtd')
        shutil.copyfile('./resources/ranking.dtd', rep + '/ranking.dtd')
        shutil.copyfile('./resources/bgp.dtd', rep + '/bgp.dtd')
    rep = rep + '/'
    return rep


def setStdArgs(exp):
    # https://docs.python.org/3/library/argparse.html
    # https://docs.python.org/3/howto/argparse.html
    parser = argparse.ArgumentParser(description=exp)
    parser.add_argument("-l", "--log", dest="logLevel",
                        choices=[
                            'DEBUG',
                            'INFO',
                            'WARNING',
                            'ERROR',
                            'CRITICAL'],
                        help="Set the logging level", default='INFO')
    parser.add_argument("-f", "--file", dest="file",
                        help="Set the file to study", default='log.log')
    parser.add_argument(
        "-t",
        "--datetime",
        dest="refdate",
        help="Set the date-time to study in the log",
        default='')
    parser.add_argument("-d", "--dir", dest="baseDir",
                        help="Set the directory for results", default='./logs')
    parser.add_argument("-r","--ranking", help="do ranking after extraction",
                    action="store_true",dest="doR")
    return parser


def manageStdArgs(args):
    manageLogging(args.logLevel)
    refDate = manageDT(args.refdate)
    baseDir = manageDirectories(args.baseDir)
    doRanking = args.doR
    logging.info('Ouverture de "%s"' % args.file)
    f_in = open(args.file, 'r')
    return (refDate, baseDir, f_in, doRanking)


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
