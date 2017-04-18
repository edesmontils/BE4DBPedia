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
import os

from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.algebra import translateQuery

from bgp import *
from QueryManager import *

from operator import itemgetter
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

def existDBPEDIA(line,query,ctx):
    """
    test if the query has at least one response
    """
    try:
        (ok, wellFormed) = ctx.endpoint.notEmpty(ctx.qe.simplifyQuery(query))
        if wellFormed:
            return (ok, 'empty')
        else:
            return (False,'QBF')
    except Exception as e:
        message = e.__str__()
        print('Erreur existDBPEDIA:',line, message, query)
        if message.startswith('QueryBadFormed'):
            logging.warning('PB Endpoint (QueryBadFormed):%s',e)
            return (False, 'QBF')
        else:
            logging.warning('PB Endpoint (autre):%s',e)
            return (False, 'autre')

def validate(cpt, line, ip, query, ctx):
    #if (reSelect.search(query) is not None):
    if ctx.qe.queryType(query) == SELECT:
        cpt.select()
        if not(ctx.qe.containsUnion(query)):
            try:
                tree = parseQuery(query)
                (ok, n_query, bgp) = translate(cpt, line, ip, query, tree, ctx)
                if ok:
                    if not(valid(bgp)):
                        cpt.bgp_not_valid()
                        return (False, None, None)
                    if ctx.doTPFC: 
                        if not(ctx.qe.isTPFCompatible(n_query)):
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
