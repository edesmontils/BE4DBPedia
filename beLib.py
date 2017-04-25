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
from urllib.parse import urlparse, parse_qsl

from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.algebra import translateQuery

from bgp import *
from tools import *
from QueryManager import *
from Endpoint import *
from Stat import *
from operator import itemgetter
#from lxml import etree  # http://lxml.de/index.html#documentation

#==================================================
STD_BE4DBP_REFTABLE = ['line','ok','emptyQuery','union','bgp_not_valid','err_qr','err_ns','err_tpf','err_endpoint','timeout']

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
    date = date2str(dt.datetime(*tt)) #.__str__().replace(' ', 'T')

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

def validate(cpt, line, ip, query, ctx):
    if ctx.QM.queryType(query) in [SELECT]:
        if ctx.QM.containsUnion(query):
            logging.debug('Union (%d) : %s', line, query)
            cpt.inc('union') #union()
            return (False, None, None, None)
        else:
            try:
                (bgp, n_query) = ctx.QM.extractBGP(query)
                quality = dict()
                if ctx.doTPFC and not(ctx.emptyTest == MODE_TE_TPF): 
                    if not(ctx.QM.isTPFCompatible(n_query)):
                        logging.debug('PB TPF Client (%d) : %s', line, n_query)
                        cpt.inc('err_tpf')#err_tpf()
                        return (False, None, None, None)
                if ctx.emptyTest is not None :
                    (done, mss) = existDBPEDIA(line,n_query,ctx)
                    if not(done):
                        if mss=='Empty':
                            logging.debug('Empty Query (%d) : %s', line, query)
                            cpt.inc('emptyQuery')#emptyQuery()
                            quality['valid'] = 'Empty'+ctx.emptyTest
                        elif mss=='QBF':
                            if ctx.emptyTest == MODE_TE_TPF:
                                cpt.inc('err_tpf')
                            else:
                                cpt.inc('err_qr')#err_qr()
                            quality['valid'] = 'QBF'+ctx.emptyTest
                        elif mss=='TO':
                            cpt.inc('timeout')
                            quality['valid'] = 'TO'+ctx.emptyTest
                        else:
                            cpt.inc('err_endpoint')#err_endpoint()
                            #quality['valid'] = 'QBF'+ctx.emptyTest
                    else:
                        quality['valid'] = ctx.emptyTest
                cpt.inc('ok')#.ok()
                return (True, n_query, bgp, quality)
            except BGPUnvalidException as e:
                cpt.inc('bgp_not_valid')#.bgp_not_valid()
                return (False, None, None, None)
            except BGPException as e:
                logging.debug('PB URI in BGP (%d) : %s\n%s', line, e, query)
                cpt.inc('err_qr')#.err_qr()
                return (False, None, None, None)
            except SPARQLException as e:
                logging.debug('PB SPARQLError (%d) : %s\n%s', line, e, query)
                cpt.inc('err_qr')#.err_qr()
                return (False, None, None, None)
            except NSException as e:
                logging.debug('PB NS (%d) : %s\n%s', line, e, query)
                cpt.inc('err_ns')#.err_ns()
                return (False, None, None, None) 
            except TranslateQueryException as e:
                logging.debug('PB translate (%d) : %s\n%s', line, e, query)
                cpt.inc('err_qr')#.err_qr()
                return (False, None, None, None)                
            except ParseQueryException as e:
                logging.debug('PB parseQuery (%d) : %s\n%s', line, e, query)
                cpt.inc('err_qr')#.err_qr()
                return (False, None, None, None)
    else:
        #cpt.inc('autre')
        return (False, None, None, None)

#==================================================
reTimeout = re.compile(r'TimeoutExpired')
def existDBPEDIA(line,query,ctx):
    """
    test if the query has at least one response
    """
    try:
        qr = ctx.QM.simplifyQuery(query)
        hq = ctx.endpoint.hash(qr)
        if hq in ctx.cacheTO:
            return (False, 'TO')
        else:
            (ok, wellFormed) = ctx.endpoint.notEmpty(qr)
            if ok:
                return (ok, 'NotEmpty')
            elif wellFormed:
                return (ok, 'Empty')
            else:
                return (False,'QBF')
    except Exception as e:
        message = e.__str__()
        #print('Erreur existDBPEDIA:',line, message, query)
        if message.startswith('QueryBadFormed'):
            #logging.warning('PB Endpoint (QueryBadFormed):%s',e)
            return (False, 'QBF')
        elif reTimeout.search(e.__str__()):
            ctx.cacheTO.add(hq)
            return (False, 'TO')
        else:
            #logging.warning('PB Endpoint (autre):%s',e)
            return (False, 'autre')

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


def buildXMLBGP(nquery, param_list, bgp, host, date, line, qlt):
    try:
        entry_node = etree.Element('entry')
        # entry_node.set('ip', 'ip-%s' % host)
        entry_node.set('datetime', '%s' % date)
        entry_node.set('logline', '%d' % line)
        request_node = etree.SubElement(entry_node, 'request')
        request_node.text = nquery  # '<![CDATA[%s]]>' % nquery
        if 'valid' in qlt:
            entry_node.set('valid',qlt['valid'])
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

def saveEntry(file, s, host, test=existFile):
    #---
    assert s is not None
    #---
    try:
        xml_entry = etree.tostring(s,encoding="UTF-8",pretty_print=True)
        if test(file): 
            logging.debug('MàJ de "%s"', file)
            f_out = open(file, 'a')
        else:
            logging.debug('Création de "%s"', file)
            xml_str = '<?xml version="1.0" encoding="utf-8"?>\n'
            xml_str += '<!DOCTYPE log SYSTEM "log.dtd">\n'
            xml_str += '<log ip="%s" date="%s">\n' % (host,now())
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
#==================================================
#==================================================

if __name__ == '__main__':
    print('main')
