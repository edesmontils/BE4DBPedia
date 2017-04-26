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
from beTestEPValid import testQuery
#from lxml import etree  # http://lxml.de/index.html#documentation

#==================================================
STD_BE4DBP_REFTABLE = ['line','ok','emptyQuery','union','bgp_not_valid','err_qr','err_ns','err_tpf','err_endpoint','timeout']

#==================================================

def validate(date, line, ip, query, ctx):
    if ctx.QM.queryType(query) in [SELECT]:
        if ctx.QM.containsUnion(query):
            logging.debug('Union (%d) : %s', line, query)
            ctx.stat.put(date,'union') #union()
            return (False, None, None, None)
        else:
            try:
                (bgp, n_query) = ctx.QM.extractBGP(query)
                quality = dict()
                if ctx.doTPFC and not(ctx.emptyTest == MODE_TE_TPF): 
                    if not(ctx.QM.isTPFCompatible(n_query)):
                        logging.debug('PB TPF Client (%d) : %s', line, n_query)
                        ctx.stat.put(date,'err_tpf')#err_tpf()
                        return (False, None, None, None)
                if ctx.emptyTest is not None :
                    (done, mss) = testQuery(ctx.QM.simplifyQuery(n_query),ctx.endpoint, ctx.cacheTO)
                    if not(done):
                        if mss=='Empty':
                            logging.debug('Empty Query (%d) : %s', line, query)
                            ctx.stat.put(date,'emptyQuery')#emptyQuery()
                            quality['valid'] = 'Empty'+ctx.emptyTest
                        elif mss=='QBF':
                            if ctx.emptyTest == MODE_TE_TPF:
                                ctx.stat.put(date,'err_tpf')
                            else:
                                ctx.stat.put(date,'err_qr')#err_qr()
                            quality['valid'] = 'QBF'+ctx.emptyTest
                        elif mss=='TO':
                            ctx.stat.put(date,'timeout')
                            quality['valid'] = 'TO'+ctx.emptyTest
                        else:
                            ctx.stat.put(date,'err_endpoint')#err_endpoint()
                            #quality['valid'] = 'QBF'+ctx.emptyTest
                    else:
                        quality['valid'] = ctx.emptyTest
                ctx.stat.put(date,'ok')#.ok()
                return (True, n_query, bgp, quality)
            except BGPUnvalidException as e:
                ctx.stat.put(date,'bgp_not_valid')#.bgp_not_valid()
                return (False, None, None, None)
            except BGPException as e:
                logging.debug('PB URI in BGP (%d) : %s\n%s', line, e, query)
                ctx.stat.put(date,'err_qr')#.err_qr()
                return (False, None, None, None)
            except SPARQLException as e:
                logging.debug('PB SPARQLError (%d) : %s\n%s', line, e, query)
                ctx.stat.put(date,'err_qr')#.err_qr()
                return (False, None, None, None)
            except NSException as e:
                logging.debug('PB NS (%d) : %s\n%s', line, e, query)
                ctx.stat.put(date,'err_ns')#.err_ns()
                return (False, None, None, None) 
            except TranslateQueryException as e:
                logging.debug('PB translate (%d) : %s\n%s', line, e, query)
                ctx.stat.put(date,'err_qr')#.err_qr()
                return (False, None, None, None)                
            except ParseQueryException as e:
                logging.debug('PB parseQuery (%d) : %s\n%s', line, e, query)
                ctx.stat.put(date,'err_qr')#.err_qr()
                return (False, None, None, None)
    else:
        #cpt.inc('autre')
        return (False, None, None, None)

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
