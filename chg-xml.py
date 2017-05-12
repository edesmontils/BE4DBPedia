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
import re
import argparse

from tools.tools import *
from tools.ProcessSet import *
from tools.Stat import *

from lxml import etree  # http://lxml.de/index.html#documentation


#==================================================

reLiteral1 = re.compile(r'\A"(?P<str>.*)"\^\^\<?(?P<iri>http.*)\>?\s*\Z')
reLiteral2 = re.compile(r'\A"(?P<str>.*)"\@(?P<lang>\w+)\s*\Z')
reLiteral3 = re.compile(r'\A"(?P<str>.*)"\s*\Z')

#==================================================

def analyseLiteral(i):
    i.set("type","lit")
    val = i.get("val")
    i.attrib.pop("val", None)
    m = reLiteral1.search(val)
    if m:
        lit = m.group('str')
        iri = m.group('iri')
        i.text = re.sub('"','\'',lit)
        i.set("datatype",iri)
    else:
        m = reLiteral2.search(val)
        if m:
            lit = m.group('str')
            lang = m.group('lang')
            i.text = re.sub('"','\'',lit)
            i.set("language",lang)
        else: 
            m = reLiteral3.search(val)
            if m:
                lit = m.group('str')
                i.text = re.sub('"','\'',lit)
            else: 
                print('pb:',val)
                i.text = val

def analysis(idp, file):
    print('testAnalysis for %s' % file)
    file_tested = file[:-4]+'-verified.xml'
    if existFile(file_tested):
        print('%s already tested' % file_tested)
        parser = etree.XMLParser(recover=True, strip_cdata=True)
        tree = etree.parse(file_tested, parser)
    else:
        parser = etree.XMLParser(recover=True, strip_cdata=True)
        tree = etree.parse(file, parser)

    root_node = tree.getroot()
    nbe = 0
    for entry in root_node:
        nbe += 1
        if (entry.find('request').text == 'SELECT * WHERE{ ?s1 ?p1 ?o1 .}'):
            root_node.remove(entry)
        elif (entry.find('request').text == 'SELECT * WHERE{}'):
            root_node.remove(entry)
        else:
            bgp_node = entry.find('bgp')
            for tp in bgp_node:
                for i in tp:
                    type = i.get("type")
                    if type == "literal":
                        analyseLiteral(i)
                    elif type=='iri':
                        iri = i.get("val")
                        if not(isValidURI(iri)):
                            print('Pb iri avec ',iri)
                            analyseLiteral(i)

    dtd = etree.DTD('./resources/log.dtd')
    #---
    assert dtd.validate(tree), '%s non valide au chargement : %s' % (
        file, dtd.error_log.filter_from_errors()[0])
    #---

    try:
        print('Ecriture de "%s"' % file_tested)
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
            print(
                'PB Test Analysis saving %s : %s',
                file,
                e.__str__())
        finally:
            f.close()
    except etree.DocumentInvalid as e:
        print('PB Test Analysis, %s not validated : %s' % (file, e))

#==================================================

parser = argparse.ArgumentParser(description='Etude des requêtes')
parser.add_argument('files', metavar='file', nargs='+',
                    help='files to analyse')
parser.add_argument("-p", "--proc", type=int, default=mp.cpu_count(), dest="nb_processes",
                    help="Number of processes used (%d by default)" % mp.cpu_count())
args = parser.parse_args()
file_set = args.files
current_dir = os.getcwd()
nb_processes = args.nb_processes
print('Lancement des %d processus d\'analyse' % nb_processes)
ps = ProcessSet(nb_processes, analysis)
ps.start()

for file in file_set:
    if existFile(file):
        print('Analyse de "%s"' % file)
        ps.put(file)

print('Arrêt des processus d' 'analyse')
ps.stop()

print('Fin')
