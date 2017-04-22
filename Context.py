#!/usr/bin/env python3.6
# coding: utf8
"""
Tools to manage context of processing
"""
#    Copyright (C) 2017 by
#    Emmanuel Desmontils <emmanuel.desmontils@univ-nantes.fr>
#    Patricia Serrano-Alvarado <patricia.serrano-alvarado@univ-nantes.fr>
#    All rights reserved.
#    GPL v 2.0 license.

import multiprocessing as mp

import datetime as dt

import csv
import sys
import os
import shutil

import logging
import argparse

from QueryManager import *
from Endpoint import *
from tools import *

#==================================================

class Context:
    def __init__(self,description):
        self.setArgs(description)
        self.args = self.parser.parse_args()
        self.startDate = date2str(now())
        self.manageLogging(self.args.logLevel, 'be4dbp-'+date2filename(now())+'.log')

        self.refDate = self.manageDT(self.args.refdate)
        self.current_dir = os.getcwd()
        self.baseDir = self.manageDirectories(self.args.baseDir)
        self.resourcesDir = 'resources'
        self.resourceSet = {'log.dtd', 'bgp.dtd', 'ranking.dtd'}

        self.loadPrefixes()

        self.QM = QueryManager(defaultPrefixes = self.default_prefixes)

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
            if self.emptyTest == 'SPARQLEP':
                if self.args.ep == '':
                    self.endpoint = SPARQLEP(cacheDir = self.current_dir+'/'+self.resourcesDir)
                else:
                    self.endpoint = SPARQLEP(self.args.ep, cacheDir = self.current_dir+'/'+self.resourcesDir)
            else:
                if self.args.ep == '':
                    self.endpoint = TPFEP(cacheDir = self.current_dir+'/'+self.resourcesDir)
                else:
                    self.endpoint = TPFEP(service = self.args.ep, cacheDir = self.current_dir+'/'+self.resourcesDir)
                self.endpoint.setEngine('/Users/desmontils-e/Programmation/TPF/Client.js-master/bin/ldf-client')
            logging.info('Empty responses tests with %s' % self.endpoint)
            self.endpoint.caching(True)
            self.endpoint.setTimeOut(20)
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
        self.QM.printStats()
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
                        choices=['SPARQLEP','TPF', 'None'],dest="doEmpty",default='None')
        self.parser.add_argument("-ep","--endpoint", help="The endpoint requested for the '-e' ('--empty') option ( for exemple 'http://dbpedia.org/sparql' for SPARQL)",
                        dest="ep", default='')

    def loadPrefixes(self):
        logging.info('Reading of default prefixes')
        self.default_prefixes = dict()
        with open(self.current_dir+'/'+self.resourcesDir+'/PrefixDBPedia.txt', 'r') as f:
            reader = csv.DictReader(f, fieldnames=['prefix', 'uri'], delimiter='\t')
            try:
                for row in reader:
                    self.default_prefixes[row['prefix']] = row['uri']
            except csv.Error as e:
                sys.exit('file %s, line %d: %s' % (f, reader.line_num, e))

    def newDir(self, date):
        rep = self.baseDir + date2filename(date) #date.replace('-', '').replace(':', '').replace('+', '-')
        if not (os.path.isdir(rep)):
            logging.info('Creation of "%s"', rep)
            os.makedirs(rep)
            for x in self.resourceSet:
                shutil.copyfile(self.current_dir+'/'+self.resourcesDir+'/'+x, rep + '/'+x)
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

class ParallelContext(Context):
    def __init__(self,description):
        Context.__init__(self,description)
        self.nb_processes = min(self.args.nb_processes,self.max_processes)
        self.mp_manager = mp.Manager()
        self.sem = self.mp_manager.Semaphore()

    def setArgs(self,exp):
        Context.setArgs(self,exp)
        self.max_processes = mp.cpu_count()
        nb_processes_default = min(4, self.max_processes / 2)
        self.parser.add_argument("-p", "--proc", type=int, default=nb_processes_default, dest="nb_processes",
                        help="Number of processes used to extract (%d by default) over %d usuable processes" % (nb_processes_default,self.max_processes))

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
    print(mp.cpu_count(),' proccesses availables')
