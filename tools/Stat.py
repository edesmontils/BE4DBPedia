#!/usr/bin/env python3.6
# coding: utf8
"""
Class to process statistics in a parallel context
"""
#    Copyright (C) 2017 by
#    Emmanuel Desmontils <emmanuel.desmontils@univ-nantes.fr>
#    Patricia Serrano-Alvarado <patricia.serrano-alvarado@univ-nantes.fr>
#    All rights reserved.
#    GPL v 2.0 license.

import multiprocessing as mp
from queue import Empty
import time
from tools.Counter import *
import csv
#==================================================
#==================================================
#==================================================

def abs_count_stat(in_queue, out_queue, AbsCounterClass,refTable):
    counter_list = dict()
    while True:
        try:
            mess = in_queue.get()
            if mess is None:
                break
            elif len(mess) == 1:
                (grp,) = mess
                if grp in counter_list:
                    out_queue.put( (grp, counter_list[grp]) )
                else:
                    out_queue.put(None)
            elif len(mess) == 2:
                (grp, c) = mess
                if not (grp in counter_list):
                    counter_list[grp] = AbsCounterClass.build(refTable)
                counter_list[grp].inc(c)
            else:
                (grp, c, qte) = mess
                if not (grp in counter_list):
                    counter_list[grp] = AbsCounterClass.build(refTable)
                counter_list[grp].add(c,qte)
        except Empty as e:
            pass #print('empty!')
        except Exception as e:
            print(e)
            break
    for d in counter_list:
        out_queue.put( (d, counter_list[d]) )
    out_queue.put(None)

#==================================================

class Stat:
    def __init__(self, AbsCounterClass, refTable):
        self.refTable = refTable
        self.total = AbsCounterClass.build(refTable)
        self.counters = dict()
        self.groups = set()
        self.stat_queue = mp.Queue()
        self.res_queue = mp.Queue()
        self.stat_proc = mp.Process(target=abs_count_stat, args=(self.stat_queue, self.res_queue, AbsCounterClass,refTable))
        self.stat_proc.start()
        self.stopped = False
        self.backuped = False
        self.sem = mp.Semaphore()

    def put(self, grp, v):
        self.groups.add(grp)
        self.stat_queue.put( (grp,v) )

    def stdput(self, v):
        self.put('',v)

    def mput(self, grp, v, qte):
        self.groups.add(grp)
        self.stat_queue.put( (grp,v,qte) )

    def stdmput(self, v, qte):
        self.mput('',v,qte)

    def get(self,grp = ''):
        self.stat_queue.put( (grp,) )
        r = self.res_queue.get()
        if r is not None:
            (d,c) = r
            return c
        else: return None

    def backup(self, file = ''):
        with self.sem :
            for d in self.groups:
                self.stat_queue.put( (d,) )
            for d in self.groups:
                r = self.res_queue.get()
                if r is not None:
                    (d,c) = r
                    self.counters[d] = c
            self.backuped = True
        if file != '':
            self.saveCSV(file)

    def stop(self, stdout = False):
        with self.sem :
            self.stat_queue.put(None)
            nb = 0
            r = self.res_queue.get()
            while r is not None:
                (d, c) = r
                nb +=1
                self.total.join(c)
                self.counters[d] = c
                r = self.res_queue.get()
        self.stat_proc.join()
        self.stopped = True
        if stdout and nb>0: 
            self.print()

    def saveCSV(self, file, sep='\t'):
        if self.stopped or self.backuped:
            saveCounterDict2CSV(file, self.counters, self.refTable, sep)
        else:
            print('No Data To Save')

    def print(self):
        if self.stopped or self.backuped:
            nb = len(self.counters.keys())
            for d in self.counters:
                counter = self.counters[d]
                if d != '' : print('----------- %s -------------' % d)
                counter.print()
            if nb > 1 :
                print('=========== total (%d groups) =============' % nb)
                self.total.print()
            elif nb == 0:
                print('Nothing to print')
            else:
                pass
        else:
            print('No Data To Print')

#==================================================
#==================================================
#==================================================

if __name__ == '__main__':
    print('main')
    print(mp.cpu_count(),' proccesses availables')

    stat = Stat(Counter, ['ok','union'])

    date = date2str(now())

    stat.put( '2017-04-22','ok') 
    # time.sleep(2)
    counter = stat.get('2017-04-22')
    counter.print()
    stat.backup()
    stat.put( '2017-04-23','union') 
    stat.put( '2017-04-23','union')
    stat.put( '2017-04-23','ok')
    print('end')
    stat.stop(True)
    stat.saveCSV('test.csv')

