#!/usr/bin/env python3.6
# coding: utf8
"""
Class to process statrictics in a parallel context
"""
#    Copyright (C) 2017 by
#    Emmanuel Desmontils <emmanuel.desmontils@univ-nantes.fr>
#    Patricia Serrano-Alvarado <patricia.serrano-alvarado@univ-nantes.fr>
#    All rights reserved.
#    GPL v 2.0 license.

from pprint import pprint

import multiprocessing as mp
from queue import Empty
import logging
import time
from beLib import *
from Counter import *

#==================================================
#==================================================
#==================================================

def abs_count_stat(in_queue, out_queue, AbsCounterClass,refTable):
    logging.debug('Start stat worker "%s"', os.getpid())
    nb = 0
    counter_list = dict()
    while True:
        try:
            mess = in_queue.get()
            # if mess is None:print('recieve: None')
            # else: print('recieve:',mess, len(mess))
            if mess is None:
                break
            elif len(mess) == 1:
                (date,) = mess
                if date in counter_list:
                    out_queue.put( (date, counter_list[date]) )
                else:
                    out_queue.put(None)
            else:
                nb += 1
                (date, c) = mess
                if not (date in counter_list):
                    counter_list[date] = AbsCounterClass.build(refTable)
                counter_list[date].cpt[c] += 1
        except Empty as e:
            print('empty!')
        except Exception as e:
            print(e)
            break

    for d in counter_list:
        out_queue.put( (d, counter_list[d]) )
    out_queue.put(None)
    logging.debug('Stop stat worker "%s" with %d messages', os.getpid(), nb)

class AbstractStat:
    def __init__(self, AbsCounterClass, refTable):
        self.total = AbsCounterClass.build(refTable)
        self.counters = dict()
        self.dates = set()
        self.stat_queue = mp.Queue()
        self.res_queue = mp.Queue()
        self.stat_proc = mp.Process(target=abs_count_stat, args=(self.stat_queue, self.res_queue, AbsCounterClass,refTable))
        self.stat_proc.start()

    def put(self, date, v):
        self.dates.add(date)
        self.stat_queue.put( (date,v) )

    def get(self,date = ''):
        self.stat_queue.put( (date,) )
        r = self.res_queue.get()
        if r is not None:
            (d,c) = r
            return c
        else: return None

    def backup(self):
        for d in self.dates:
            c = self.get(d)
            if c is not None:
                self.counters[d] = c

    def stop(self, print = False):
        self.stat_queue.put(None)
        self.stat_proc.join()
        nb = 0
        r = self.res_queue.get()
        while r is not None:
            (d, c) = r
            nb +=1
            self.total.join(c)
            self.counters[d] = c
            r = self.res_queue.get()

        if print and nb>0: 
            self.print()

    def print(self):
        nb = len(self.counters.keys())
        for d in self.counters:
            counter = self.counters[d]
            if nb>1: print('----------- %s -------------' % d)
            counter.print()
        if nb > 1 :
            print('=========== total (%d dates) =============' % nb)
            self.total.print()
        elif nb == 0:
            print('Nothing to print')
        else:
            pass

#==================================================

class Stat(AbstractStat):
    def __init__(self):
        AbstractStat.__init__(self, AbstractCounter, STD_BE4DBP_REFTABLE)

#==================================================
#==================================================
#==================================================

if __name__ == '__main__':
    print('main')
    print(mp.cpu_count(),' proccesses availables')

    stat = Stat()
    date = date2str(now())

    stat.put( date,'ok') 
    # time.sleep(2)
    counter = stat.get(date)
    counter.print()
    stat.backup()
    stat.put( date,'union') 
    print('end')
    stat.stop(True)


