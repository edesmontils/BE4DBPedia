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

from beLib import *
from Counter import *

#==================================================


def count_stat(in_queue):
    logging.debug('Start stat worker "%s"', os.getpid())
    nb = 0
    counter_list = dict()
    total = Counter()
    while True:
        try:
            mess = in_queue.get()
            if mess is None:
                break
            else:
                nb += 1
                (date, c) = mess
                #print('recieve ',date,c)
                if not (date in counter_list):
                    counter_list[date] = Counter(date)
                counter_list[date].cpt[c] += 1
        except Empty as e:
            print('empty!')
        except Exception as e:
            print(e)
            break
    for d in counter_list:
        total.join(counter_list[d])
        counter_list[d].print()
    print('=========== total (%d date(s)) =============' % len(counter_list))
    total.print()
    logging.debug('Stop stat worker "%s" with %d', os.getpid(), nb)


class Stat:
    def __init__(self):
        self.stat_queue = mp.Queue()
        self.stat_proc = mp.Process(
            target=count_stat, args=(self.stat_queue, ))
        self.stat_proc.start()

    def put(self, v):
        self.stat_queue.put(v)

    def stop(self):
        self.stat_queue.put(None)
        self.stat_proc.join()


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
    #print(len(os.sched_getaffinity(0)))