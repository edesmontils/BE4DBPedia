#!/usr/bin/env python3.6
# coding: utf8
"""
Tools to manage statistics on processes
"""
#    Copyright (C) 2017 by
#    Emmanuel Desmontils <emmanuel.desmontils@univ-nantes.fr>
#    Patricia Serrano-Alvarado <patricia.serrano-alvarado@univ-nantes.fr>
#    All rights reserved.
#    GPL v 2.0 license.

from pprint import pprint
from tools.tools import *
from collections import OrderedDict
import csv

#==================================================

class Counter:
    def __init__(self, refTable):
        self.refTable = refTable
        self.cpt = OrderedDict()
        self.clear()

    def get(self, mess):
        return self.cpt[mess]

    def inc(self, mess):
        self.cpt[mess] += 1

    def add(self, mess, v):
        self.cpt[mess] += v

    def join(self, c):
        for x in c.cpt:
            self.cpt[x] += c.cpt[x]

    def print(self):
        # pprint(self.cpt)
        pprint_dict(self.cpt)

    def clear(self):
        self.cpt.clear()
        for x in self.refTable:
            self.cpt[x] = 0

    def build(refTable):
        return Counter(refTable)

#==================================================

def saveCounterDict2CSV(file, counter_dict, refTable, sep='\t', keyName = 'grp'):
    with open(file,"w", encoding='utf-8') as f:
        fn=[keyName]+[w for w in refTable]
        writer = csv.DictWriter(f,fieldnames=fn,delimiter=sep)
        writer.writeheader()
        for x in iter(counter_dict.keys()):
            if x == '':
                linename = '?'
            else:
                linename = x
            s = dict({(keyName,linename)} | {v for v in counter_dict[x].cpt.items()})
            writer.writerow(s)

#==================================================
#==================================================
#==================================================

if __name__ == '__main__':
    print('main')
    cpt = Counter.build(['ok', 'go'])
    cpt.inc('ok')
    cpt.print()


