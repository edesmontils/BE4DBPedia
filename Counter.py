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
from tools import *
from collections import OrderedDict
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

class ParallelCounter(Counter):

    def build(refTable):
        return ParallelCounter(refTable)

    def __init__(self, stat, refTable, grp=''):
        Counter.__init__(self,refTable)
        self.stat = stat
        self.grp = grp 

    def inc(self, mess):
        self.stat.put( self.grp, mess )
        Counter.inc(self,mess)

    def add(self, mess, qte):
        self.stat.mput( self.grp, mess, qte)
        Counter.add(self,mess, qte)

    def print(self):
        if (self.grp != ''):
            print('=========== ', self.grp, '=============')
        Counter.print(self)

#==================================================
#==================================================
#==================================================

if __name__ == '__main__':
    print('main')
    cpt = Counter.build(['ok', 'go'])
    cpt.inc('ok')
    cpt.print()


