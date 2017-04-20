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

class AbstractCounter:
    def __init__(self, refTable):
        self.refTable = refTable
        self.cpt = OrderedDict()
        self.clear()

    def inc(self, mess):
        self.cpt[mess] += 1

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
        return AbstractCounter(refTable)

class AbstractParallelCounter(AbstractCounter):
    def __init__(self, stat, refTable, date=''):
        AbstractCounter.__init__(self,refTable)
        self.stat = stat
        self.date = date #date2str(now())

    def inc(self, mess):
        self.stat.put( self.date, mess )
        AbstractCounter.inc(self,mess)

    def build(refTable):
        return AbstractParallelCounter(refTable)

    def print(self):
        if (self.date != ''):
            print('=========== ', self.date, '=============')
        AbstractCounter.print(self)

#==================================================
#==================================================
#==================================================
STD_BE4DBP_REFTABLE = ['line','ok','emptyQuery','union','bgp_not_valid','err_qr','err_ns','err_tpf','err_endpoint']

class Counter(AbstractCounter):
    def __init__(self, date=''):
        AbstractCounter.__init__(self,STD_BE4DBP_REFTABLE)
        self.setDate(date)

    def setDate(self, date):
        self.date = date

    def line(self):
        self.inc('line')

    def getLine(self):
        return self.cpt['line']

    def err_qr(self):
       self.inc('err_qr')

    def err_endpoint(self):
        self.inc('err_endpoint')

    def err_ns(self):
        self.inc('err_ns')

    def emptyQuery(self):
        self.inc('emptyQuery')

    def ok(self):
        self.inc('ok')

    def union(self):
        self.inc('union')

    def bgp_not_valid(self):
        self.inc('bgp_not_valid')

    def err_tpf(self):
        self.inc('err_tpf')

    def print(self):
        if (self.date != ''):
            print('=========== ', self.date, '=============')
        # else:
        #   print('=========== ','xxxxxxxx','=============')
        AbstractCounter.print(self)

#==================================================

class ParallelCounter(Counter):
    def __init__(self, stat, date=''):
        Counter.__init__(self, date)
        self.stat = stat

    def line(self):
        self.stat.put(self.date, 'line')
        Counter.line(self)

    def err_qr(self):
        self.stat.put(self.date, 'err_qr')
        Counter.err_qr(self)

    def err_ns(self):
        self.stat.put(self.date, 'err_ns')
        Counter.err_ns(self)

    def ok(self):
        self.stat.put(self.date, 'ok')
        Counter.ok(self)

    def emptyQuery(self):
        self.stat.put(self.date, 'emptyQuery')
        Counter.emptyQuery(self)

    def err_endpoint(self):
        self.stat.put(self.date, 'err_endpoint')
        Counter.err_endpoint(self)

    def union(self):
        self.stat.put(self.date, 'union')
        Counter.union(self)

    def bgp_not_valid(self):
        self.stat.put(self.date, 'bgp_not_valid')
        Counter.bgp_not_valid(self)

    def err_tpf(self):
        self.stat.put(self.date, 'err_tpf')
        Counter.err_tpf(self)

#==================================================
#==================================================
#==================================================

if __name__ == '__main__':
    print('main')
    cpt = AbstractCounter.build(['ok', 'go'])
    cpt.inc('ok')
    cpt.print()


