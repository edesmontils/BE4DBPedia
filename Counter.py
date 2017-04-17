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

#==================================================

class Counter:
    def __init__(self, date=''):
        self.setDate(date)
        self.cpt = {
            'line': 0,
            'err_qr': 0,
            'err_ns': 0,
            'ok': 0,
            'emptyQuery':0,
            'select': 0,
            'autre': 0,
            'union': 0,
            'bgp_not_valid': 0,
            'err_tpf': 0,
            'err_endpoint':0
        }

    def setDate(self, date):
        self.date = date

    def line(self):
        self.cpt['line'] += 1

    def getLine(self):
        return self.cpt['line']

    def err_qr(self):
        self.cpt['err_qr'] += 1

    def err_endpoint(self):
        self.cpt['err_endpoint'] += 1

    def err_ns(self):
        self.cpt['err_ns'] += 1

    def emptyQuery(self):
        self.cpt['emptyQuery'] += 1

    def ok(self):
        self.cpt['ok'] += 1

    def select(self):
        self.cpt['select'] += 1

    def autre(self):
        self.cpt['autre'] += 1

    def union(self):
        self.cpt['union'] += 1

    def bgp_not_valid(self):
        self.cpt['bgp_not_valid'] += 1

    def err_tpf(self):
        self.cpt['err_tpf'] += 1

    def join(self, c):
        for x in c.cpt:
            self.cpt[x] += c.cpt[x]

    def print(self):
        if (self.date != ''):
            print('=========== ', self.date, '=============')
        # else:
        #   print('=========== ','xxxxxxxx','=============')
        pprint(self.cpt)

#==================================================

class ParallelCounter(Counter):
    def __init__(self, stat, date=''):
        Counter.__init__(self, date)
        self.stat = stat

    def line(self):
        self.stat.put((self.date, 'line'))
        Counter.line(self)

    def err_qr(self):
        self.stat.put((self.date, 'err_qr'))
        Counter.err_qr(self)

    def err_ns(self):
        self.stat.put((self.date, 'err_ns'))
        Counter.err_ns(self)

    def ok(self):
        self.stat.put((self.date, 'ok'))
        Counter.ok(self)

    def emptyQuery(self):
        self.stat.put((self.date, 'emptyQuery'))
        Counter.emptyQuery(self)

    def err_endpoint(self):
        self.stat.put((self.date, 'err_endpoint'))
        Counter.err_endpoint(self)

    def select(self):
        self.stat.put((self.date, 'select'))
        Counter.select(self)

    def autre(self):
        self.stat.put((self.date, 'autre'))
        Counter.autre(self)

    def union(self):
        self.stat.put((self.date, 'union'))
        Counter.union(self)

    def bgp_not_valid(self):
        self.stat.put((self.date, 'bgp_not_valid'))
        Counter.bgp_not_valid(self)

    def err_tpf(self):
        self.stat.put((self.date, 'err_tpf'))
        Counter.err_tpf(self)

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