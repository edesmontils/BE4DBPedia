#!/usr/bin/env python3.6
# coding: utf8
"""
Generic set of tools
"""
#    Copyright (C) 2017 by
#    Emmanuel Desmontils <emmanuel.desmontils@univ-nantes.fr>
#    Patricia Serrano-Alvarado <patricia.serrano-alvarado@univ-nantes.fr>
#    All rights reserved.
#    GPL v 2.0 license.

import datetime as dt
import os.path

#==================================================
def existFile(f):
    return os.path.isfile(f)

#==================================================
def now():
	return dt.datetime.now()

#==================================================
def date2str(date):
	return date.__str__().replace(' ', 'T')[0:19]

#==================================================
def date2filename(date):
	return date.__str__().replace(' ', 'T').replace(':', '-').replace('+', '-')

#==================================================

def pprint_dict(d):
    length = max([len(str(i)) for i in d.keys()])
    for t in iter(d.keys()):
        print('\t',str(t).ljust(length),'=',d[t])

#==================================================
#==================================================
#==================================================

if __name__ == '__main__':
    print('main',now())
