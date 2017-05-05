#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
"""
Application to manage a set of processus
"""
#    Copyright (C) 2017 by
#    Emmanuel Desmontils <emmanuel.desmontils@univ-nantes.fr>
#    Patricia Serrano-Alvarado <patricia.serrano-alvarado@univ-nantes.fr>
#    All rights reserved.
#    GPL v 2.0 license.

import multiprocessing as mp
from queue import Empty

from tools.tools import *
from tools.Stat import *

def compute(in_queue, stat, func, *args):
    while True:
        try:
            mess = in_queue.get()
            if mess is None: break
            else: func(mess, stat, *args)
        except Empty as e:
            print('empty!')
        except Exception as e:
            print(mess, e)
            break

class ProcessSet:
	def __init__(self, nb_processes, stat, func, *args):
		self.compute_queue = mp.Queue(nb_processes)
		self.process_list = [
		    mp.Process(target=compute, args=(self.compute_queue, stat, func, *args))
		    for _ in range(nb_processes)
		]
		for process in self.process_list:
		    process.start()

	def put(self,v):
		self.compute_queue.put(v)

	def stop(self):
		for process in self.process_list:
			self.compute_queue.put(None)
		for process in self.process_list:
			process.join()


if __name__ == "__main__":
	print("main ProcessSet")
	def f(mess, stat, i, j):
		print("mess ",None,i,j)
		print(mess, 'treated')
		print('by')

	ps = ProcessSet(3,None, f, 2, 3)
	for k in range(1,10): ps.put(k)
	ps.stop()

  






