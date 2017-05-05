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

class ProcessSet:
	def __init__(self, nb_processes, func, *args):
		self.nb_processes = nb_processes
		self.func = func
		self.args = args
		self.compute_queue = mp.Queue(nb_processes)
		self.process_list = [
		    mp.Process(target=ProcessSet.compute2, args=(self.compute_queue, func, *args))
		    for _ in range(nb_processes)
		]
		self.isStarted = False
		self.stat = None

	def setStat(self,stat):
		if not(self.isStarted) and stat is not None:
			self.stat = stat
			self.process_list = [
			    mp.Process(target=ProcessSet.compute1, args=(self.compute_queue, stat, self.func, *self.args))
			    for _ in range(self.nb_processes)
			]
		elif stat is not None: 
			raise Exception("(setStat function) Processes are started !")
		else: 
			self.stat = None
			self.process_list = [
			    mp.Process(target=ProcessSet.compute2, args=(self.compute_queue, self.func, *self.args))
			    for _ in range(self.nb_processes)
			]

	def start(self):
		if not(self.isStarted): 
			for process in self.process_list:
			    process.start()
			self.isStarted = True
		else: raise Exception("(start function) Processes are started !")

	def compute1(in_queue, stat, func, *args):
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

	def compute2(in_queue, func, *args):
	    while True:
	        try:
	            mess = in_queue.get()
	            if mess is None: break
	            else: func(mess, *args)
	        except Empty as e:
	            print('empty!')
	        except Exception as e:
	            print(mess, e)
	            break

	def put(self,v):
		if self.isStarted: self.compute_queue.put(v)
		else: raise Exception("Processes are stoped !")

	def stop(self):
		if self.isStarted: 
			for process in self.process_list:
				self.compute_queue.put(None)
			for process in self.process_list:
				process.join()
			self.isStarted = False
		else: raise Exception("Processes are stoped !")


if __name__ == "__main__":
	print("main ProcessSet")
	def f(mess, stat, i, j):
		print("mess ",None,i,j)
		print(mess, 'treated')
		print('by')

	ps = ProcessSet(3,None, f, 2, 3)
	for k in range(1,10): ps.put(k)
	ps.stop()

  






