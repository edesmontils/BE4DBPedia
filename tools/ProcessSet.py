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
		self.compute_queue = mp.Queue(2*nb_processes)
		self.isStarted = False
		self.stat = None
		self.process_list = []
		self.setStat(None)

	def setStat(self,stat):
		if not(self.isStarted) and stat is not None:
			self.stat = stat
			self.process_list = [
			    mp.Process(target=ProcessSet.compute1, args=(i, self.compute_queue, stat, self.func, *self.args))
			    for i in range(self.nb_processes)
			]
		elif stat is not None: 
			raise Exception("(setStat function) Processes are started !")
		else: 
			self.stat = None
			self.process_list = [
			    mp.Process(target=ProcessSet.compute2, args=(i, self.compute_queue, self.func, *self.args))
			    for i in range(self.nb_processes)
			]

	def start(self):
		if not(self.isStarted): 
			for process in self.process_list:
			    process.start()
			self.isStarted = True
			#print(mp.active_children())
		else: raise Exception("(start function) Processes are started !")

	def compute1(idp, in_queue, stat, func, *args):
	    while True:
	        try:
	            mess = in_queue.get()
	            if mess is None: break
	            else: func(idp, mess, stat, *args)
	        except Empty as e:
	            print('empty!')
	        except Exception as e:
	            print(mess, e)
	            break

	def compute2(idp, in_queue, func, *args):
	    while True:
	        try:
	            mess = in_queue.get()
	            if mess is None: break
	            else: func(idp, mess, *args)
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

	def isStoped(self):
		return not(self.isStarted)

class ProcessSetBack(ProcessSet):
	def __init__(self, nb_processes, func, *args):
		self.back_queue = mp.Queue()
		ProcessSet.__init__(self, nb_processes, func, *args)

	def setStat(self,stat):
		if not(self.isStarted) and stat is not None:
			self.stat = stat
			self.process_list = [
			    mp.Process(target=ProcessSetBack.compute3, args=(i, self.compute_queue, self.back_queue, stat, self.func, *self.args))
			    for i in range(self.nb_processes)
			]
		elif stat is not None: 
			raise Exception("(setStat function) Processes are started !")
		else: 
			self.stat = None
			self.process_list = [
			    mp.Process(target=ProcessSetBack.compute4, args=(i, self.compute_queue, self.back_queue, self.func, *self.args))
			    for i in range(self.nb_processes)
			]

	def compute3(idp, in_queue, out_queue, stat, func, *args):
	    while True:
	        try:
	            mess = in_queue.get()
	            if mess is None: break
	            else: 
	            	v = func(idp, mess, stat, *args)
	            	out_queue.put(v)
	        except Empty as e:
	            print('empty!')
	        except Exception as e:
	            print(mess, e)
	            break
	    out_queue.put(None)

	def compute4(idp, in_queue, out_queue, func, *args):
	    while True:
	        try:
	            mess = in_queue.get()
	            if mess is None: break
	            else: 
	            	v = func(idp, mess, *args)
	            	out_queue.put(v)
	        except Empty as e:
	            print('empty!')
	        except Exception as e:
	            print(mess, e)
	            break
	    out_queue.put(None)

	def get(self):
		return self.back_queue.get_nowait()

if __name__ == "__main__":
	print("main ProcessSet")


