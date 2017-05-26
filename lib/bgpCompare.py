#!/usr/bin/env python3.6
# coding: utf8
'''
Compare(file1.xml,file2.xml) calculates precision and recall of BGPs in both files taking as ground truth the first file.
Both XML files should be conform to ranking.dtd.
'''
#    Copyright (C) 2017 by
#    Emmanuel Desmontils <emmanuel.desmontils@univ-nantes.fr>
#    Patricia Serrano-Alvarado <patricia.serrano-alvarado@univ-nantes.fr>
#    All rights reserved.
#    GPL v 2.0 license.

import rdflib
from rdflib import Graph, Literal, BNode, Namespace, RDF, URIRef, Variable
from rdflib.compare import to_canonical_graph, graph_diff

import xml.etree.ElementTree as ET

from lib.bgp import *

import csv
import os

from tools.ProcessSet import * #ED
import multiprocessing as mp #ED


#==================================================
#ED


def calcPrecisionRecall2(cano_dbp,cano_lift,size_bgp_dbp,size_bgp_lift):
	if cano_dbp == cano_lift:# If ground truth and deduction is equal then precision and recall are 1
		precision = 1
		recall = 1
	else:
		in_both, in_first, in_second = graph_diff(cano_dbp,cano_lift)
		b = len(in_both) # b has the number of well deduced triple patterns
		for s,p,o in in_first:
			for ss,pp,oo in in_second:
				if ((isinstance(s,Variable) and isinstance(ss,Variable)) and p==pp and o==oo) or (
				s==ss and p==pp and (isinstance(o,Variable) and isinstance(oo,Variable)) or (
				s==ss and (isinstance(p,Variable) and isinstance(pp,Variable)) and o==oo)):
					b += 1 # b is incremented with triple patterns whose variables were canonized differently because the size of the BGP and that have two things in common (subject, predicate, or object);
					break
		try:
			precision = b/size_bgp_lift #How many deduced triple patterns are relevant
		except ZeroDivisionError:
			precision = 0
			print ("Division by zero in precision with size_bgp_lift")
		try:
			recall = b/size_bgp_dbp # How many relevant triple patterns are deduced
		except ZeroDivisionError:
			recall = 0
			print ("Division by zero in recall with size_bgp_dbp")				
	return (precision, recall)

def processBGPs(idp, mess):
	(cano_dbp, cano_lift,size_bgp_dbp,size_bgp_lift,dbp_occ, lift_occ, dbp_rank, lift_rank) = mess
	#(precision, recall) = calcPrecisionRecall2(cano_dbp,cano_lift,size_bgp_dbp,size_bgp_lift)
	(precision, recall, inter, mapping) = calcPrecisionRecall(cano_dbp,cano_lift)
	#print(precision,recall,'|',precision2,recall2)
	if recall == 0:
		return ()
	else: 
		result = (cano_dbp,cano_lift,dbp_occ, lift_occ, dbp_rank, lift_rank,precision,recall)
		return result

#==================================================
#ED
manager = mp.Manager()

class Context:
	def __init__(self):
		self.result_set = manager.list()

def processResults(inq, ctx): 
	result = inq.get()
	while result is not None:
		if result != (): 
			#print(result)
			ctx.result_set.append(result)
		result = inq.get()

#==================================================
def write_result_csv(result,path,fileName):
	head,tail = os.path.split(path)
	absPath = os.path.abspath(os.path.join(path,fileName+"precisionRecall.csv"))
	with open(absPath,"w") as f:
		fn=['date','ip','bgp_dbp','bgp_lift','nb_occurrences_dbp','nb_occurrences_lift','rank_dbp','rank_lift','precision','recall']
		writer = csv.DictWriter(f,delimiter=';',quotechar='"',fieldnames=fn)
		writer.writeheader()
		for (bgp_dbp,bgp_lift,nb_occurrences_dbp,nb_occurrences_lift,rank_dbp,rank_lift,precision,recall) in result:
			writer.writerow({'date':tail,'ip':fileName[:-1],'bgp_dbp':bgp_dbp,'bgp_lift':bgp_lift,'nb_occurrences_dbp':nb_occurrences_dbp,'nb_occurrences_lift':nb_occurrences_lift,'rank_dbp':rank_dbp,'rank_lift':rank_lift,'precision':precision,'recall':recall})

#==================================================
def compare(file_ground_truth, file_lift_deduction):
	tree_dbp = ET.parse(file_ground_truth)
	root_dbp = tree_dbp.getroot()
	tree_lift = ET.parse(file_lift_deduction)
	root_lift = tree_lift.getroot()

	result = []

	psb = ProcessSetBack(mp.cpu_count(),processBGPs,)	#ED  mp.cpu_count()
	psb.start()	#ED
	ctx = Context() #ED
	resultProcess = mp.Process(target=processResults,args=(psb.back_queue,ctx))	#ED
	resultProcess.start() #ED
	
	for entry_dbp in root_dbp.findall('entry-rank'):
		size_bgp_dbp = len(entry_dbp.findall("./bgp/tp"))
		if size_bgp_dbp > 1: # Only BGPs with more than one triple pattern are analyzed
			cano_dbp = unSerializeBGP(entry_dbp.find('bgp'))
			for entry_lift in root_lift.findall('entry-rank'):
				size_bgp_lift = len(entry_lift.findall("./bgp/tp"))
				cano_lift = unSerializeBGP(entry_lift.find('bgp'))
				#print(cano_dbp)
				#print(cano_lift)
				#print('---')
				psb.put( (cano_dbp, cano_lift,size_bgp_dbp, size_bgp_lift,entry_dbp.get('nb-occurrences'),entry_lift.get('nb-occurrences'),entry_dbp.get('rank'),entry_lift.get('rank')) ) #ED

				# if cano_dbp == cano_lift:# If ground truth and deduction is equal then precision and recall are 1
				# 	result += [(cano_dbp,cano_lift,entry_dbp.get('nb-occurrences'),entry_lift.get('nb-occurrences'),entry_dbp.get('rank'),entry_lift.get('rank'),1,1)]
				# else:
				# 	in_both, in_first, in_second = graph_diff(cano_dbp,cano_lift)
				# 	b = len(in_both) # b has the number of well deduced triple patterns
				# 	for s,p,o in in_first:
				# 		for ss,pp,oo in in_second:
				# 			if ((isinstance(s,Variable) and isinstance(ss,Variable)) and p==pp and o==oo) or (
				# 			s==ss and p==pp and (isinstance(o,Variable) and isinstance(oo,Variable)) or (
				# 			s==ss and (isinstance(p,Variable) and isinstance(pp,Variable)) and o==oo)):
				# 				b += 1 # b is incremented with triple patterns whose variables were canonized differently because the size of the BGP and that have two things in common (subject, predicate, or object);
				# 				break
				# 	try:
				# 		precision = b/size_bgp_lift #How many deduced triple patterns are relevant
				# 	except ZeroDivisionError:
				# 		precision = 0
				# 		print ("Division by zero in precision with size_bgp_lift")
				# 	try:
				# 		recall = b/size_bgp_dbp # How many relevant triple patterns are deduced
				# 	except ZeroDivisionError:
				# 		recall = 0
				# 		print ("Division by zero in recall with size_bgp_dbp")				
				# 	if (recall != 0): #ED
				# 		result += [(cano_dbp,cano_lift,entry_dbp.get('nb-occurrences'),entry_lift.get('nb-occurrences'),entry_dbp.get('rank'),entry_lift.get('rank'),precision,recall)]
	
	psb.stop() #ED
	resultProcess.join()	#ED

	# print(ctx.result_set)

	return ctx.result_set

# result = compare("rankDBpedia.xml","rankLIFT.xml")
# write_result_csv(result,)size_bgp_lift