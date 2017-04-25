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
from bgp import *
import csv
import os

def write_result_csv(result,path,fileName):
	head,tail = os.path.split(path)
	absPath = os.path.abspath(os.path.join(path,fileName+"precisionRecall.csv"))
	with open(absPath,"w") as f:
		fn=['date','ip','bgp_dbp','bgp_lift','nb_occurrences_dbp','nb_occurrences_lift','rank_dbp','rank_lift','precision','recall']
		writer = csv.DictWriter(f,delimiter=';',quotechar='"',fieldnames=fn)
		writer.writeheader()
		for (bgp_dbp,bgp_lift,nb_occurrences_dbp,nb_occurrences_lift,rank_dbp,rank_lift,precision,recall) in result:
			writer.writerow({'date':tail,'ip':fileName[:-1],'bgp_dbp':bgp_dbp,'bgp_lift':bgp_lift,'nb_occurrences_dbp':nb_occurrences_dbp,'nb_occurrences_lift':nb_occurrences_lift,'rank_dbp':rank_dbp,'rank_lift':rank_lift,'precision':precision,'recall':recall})

def compare(file_ground_truth, file_lift_deduction):
	tree_dbp = ET.parse(file_ground_truth)
	root_dbp = tree_dbp.getroot()
	tree_lift = ET.parse(file_lift_deduction)
	root_lift = tree_lift.getroot()

	result = []
	for entry_dbp in root_dbp.findall('entry-rank'):
		size_bgp_dbp = len(entry_dbp.findall("./bgp/tp"))
		cano_dbp = unSerializeBGP(entry_dbp.find('bgp'))
		for entry_lift in root_lift.findall('entry-rank'):
			size_bgp_lift = len(entry_lift.findall("./bgp/tp"))
			cano_lift = unSerializeBGP(entry_lift.find('bgp'))
			if cano_dbp == cano_lift:# If ground truth and deduction is equal then precision and recall are 1
				result += [(cano_dbp,cano_lift,entry_dbp.get('nb-occurrences'),entry_lift.get('nb-occurrences'),entry_dbp.get('rank'),entry_lift.get('rank'),1,1)]
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
				precision = b/size_bgp_lift #How many deduced triple patterns are relevant
				recall = b/size_bgp_dbp # How many relevant triple patterns are deduced
				result += [(cano_dbp,cano_lift,entry_dbp.get('nb-occurrences'),entry_lift.get('nb-occurrences'),entry_dbp.get('rank'),entry_lift.get('rank'),precision,recall)]
	return result

# result = compare("rankDBpedia.xml","rankLIFT.xml")
# write_result_csv(result,)
