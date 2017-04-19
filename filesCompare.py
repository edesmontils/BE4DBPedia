#!/usr/bin/env python3.6
# coding: utf8
'''
Calls the function compare() from bgpCompare.py to calculate the precision and recall of fileDeduced against the ground truth file
'''
#    Copyright (C) 2017 by
#    Emmanuel Desmontils <emmanuel.desmontils@univ-nantes.fr>
#    Patricia Serrano-Alvarado <patricia.serrano-alvarado@univ-nantes.fr>
#    All rights reserved.
#    GPL v 2.0 license.

import os
import argparse
from bgpCompare import *

parser = argparse.ArgumentParser()
parser.add_argument("groundTruth", help="the ground truth directory with xml files WITHOUT THE LAST SLASH /")
parser.add_argument("deduction", help="the deduction directory with xml files WITHOUT THE LAST SLASH /")
args = parser.parse_args()

path = "precisionRecall/"+args.groundTruth
head,tail = os.path.split(path)
path = os.path.join("precisionRecall",tail)

if not os.path.exists(path): # if the directory does not exist
    os.mkdir(path) # make the directory
else: # the directory exists
    #removes all files in a folder
    for the_file in os.listdir(path):
        file_path = os.path.join(path, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path) # unlink (delete) the file
        except Exception as e:
            print (e)

for file1, file2 in zip(sorted(os.listdir(args.groundTruth)),sorted(os.listdir(args.deduction))):
	if file1.endswith("ranking.xml") and file2.endswith("ranking.xml"):
		print("Analyzing ",file1," with ", file2)
		result = compare(os.path.join(args.groundTruth,file1),os.path.join(args.deduction,file2))
		write_result_csv(result,path,file1[:-11])