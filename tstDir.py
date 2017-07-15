#!/usr/bin/env python3.6
# coding: utf8

import os
import os.path
import glob
import datetime as dt
from tools.tools import *
from functools import reduce
from copy import *
from random import *

from lxml import etree  # http://lxml.de/index.html#documentation

# def listdirectory(path):  
#     fichiers=[]  
#     for root, dirs, files in os.walk(path):  
#         for i in files:  
#             fichiers.append(os.path.join(root, i))  
#     return fichiers
# print(listdirectory('/Users/desmontils-e/Programmation/Python/data/logs20151031/logs-20151031-all2'))

def listdirectory2(path):  
    fichier=[]  
    l = glob.glob(path)  
    for i in l:  
        if os.path.isdir(i): fichier.extend(listdirectory(i))  
        else: fichier.append(i)  
    return fichier

#Récupérer les heures
dirs = [x for x in os.listdir('/Users/desmontils-e/Programmation/Python/data/logs20151031/logs-20151031-all2') if not(os.path.isdir(x)) and not(x.startswith('.')) ]
dirs.sort()
print(dirs)

hours = dict()
minutes = dict()
for h in dirs:
	h = h[:13]+':'+h[14:16]+':'+h[17:]
	hours[h] = []
	for i in range(0,60):
		m1 = h[:-5]
		m2 = h[-3:]
		if i<10: minutes[m1+'0'+str(i)+m2]=[]
		else: minutes[m1+str(i)+m2]=[]
# print(minutes)
print(hours)

#Récupérer les fichiers
lst = listdirectory2('/Users/desmontils-e/Programmation/Python/data/logs20151031/logs-20151031-all2/*/*-be4dbp-tested-TPF.xml')
# print("\n".join(lst))

#Trier les fichiers par IP
d = dict()
for f in lst :
	ip = f[-54:-22]
	if ip in d:
		d[ip].append(f)
	else:
		d[ip] = [f]

#Pour chaque IP, 
parser = etree.XMLParser(recover=True, strip_cdata=True)
for (ip,l) in d.items():
	print('Etude de ',ip)
	# Récupérer les entries
	entries = []
	ip_hours = deepcopy(hours)
	ip_minutes = deepcopy(minutes)
	for file in l:
		print('Traitement de %s' % file)	
		tree = etree.parse(file, parser)
		#---
		dtd = etree.DTD('http://documents.ls2n.fr/be4dbp/log.dtd')
		assert dtd.validate(tree), '%s non valide au chargement : %s' % (
		    file, dtd.error_log.filter_from_errors()[0])
		#---
		# print('DTD valide !')
		for entry in tree.getroot():
			if entry.get('valid') == 'TPF': 
				entries.append(entry)
				date = entry.get('datetime')
				ip_hours[date].append(entry)

	nb_req = reduce( lambda x,y: x+y, [len(x) for x in ip_hours.values() ] )
	print(' - nb hours:', len(l) )
	print('- nb req:', nb_req )

	if (nb_req>0):
		for h in ip_hours:
			nb = len(ip_hours[h])
			print(nb)
			v = sorted([randrange(0,3600) for i in range(nb)])
			for i in range(0,nb):
				e = ip_hours[h][i]
				t = date2str(fromISO(e.get('datetime'))+dt.timedelta(seconds=v[i])) 
				# print(t)
				e.set('datetime',t)

		# Générer le fichier de l'IP
		try:
			nfile = './tst/'+ip+'.xml'
			print('Création de "%s"', nfile)
			xml_str = '<?xml version="1.0" encoding="utf-8"?>\n'
			xml_str += '<!DOCTYPE log SYSTEM "http://documents.ls2n.fr/be4dbp/log.dtd">\n'
			xml_str += '<log ip="%s" date="%s">\n' % (ip,now())
			f_out = open(nfile, 'w')
			f_out.write(xml_str)

			for s in entries:
				xml_entry = etree.tostring(s,encoding="UTF-8",pretty_print=True)
				f_out.write(xml_entry.decode('utf-8'))

			f_out.write('</log>')

		except Exception as e:
		    print('PB Save Entry : %s', e.__str__())
		finally:
		    if f_out: f_out.close()
	else:
		print('No file produced')
