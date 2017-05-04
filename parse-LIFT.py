#!/usr/bin/env python3.6
# coding: utf8
from pprint import pprint

#import xml.etree.ElementTree as ET
from lxml import etree
import rdflib as rl
from tools.Log import Log
import re
from urllib.parse import *
from tools.tools import *
from tools.FSM import *
from lib.bgp import *
import sys
import argparse
import multiprocessing as mp
from queue import Empty


#==================================================
#==================================================

class LiftLog(Log):
	def __init__(self,file_name):
		Log.__init__(self,file_name)
		self.ctx = CTX(file_name) #Object()
		self.ctx.start()
		s1 = InitialState(1) ; s2 = State(2) ; s3 = State(3,action_in=LiftLog.newBGP)
		s4 = State(4, action_in=LiftLog.newTP) ; s5 = State(5) ; s6 = State(6)
		s7 = State(7,) ; s8 = State(8) ; s9 = State(9)
		s10 = State(10) ; s11 = State(11) ; s12 = State(12)
		s13 = State(13) ; s14 = State(14) ; s15 = FinalState(15); s16 = State(16)
		self.FSM = FSM(
			{"Deduced BGPs", "Single LDFs", " ", "Deduced LDF_", "received @[dbpediaLDF]", "BGP", "#", "S"},
			{s1, s2, s3, s4, s5, s6, s7,s8, s9, s10, s11, s12, s13, s14, s15,s16},
			s1, 
			{s15}, 
			{Transition("Deduced BGPs",s1,s2), 
			 Transition(" ",s2,s16), Transition("BGP",s16,s3), Transition(" ",s16,s16),
			 Transition("Deduced BGPs",s16,s7), # pb source 2969
			 Transition("Deduced LDF_",s3,s4), 
			 Transition(" ",s3,s6), # PB source 2773...
			 Transition("received @[dbpediaLDF]",s4,s5),
			 Transition("Deduced LDF_",s5,s4),
			 Transition("Deduced BGPs",s5,s7,action=LiftLog.finBGP), #PB source 2914
			 Transition("BGP",s5,s3,action=LiftLog.finBGP), Transition(" ",s5,s6,action=LiftLog.finBGP),
			 Transition("Deduced BGPs",s6,s7),Transition(" ",s6,s6),
			 Transition(" ",s7,s8),
			 Transition("#",s8,s9),Transition(" ",s8,s8),
			 Transition("#",s9,s10),
			 Transition(" ",s10,s11),
			 Transition("Single LDFs",s11,s12), Transition(" ",s11,s11),
			 Transition(" ",s12,s13),
			 Transition("Deduced LDF_",s13,s14),#,action=LiftLog.newSingleTP), 
			 Transition(" ",s13,s13),
			 Transition("received @[dbpediaLDF]",s14,s13), Transition("Single LDFs", s13,s15),
			 Transition("S",s15,s15),
			 Transition(" ",s15,s15),
			 Transition("Deduced BGPs",s15,s2)
			  },
			 self.ctx
			)

	def end(self):
		assert self.FSM.end()
		self.ctx.end()

	reSymbolDeducedBGPs = re.compile(r'--Deduced BGPs--')
	reSymbolSingleLDFs = re.compile(r'--Single LDFs')
	reSymbolReceived = re.compile(r'received\s+\@\[dbpediaLDF\]') # PB dans source, parfois plusieurs espaces après "received" -> \s+
	reSymbolReceived2 = re.compile(r'received\s+\@null') # PB dans la génération des sources
	reSymbolBGPNo = re.compile(r'BGP\s+(s|S)?(?P<nm>\d+)') #Pb 's?' à cause d'un s2 en 406
	reSymbolBGPNo2 = re.compile(r'BGP\s+\[no(?P<nm>\d+)\]') # PB dans la génération des sources
	reSymbolDeducedLDF = re.compile(r'Deduced LDF_(?P<nm>\d+): (?P<tp>.+)')

	def extract(self,res):
		res = res.strip()
		#print('res:"%s" (%s)' % (res,self.FSM.currentState.name))
		if res == "": r = self.FSM.applyDet(" ")
		elif res.startswith('#'): r = self.FSM.applyDet("#")
		elif LiftLog.reSymbolDeducedBGPs.search(res): r = self.FSM.applyDet("Deduced BGPs")
		elif LiftLog.reSymbolSingleLDFs.search(res): r = self.FSM.applyDet("Single LDFs")
		elif LiftLog.reSymbolReceived.search(res): r = self.FSM.applyDet("received @[dbpediaLDF]")
		elif res == 'S' : r = self.FSM.applyDet("S") # erreur dans un fichier
		elif LiftLog.reSymbolReceived2.search(res): r = self.FSM.applyDet("received @[dbpediaLDF]") # PB dans la génération des sources
		else:
			s = LiftLog.reSymbolBGPNo.search(res)
			if s : r = self.FSM.applyDet("BGP", s.group('nm'))
			else:
				s = LiftLog.reSymbolBGPNo2.search(res)
				if s: 
					r = self.FSM.applyDet("BGP", s.group('nm'))				
				else:
					s = LiftLog.reSymbolDeducedLDF.search(res)
					if s : r = self.FSM.applyDet("Deduced LDF_", (s.group('nm'),s.group('tp')) )
					else: 
						print('erreur %s' % res)
						r = (None, None, None)
		return r

	def newBGP(ctx, symbol, msg):
		#ctx.bgp = []
		#print('a new BGP',msg)
		ctx.newBGP(msg)
		return True

	def finBGP(ctx, symbol, msg):
		#print('fin BGP')
		ctx.saveBGP()
		return True

	def newTP(ctx, symbol, msg):
		(nm, tp) = msg
		ntp = LiftLog.manageTP(tp,nm)
		if ntp is not None: ctx.addTP(ntp)
		return True

	def newSingleTP(ctx, symbol, msg):
		(nm, tp) = msg
		ctx.newBGP(nm)
		ntp = LiftLog.manageTP(tp,nm)
		if ntp is not None:
			ctx.addTP(ntp)
			ctx.saveBGP()
		return True		

	reTP = re.compile(r'\A(?P<s>\S+)\s+(?P<p>\S+)\s+(?P<o>.+)\Z')
	reINJECTED = re.compile(r'\AINJECTED(?P<type>\w+)\(LDF_(?P<nm>\d+)\)\Z')
	reLiteral = re.compile(r'\A(?P<str>".*"\^\^)(?P<iri>http.*)\Z')

	def manageTP(tp,nm):
		#if 'null' in tp: print(tp)
		triplet = [] #tp.split(' ')
		m = LiftLog.reTP.search(tp)
		try:
			if m: 
				triplet.append(m.group('s'))
				triplet.append(m.group('p'))
				triplet.append(m.group('o'))			# tout ce qui commence par http => IRI !!!
			else: 
				if '?s ' in tp: # on gère les cas où seulement des couples et pas des triplets
					tp += ' ?o'
				elif ' ?o' in tp:
					tp = '?s '+tp
				else: raise Exception("Erreur TP pas triplet")
				m = LiftLog.reTP.search(tp)
				if m: 
					triplet.append(m.group('s'))
					triplet.append(m.group('p'))
					triplet.append(m.group('o'))
				else: raise Exception("Erreur TP pas triplet")
			#print('ok:',triplet)
			s = ''
			for x in triplet:
				if x.startswith('http://'): s += ' <'+x+'> ' #'<http://'+quote(x[7:])+'> '
				else:
					m = LiftLog.reINJECTED.search(x)
					if m: 
						if m.group('type') == 'obj': s+= '?o_'+m.group('nm')+' '
						else: s+= '?s_'+m.group('nm')+' '
					else:
						if x=='?s': s += '?s_'+str(nm)+' '
						elif x=='?o': s += '?o_'+str(nm)+' '
						elif x=='?p': s += '?p_'+str(nm)+' '
						else: 
							m = LiftLog.reLiteral.search(x)
							if m: 
								x = m.group('str')+'<'+m.group('iri')+'>'
								s += x
							else:
								if x.startswith('?') or x.startswith('<') or x.startswith('"'):
									s += x
								else:
									if x.startswith("null"):
										print("Erreur TP : %s" % x)
										x = '"TPError:'+quote(x)+'"'
										print(x)
										s += x
										# raise Exception("Erreur TP : %s" % x)
									else: 
										x = '"'+x+'"'
										s += x
							
			s += '.'
			#print('au final:',s)
			g = rl.Graph()
			g.parse(data=s,format="turtle")
			bgp = [ (s,p,o) for (s,p,o) in g]
			newTP = bgp[0]
			(s,p,o) = newTP
			if isValidTP(s,p,o): return newTP
			else: return None
		except Exception as e:
			print('Exception de syntaxe sur le triplet !', tp)
			print(e)
			#sys.exit()
			return None

#==================================================
#==================================================

class CTX:
	def __init__(self, file):
		print('Compute ',file)
		self.root_log = None
		self.file = file+'-lift.xml'
		self.nb = 0
		self.bgp = []
		self.no_bgp = 0

	def start(self):
		#print('start')
		self.root_log = etree.Element('log')
		self.root_log.set('ip', 'host')
		self.root_log.set('date', date2str(now()))

	def end(self):
		#print('end')
		try:
			f_out = open(self.file, 'w')
			cdc = etree.tostring(self.root_log,
						encoding="UTF-8",
						pretty_print=True,
						xml_declaration=True, doctype='<!DOCTYPE log SYSTEM "./resources/log.dtd">'
						)
			f_out.write(cdc.decode('utf-8'))
		except Exception as e:
			print(e)
		finally:
		    f_out.close()

	def newBGP(self,no):
		#print('new bgp')
		self.bgp = []
		self.no_bgp = no

	def addTP(self,tp):
		#print('add TP',tp)
		self.bgp.append(tp)

	def saveBGP(self):
		#print('saveBGP',self.bgp)
		self.nb += 1
		s = self.buildXMLBGP(self.bgp, "%s" % self.no_bgp, "date", self.nb)
		self.root_log.append(s)

	def buildXMLBGP(self, bgp, host, date, line):
	    try:
	        entry_node = etree.Element('entry')
	        entry_node.set('datetime', '%s' % date)
	        entry_node.set('logline', '%d' % line)
	        request_node = etree.SubElement(entry_node, 'request')
	        g = rl.Graph()
	        query = 'select * where{\n'
	        for (s,p,o) in bgp:
	        	g.add( (s,p,o) )
	        	#query += serialize2string(s)+' '+serialize2string(p)+' '+serialize2string(o)+' .\n'
	        #print(g.serialize(format='nt').decode('utf8'))
	        query += g.serialize(format='nt').decode('utf8')
	        query += '}'
	        request_node.text = query
	        try:
	            bgp_node = serializeBGP(bgp)
	            entry_node.insert(1, bgp_node)
	        except Exception as e:
	            print('(%s) PB serialize BGP : %s\n%s', host, e.__str__(), bgp)
	        return entry_node
	    except ValueError as e:
	        return None
	    except Exception as e :
	    	print(e)
	    	return None

#==================================================
#==================================================

def analyse(in_queue):
	while True:
		try:
			mess = in_queue.get()
			if mess is None: break
			else: 
				l = LiftLog(mess)
				for (x, y, z) in l :
					pass #assert (x,y,z) == (True,True,True)
		except Empty as e:
			print('empty!')
		except Exception as e:
			print(mess, e)
			break

#==================================================
#==================================================
#==================================================

if __name__ == '__main__':
	print('main de parse-LIFT.py')
	parser = argparse.ArgumentParser(description='Etude du log LIFT')
	parser.add_argument('files', metavar='file', nargs='+',
	                    help='files to analyse')
	parser.add_argument("-p", "--proc", type=int, default=mp.cpu_count(), dest="nb_processes",
	                    help="Number of processes used (%d by default)" % mp.cpu_count())
	args = parser.parse_args()
	file_set = args.files
	nb_processes = args.nb_processes

	compute_queue = mp.Queue(nb_processes)
	process_list = [
	    mp.Process(target=analyse, args=(compute_queue,))
	    for _ in range(nb_processes)
	]
	for process in process_list:
	    process.start()

	for file in file_set:
	    if existFile(file):
	        compute_queue.put(file)

	for process in process_list:
	    compute_queue.put(None)
	for process in process_list:
	    process.join()





