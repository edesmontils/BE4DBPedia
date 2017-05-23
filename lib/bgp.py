#!/usr/bin/env python3.6
# coding: utf8
"""
Tools to manage BGP
"""
#    Copyright (C) 2017 by
#    Emmanuel Desmontils <emmanuel.desmontils@univ-nantes.fr>
#    Patricia Serrano-Alvarado <patricia.serrano-alvarado@univ-nantes.fr>
#    All rights reserved.
#    GPL v 2.0 license.

from pprint import pprint

import rdflib
from rdflib import Literal, BNode, Namespace, RDF, URIRef, Variable
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.algebra import translateQuery, pprintAlgebra
from rdflib.compare import to_canonical_graph
from rdflib.namespace import XSD

import networkx as nx
import networkx.algorithms.isomorphism as iso
import re
import sys
import xml.etree.ElementTree as ET

from urllib.parse import urlparse, urlencode, quote, unquote
from lxml import etree  # http://lxml.de/index.html#documentation

from tools.tools import *

#==================================================


def canonicalize_sparql_bgp(gp):
    """Returns a canonical basic graph pattern (BGP) with canonical var names.

    :param gp: a GraphPattern in form of a list of triples with Variables
    :return: A canonical GraphPattern with Variables renamed.

    >>> U = URIRef
    >>> V = Variable
    >>> gp1 = [
    ...     (V('blub'), V('bar'), U('blae')),
    ...     (V('foo'), V('bar'), U('bla')),
    ...     (V('foo'), U('poo'), U('blub')),
    ... ]
    >>> cgp = canonicalize_sparql_bgp(gp1)
    >>> v_blub = V('cb0')
    >>> v_bar = V(
    ...  'cb3d1b27f6269e23775a8da8d966dd669aa8262176ae6b938cccd653316791c42269')
    >>> v_foo = V(
    ...  'cb3b2718590899b3875a33cdc4aad060832711a614ee9c0ac83323f2e961bcc3f2db')
    >>> expected = [
    ...     (v_blub, v_bar, U('blae')),
    ...     (v_foo, v_bar, U('bla')),
    ...     (v_foo, U('poo'), U('blub'))
    ... ]
    >>> cgp == expected
    True

    To show that this is variable name and order independent we shuffle gp1 and
    rename its vars:
    >>> gp2 = [
    ...     (V('foonkyname'), V('baaar'), U('bla')),
    ...     (V('foonkyname'), U('poo'), U('blub')),
    ...     (V('funkyname'), V('baaar'), U('blae')),
    ... ]
    >>> cgp == canonicalize_sparql_bgp(gp2)
    True

    Source : https://github.com/RDFLib/rdflib/issues/483 

    """
    #assert isinstance(gp, Iterable)
    g = rdflib.Graph()
    for t in gp:
        triple_bnode = BNode()
        s, p, o = [BNode(i) if isinstance(i, Variable) else i for i in t]
        g.add((triple_bnode, RDF['type'], RDF['Statement']))
        g.add((triple_bnode, RDF['subject'], s))
        g.add((triple_bnode, RDF['predicate'], p))
        g.add((triple_bnode, RDF['object'], o))
    cg = rdflib.compare.to_canonical_graph(g)
    cgp = []
    for triple_bnode in cg.subjects(RDF['type'], RDF['Statement']):
        t = [
            cg.value(triple_bnode, p)
            for p in [RDF['subject'], RDF['predicate'], RDF['object']]
        ]
        t = tuple([Variable(i) if isinstance(i, BNode) else i for i in t])
        cgp.append(t)
    return sorted(cgp)


#==================================================
basicNodes = [
            '_vars', 'start', 'length', 'PV', 'datasetClause', 'arg', 'op',
            'Literal', 'other', 'expr', 'lazy', 'var', 'A', 'template', 'term',
            'res'
    ]

def parse(tt, n, ind=''):
    print(ind, '--', tt, '--')
    if tt in basicNodes:
        print(ind, n)
    elif tt == 'part':
        print(ind, 'part:', n)
        for k in n:
            parse('', k, ind + ' | ')
    else:
        if (tt == 'triples'):
            print(n)
        elif (n.name in 'BGP'):
            print(ind, 'BGP:', n.triples)
        else:
            print(ind, 'node:', n.name)
            for k in n:
                parse(k, n[k], ind + ' | ')


#==================================================


def _getBGP(tt, n):
    if (tt in basicNodes):
        return []
    elif tt == 'part':
        s = []
        for k in n:
            s += _getBGP('', k)
        return s
    elif tt == 'triples':
        return n
    elif (n.name == 'BGP'):
        # if n.triples == []: raise ValueError('Empty BGP')
        # else: return n.triples
        return n.triples
    else:
        s = []
        for k in n:
            s += _getBGP(k, n[k])
        return s


def treat(i):
    if isinstance(i, URIRef):
        if rdflib.term._is_valid_uri(i):
            return i
        else:
            raise ValueError(i)
    else:
        return i


def getBGP(n):
    bgp = _getBGP('root', n)
    nbgp = []
    for (s, p, o) in bgp:
        nbgp.append((treat(s), treat(p), treat(o)))
    return nbgp

#==================================================

def serialize2string(i):
    if isinstance(i, Variable):
        return '?'+ i.__str__()
    elif isinstance(i, URIRef):
        return '<' + i.__str__() + '>'
    elif isinstance(i, Literal):
        if i.language is not None: return '"'+i.__str__() + '"@'+str(i.language)
        elif i.datatype is not None: 
            return '"'+i.__str__() + '"^^'+str(i.datatype)
        else: 
            return '"'+i.__str__() + '"'
    else:
        return i.__str__()

def serialize2str(name, i):
    if isinstance(i, Variable):
        return '<' + name + ' type="var" val="' + i.__str__() + '"/>'
    elif isinstance(i, URIRef):
        # return '<' + name + ' type="iri" val="' + i.__str__().replace('&', '&amp;') + '"/>'
        return '<' + name + ' type="iri" val="' + i.__str__().replace('&', '&amp;') + '"/>'
    elif isinstance(i, Literal):
        s = '<' + name + ' type="lit"'
        if i.language: s += ' language="'+str(i.language)+'"'
        elif i.datatype: s += ' datatype="'+str(i.datatype)+'"'
        s += '><![CDATA[' + i.__str__() + ']]></' + name + '>'
        return s
    else:
        return '<' + name + ' type="bnode" val="' + i.__str__() + '"/>'


def serialize(name, i):
    node = etree.Element(name)
    if isinstance(i, Variable):
        node.set('type', 'var')
        node.set('val', i.__str__())
    elif isinstance(i, URIRef):
        node.set('type', 'iri')
        # unquote(i.__str__()).replace('&', '&amp;'))
        node.set('val', i.__str__())
    elif isinstance(i, Literal):
        node.set('type', 'lit')
        node.text = i.__str__()  # '<![CDATA[' + i.__str__() + ']]>'
        if i.language: node.set('language',str(i.language))
        elif i.datatype: node.set('datatype',str(i.datatype))
    else:
        node.set('type', 'bnode')
        node.set('val', i.__str__())
    return node

#==================================================

def serializeTP2str(s,p,o):
    return '<tp>' + serialize2str('s', s) + serialize2str('p', p) + serialize2str('o', o) + '</tp>'


def serializeBGP2str(bgp):
    """
    from rdflib -> string
    """
    #---
    assert bgp is not None
    #---
    ser = '<bgp>\n'
    for (s,p,o) in bgp:
        ser += serializeTP2str(s,p,o) + '\n' # '<tp>' + serialize2str('s', s) + serialize2str('p', p) + serialize2str('o', o) + '</tp>\n'
    ser += '</bgp>\n'
    return ser


def serializeBGP(bgp):
    """
    from rdflib -> lxml
    """
    #---
    assert bgp is not None
    #---
    bgp_node = etree.Element('bgp')
    for (s, p, o) in bgp:
        tp_node = etree.Element('tp')
        tp_node.append(serialize('s', s))
        tp_node.append(serialize('p', p))
        tp_node.append(serialize('o', o))
        bgp_node.append(tp_node)
    return bgp_node

#==================================================
reLiteral1 = re.compile(r'\A"(?P<str>.*)"\^\^\<?(?P<iri>http.*)\>?\s*\Z')
reLiteral2 = re.compile(r'\A"(?P<str>.*)"\@(?P<lang>\w+)\s*\Z')
reLiteral3 = re.compile(r'\A"(?P<str>.*)"\s*\Z')

def unSerialize(i):
    if i.attrib['type'] == 'var':
        return Variable(i.attrib['val'])
    elif i.attrib['type'] == 'iri':
        return URIRef(i.attrib['val'])  # quote(i.attrib['val']) )
    elif i.attrib['type'] == 'lit':
        if 'language' in i.attrib : 
            lang = i.attrib['language']
            #print ('lang:',lang)
            return Literal(i.text, lang=lang)
        elif 'datatype' in i.attrib : 
            dtt = i.attrib['datatype']
            #print('datatype:',dtt,':')
            l = Literal(i.text, datatype=dtt)
            #print(isValidURI(dtt))
            return l
        else: 
            val = i.text
            m = reLiteral1.search(val)
            if m:
                lit = m.group('str')
                iri = m.group('iri')
                if not(isValidURI(iri)) : print ('***************** Pb iri :',iri)
                return Literal(re.sub('"','\'',lit), datatype=iri)
            else:
                m = reLiteral2.search(val)
                if m:
                    lit = m.group('str')
                    lang = m.group('lang')
                    return Literal(re.sub('"','\'',lit), lang=lang)
                else: 
                    m = reLiteral3.search(val)
                    if m:
                        lit = m.group('str')
                        return Literal(re.sub('"','\'',lit))
                    else: 
                        return Literal(re.sub('"','\'',val))
    else:
        return BNode(i.attrib['val'])

def unSerializeTP(tp):
    return (unSerialize(tp[0]), unSerialize(tp[1]), unSerialize(tp[2]))

def unSerializeBGP(bgp):
    """
    from lxml -> rdflib
    """
    nbgp = []
    for tp in bgp:
        nbgp += [unSerializeTP(tp)] #[(unSerialize(tp[0]), unSerialize(tp[1]), unSerialize(tp[2]))]
    return nbgp


#==================================================


def count(q, bgp):
    """
    Compte le nombre de fois que l'on trouve la variable dans le BGP
    Si c'est > 1 alors il y a une jointure sur cette variable !
    """
    #---
    assert bgp is not None
    #---
    n = 0
    for (s, p, o) in bgp:
        if (s == q) or (p == q) or (o == q):
            n += 1
    return n


#==================================================

def isValidSubject(s):
    return (isinstance(s,Variable) or isinstance(s,URIRef) or isinstance(s,BNode) or isinstance(s,Literal))

def isValidPredicate(p):
    return (isinstance(p,Variable) or isinstance(p,URIRef) or isinstance(p,BNode))

def isValidObject(o) :
    return (isinstance(o,Variable) or isinstance(o,URIRef) or isinstance(o,BNode) or isinstance(o,Literal))

def isValidTP(s,p,o):
    return isValidSubject(s) and isValidPredicate(p) and isValidObject(o)

def valid(bgp):
    #---
    assert bgp is not None
    #---
    ok = True
    for (s, p, o) in bgp:
        ok = isValidTP(s,p,o)
        if ok and isinstance(p,Variable) : ok = (count(p,bgp) == 1)
        # ok = (isinstance(s,Variable) or isinstance(s,URIRef) or isinstance(s,BNode)) \
        #  and (isinstance(o,Variable) or isinstance(o,URIRef) or isinstance(o,BNode) or isinstance(o,Literal)) \
        #  and ((isinstance(p,Variable) and (count(p,bgp) == 1)) or isinstance(p,URIRef) or isinstance(p,BNode))
        if not (ok):
            break
    return ok

def haveJoin(bgp):
    #hSJ = False
    join_count = {'s-s':0, 'o-o':0,'s-o':0, 'sp-sp':0, 'po-po':0, 'sp-po':0, 'self':0, 'star':0, 'path':0}
    for (i, (si, pi, oi)) in enumerate(bgp):
        for (j, (sj, pj, oj)) in enumerate(bgp[(i+1):]) :
            # print('i:',i,si, pi, oi,'j:',j, sj, pj, oj)
            if (oi==si)  and isinstance(si,Variable) and isinstance(oi,Variable):
                join_count['self'] +=1
            if (pi==pj):# and isinstance(pi,Variable) and isinstance(pj,Variable) :
                if (si==sj) and isinstance(si,Variable) and isinstance(sj,Variable):
                    join_count['sp-sp'] +=1
                elif (si==oj)  and isinstance(si,Variable) and isinstance(oj,Variable):
                    join_count['sp-po'] +=1
                elif (oi==sj) and isinstance(oi,Variable) and isinstance(sj,Variable):
                    join_count['sp-po'] +=1
                if (oi==oj) and isinstance(oi,Variable) and isinstance(oj,Variable):
                    join_count['po-po'] +=1
            else:
                if (si==sj) and isinstance(si,Variable) and isinstance(sj,Variable):
                    join_count['s-s'] +=1    
                elif (si==oj) and isinstance(si,Variable) and isinstance(oj,Variable):
                    join_count['s-o'] +=1
                elif (oi==sj) and isinstance(oi,Variable) and isinstance(sj,Variable):
                    join_count['s-o'] +=1
                if (oi==oj) and isinstance(oi,Variable) and isinstance(oj,Variable):
                    join_count['o-o'] +=1
    nbTP = len(bgp)
    if  (join_count['s-s'] == nbTP) or (join_count['o-o'] == nbTP):
        join_count['star'] +=1
    if join_count['s-o'] == nbTP-1 :
        join_count['path'] += 1
    return join_count #hSJ

#==================================================

def nm(n1, n2):
    #print(n1) ; print(n2)
    t1 = n1['type']
    t2 = n2['type']
    ok = t1 == t2
    #print ('nm:',ok,'(',t1,'%%',t2,')')
    return ok

#==================================================

def em(e1, e2):
    ok = False
    for i in e1:
        t1 = e1[i]['prop']
        for j in e2:
            t2 = e2[j]['prop']
            ok = t1 == t2
            if ok:
                break
        if ok:
            break
    #print ('em2:', ok) ; print ('\t',e1) ; print ('\t',e2)
    return ok

#==================================================

def toRDFLibGraph(bgp):
    """
    BGP (list of TP) -> RDFLib Graph
    """
    #---
    assert bgp is not None
    #---
    g = rdflib.Graph()
    for (s, p, o) in bgp:
        g.add((s, p, o))
    return g 


#==================================================


def BGPtoGraph(bgp):
    """
    BGP (list of TP) -> networkx Graph
    """
    #---
    assert bgp is not None
    #---
    g = nx.MultiDiGraph()
    for tp in bgp:
        addTP(g,tp)
    return g

def addTP(g,tp):
    (s,p,o) = tp
    addNode(g,s)
    addNode(g,o)
    addEdge(g,s,p,o)

def toStr(i):
    if isinstance(i,Variable):
        return '?'+i.__str__()
    elif isinstance(i,URIRef):
        return 'URIRef@'+i.__str__()
    else: return 'Lit:'+i.__str__().replace(' ','_')

def addNode(g,n):
    s = toStr(n)
    if not (s in g):
        if isinstance(n, Variable):
            g.add_node(s, type='?Var')
        else:
            g.add_node(s, type=toStr(n))

def addEdge(g,s,p,o):
    if isinstance(p, Variable):
        g.add_edge(toStr(s), toStr(o), prop='?Var')
    else:
        g.add_edge(toStr(s), toStr(o), prop=toStr(p) )

#==================================================


def equals(g1, g2):
    #---
    assert isinstance(g1, nx.Graph) and isinstance(g2, nx.Graph)
    #---
    return nx.isomorphism.GraphMatcher(g1, g2, 
        #node_match=nm, 
        node_match=iso.categorical_node_match('type', ''),
        #edge_match=em
        edge_match=iso.categorical_multiedge_match('prop','')
        ).is_isomorphic()

# Ne fonctionne pas. Déposé un ticket sur le projet NetworkX 
# car j'ai des doutes sur le bon fonctionnement de "subgraph_is_isomorphic"
# voir le code en bas.
# A suivre...
def isSubGraphOf(g1, g2):
    #---
    assert isinstance(g1, nx.Graph) and isinstance(g2, nx.Graph)
    #---
    GM = nx.isomorphism.GraphMatcher(g2, g1, 
                                     #node_match=nm,
                                     node_match=iso.categorical_node_match('type', ''), 
                                     #edge_match=em
                                     edge_match=iso.categorical_multiedge_match('prop','type')
                                     )
    if GM.subgraph_is_isomorphic(): return GM.mapping
    else: return None




#==================================================
#==================================================
#==================================================


def inGraph(tpSet, Gref):
    g = nx.MultiDiGraph()
    for tp in tpSet: addTP(g,tp)
    return isSubGraphOf(g,Gref)

def max (un, deux):
  (p1, r1, s1) = un
  (p2, r2, s2) = deux
  if p2*r2 > p1*r1: return deux
  else: return un



def egal(tp1, tp2):
    (s1, p1, o1) = tp1
    (s2, p2, o2) = tp2

    okS = (s1 == s2) or (isinstance(s1,Variable) and  isinstance(s2,Variable))
    okP = (p1 == p2) or (isinstance(p1,Variable) and  isinstance(p2,Variable))
    okO = (o1 == o2) or (isinstance(o1,Variable) and  isinstance(o2,Variable))
    mapping = dict()
    if okS and (isinstance(s1,Variable) and  isinstance(s2,Variable)): 
        mapping[s1] = s2
    if okP and (isinstance(p1,Variable) and  isinstance(p2,Variable)): 
        if p1 in mapping:
            okP = mapping[p1] == p2
        else: mapping[p1] = p2
    if okP and (isinstance(o1,Variable) and  isinstance(o2,Variable)): 
        if o1 in mapping:
            okO = mapping[o1] == o2
        else: mapping[o1] = o2        
    ok = okS and okP and okO  
    # print('----------------------------')
    # print(tp1) 
    # print(tp2)
    # if ok: print(mapping)
    # else: print('...')
    return (ok, mapping )

def includes(BGPref, BGPtest):
    v = list()
    for tp1 in BGPtest:
        mappingList = list()
        for tp2 in BGPref:
            (ok, m) = egal(tp1,tp2)
            if ok:
                mappingList.append(m)
        v.append( (tp1, mappingList) )
    #pprint(v)
    mapping=dict()
    for (tp,ml) in v:
        for x in ml:
            for k in x:
                mapping[k]=None
    #pprint(mapping)
    res = choice('',v,mapping)
    #pprint(res)
    return res

def choice(tab,s,mapping):
    if len(s)==0:
        #print(tab,'Yes!')
        return mapping
    else:
        #print(tab,s[0])
        (tp,lm) = s[0]
        for m in lm:
            mpg = mapping.copy()
            #print(tab,mpg)
            ok = True
            for v in m:
                #print(tab,'for v=',v,'->',m[v])
                if (m[v] in mpg.values()) and mpg[v]!=m[v]:
                    ok = False
                elif mpg[v]==m[v]:
                    ok = ok and True
                elif mpg[v] is None:
                    mpg[v]=m[v]
                    ok = ok and True
                else:
                    ok = False
            if ok:
                #print(tab,mpg)
                res = choice(tab+'\t',s[1:],mpg)
                if res is not None:
                    return res
        return None

def calcPrecisionRecall(BGPref, BGPtst):
  #Gref = BGPtoGraph(BGPref)
  ref = len(BGPref)
  tst = len(BGPtst)  
  s = dict()
  m = (0,0,{})
  s[0] = []
  ltp = set()
  for tp in BGPtst:
    if includes(BGPref,[tp]):# inGraph({tp}, Gref) :
      ltp.add(tp)
      common = (1/tst, 1/ref, {tp})
      m = max(m, common)
      s[0].append( common )
  #pprint(s[0])
  for l in range(1,len(ltp)):
    s[l] = []
    for tp in ltp:
      for (p,r,x) in s[l-1] :
        if tp not in x:
          ns = x.copy()
          ns.add(tp)
          if includes(BGPref,ns):#inGraph(ns, Gref):
            cm = len(ns)
            common = (cm/tst, cm/ref,ns)
            m = max(m, common)
            if common not in s[l]: s[l].append( common )
    #print('pour ',l)
    #pprint(s[l])
  return m

def isSGO(g1, g2):
    GM = nx.isomorphism.GraphMatcher(g2, g1 ,
                                     #node_match=nm2,
                                     #node_match=iso.categorical_node_match('type', ''), 
                                     #edge_match=em2,
                                     edge_match=iso.categorical_multiedge_match('prop','type')
                                     )
    if GM.subgraph_is_isomorphic(): return GM.mapping
    else: return None

#from lib.QueryManager import *

if __name__ == "__main__":
    print("main")

    # g6 = nx.MultiDiGraph()
    # g6.add_edges_from([ (1,2,dict(prop='type')), 
    #                     (1,3,dict(prop='manage')),
    #                     #(1,4,dict(prop='manage')),
    #                     (1,3,dict(prop='knows')),
    #                     (3,2,dict(prop='type')),
    #                     (4,2,dict(prop='type'))  
    #                   ])
    # print('g6')
    # for e in g6.edges(data=True):
    #     pprint(e)

    # g7 = nx.MultiDiGraph()
    # g7.add_edges_from([ (5,6,dict(prop='type')), 
    #                     (5,7,dict(prop='knows')),
    #                     (7,6,dict(prop='type')),
    #                     (8,6,dict(prop='type')),
    #                     (9,6,dict(prop='type')),
    #                     (5,10,dict(prop='bP'))  
    #                   ])
    # print('g7')
    # for e in g7.edges(data=True):
    #     pprint(e)

    # map = isSGO(g6,g7)
    # if map is not None: print('g6 in g7 : ', map)
    # else: print('g6 not in g7') 
    # # ne répond pas bien avec : (1,3,dict(prop='manage')) -> g6 in g7 :  {5: 1, 6: 2, 7: 3, 8: 4}
    # # mais bonne réponse si remplacé par : (1,4,dict(prop='manage')) -> g6 not in g7

    # map = isSGO(g7,g6)
    # if map is not None: print('g7 in g6 : ', map)
    # else: print('g7 not in g6')

    query4 = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX ex: <http://exemple.org/ex#>
    select *
    where {?p1 rdf:type foaf:Person .
           ?p2 rdf:type foaf:Person .
           ?p1 foaf:knows ?p2 .
           ?p3 rdf:type foaf:Person .

           ?p1 ex:manage ?p2 . 
           #?p1 ex:manage ?p3 . 
           }
    """
    query5 = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX dbp: <http://fr.dbpedia.org/resource/>
    PREFIX dbo: <http://dbpedia.org/ontology/>
    select *
    where {?person1 rdf:type foaf:Person .
           ?person2 rdf:type foaf:Person .
           ?person1 foaf:knows ?person2 .
           ?person3 rdf:type foaf:Person .

           ?person4 rdf:type foaf:Person .
           ?person1 dbo:birthPlace dbp:Paris
           }
    """
    
    qm = QueryManager(modeStat = False)
    (BGPSet4, _) = qm.extractBGP(query4)
    (BGPSet5, _) = qm.extractBGP(query5)
    #print(calcPrecisionRecall(BGPSet4,BGPSet5))
    haveSelfJoin(BGPSet4)


