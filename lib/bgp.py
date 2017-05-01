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

import networkx as nx
import networkx.algorithms.isomorphism as iso

import sys
import xml.etree.ElementTree as ET

from urllib.parse import urlparse, urlencode, quote, unquote
from lxml import etree  # http://lxml.de/index.html#documentation

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


def serialize2str(name, i):
    if isinstance(i, Variable):
        return '<' + name + ' type="var" val="' + i.__str__() + '"/>'
    elif isinstance(i, URIRef):
        # return '<' + name + ' type="iri" val="' + i.__str__().replace(
        #     '&', '&amp;') + '"/>'
        return '<' + name + ' type="iri" val="' + i.__str__().replace(
            '&', '&amp;') + '"/>'
    elif isinstance(i, Literal):
        return '<' + name + ' type="lit"><![CDATA[' + i.__str__(
        ) + ']]></' + name + '>'
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
    else:
        node.set('type', 'bnode')
        node.set('val', i.__str__())
    return node

#==================================================


def serializeBGP2str(bgp):
    """
    from rdflib -> string
    """
    #---
    assert bgp is not None
    #---
    ser = '<bgp>\n'
    for (s, p, o) in bgp:
        ser += '<tp>' + serialize2str('s',
                                      s) + serialize2str('p',
                                                         p) + serialize2str('o',
                                                                            o) + '</tp>\n'
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


def unSerialize(i):
    if i.attrib['type'] == 'var':
        return Variable(i.attrib['val'])
    elif i.attrib['type'] == 'iri':
        return URIRef(i.attrib['val'])  # quote(i.attrib['val']) )
    elif i.attrib['type'] == 'lit':
        return Literal(i.text)
    else:
        return BNode(i.attrib['val'])


def unSerializeBGP(bgp):
    """
    from lxml -> rdflib
    """
    nbgp = []
    for tp in bgp:
        nbgp += [(unSerialize(tp[0]), unSerialize(tp[1]), unSerialize(tp[2]))]
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

def valid(bgp):
    #---
    assert bgp is not None
    #---
    ok = True
    for (s, p, o) in bgp:
        ok = (isinstance(s,Variable) or isinstance(s,URIRef) or isinstance(s,BNode)) \
         and (isinstance(o,Variable) or isinstance(o,URIRef) or isinstance(o,BNode) or isinstance(o,Literal)) \
         and ((isinstance(p,Variable) and (count(p,bgp) == 1)) or isinstance(p,URIRef) or isinstance(p,BNode))
        if not (ok):
            break
    return ok

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

def calcPrecisionRecall(BGPref, BGPtst):
  Gref = BGPtoGraph(BGPref)
  ref = len(BGPref)
  tst = len(BGPtst)  
  s = dict()
  m = (0,0,{})
  s[0] = []
  ltp = set()
  for tp in BGPtst:
    if inGraph({tp}, Gref) :
      ltp.add(tp)
      common = (1/tst, 1/ref, {tp})
      m = max(m, common)
      s[0].append( common )
  pprint(s[0])
  for l in range(1,len(ltp)):
    s[l] = []
    for tp in ltp:
      for (p,r,x) in s[l-1] :
        if tp not in x:
          ns = x.copy()
          ns.add(tp)
          if inGraph(ns, Gref):
            cm = len(ns)
            common = (cm/tst, cm/ref,ns)
            m = max(m, common)
            if common not in s[l]: s[l].append( common )
    print('pour ',l)
    pprint(s[l])
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

    g6 = nx.MultiDiGraph()
    g6.add_edges_from([ (1,2,dict(prop='type')), 
                        (1,3,dict(prop='manage')),
                        #(1,4,dict(prop='manage')),
                        (1,3,dict(prop='knows')),
                        (3,2,dict(prop='type')),
                        (4,2,dict(prop='type'))  
                      ])
    print('g6')
    for e in g6.edges(data=True):
        pprint(e)

    g7 = nx.MultiDiGraph()
    g7.add_edges_from([ (5,6,dict(prop='type')), 
                        (5,7,dict(prop='knows')),
                        (7,6,dict(prop='type')),
                        (8,6,dict(prop='type')),
                        (9,6,dict(prop='type')),
                        (5,10,dict(prop='bP'))  
                      ])
    print('g7')
    for e in g7.edges(data=True):
        pprint(e)

    map = isSGO(g6,g7)
    if map is not None: print('g6 in g7 : ', map)
    else: print('g6 not in g7') 
    # ne répond pas bien avec : (1,3,dict(prop='manage')) -> g6 in g7 :  {5: 1, 6: 2, 7: 3, 8: 4}
    # mais bonne réponse si remplacé par : (1,4,dict(prop='manage')) -> g6 not in g7

    map = isSGO(g7,g6)
    if map is not None: print('g7 in g6 : ', map)
    else: print('g7 not in g6')

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
    
    # qm = QueryManager(modeStat = False)
    # (BGPSet4, _) = qm.extractBGP(query4)
    # (BGPSet5, _) = qm.extractBGP(query5)
    # print(calcPrecisionRecall(BGPSet4,BGPSet5))


