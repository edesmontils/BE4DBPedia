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
from rdflib import Graph, Literal, BNode, Namespace, RDF, URIRef, Variable
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.algebra import translateQuery, pprintAlgebra
from rdflib.compare import to_canonical_graph

import networkx as nx
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

    """
    #assert isinstance(gp, Iterable)
    g = Graph()
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


def parse(tt, n, ind=''):
    print(ind, '--', tt, '--')
    if tt in [
            '_vars', 'start', 'length', 'PV', 'datasetClause', 'arg', 'op',
            'Literal', 'other', 'expr', 'lazy', 'var', 'A', 'template', 'term',
            'res'
    ]:
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
    if (tt in [
            '_vars', 'start', 'length', 'PV', 'datasetClause', 'arg', 'op',
            'Literal', 'other', 'expr', 'lazy', 'var', 'A', 'template', 'term',
            'res'
    ]):
        return []
    elif tt == 'part':
        s = []
        for k in n:
            s += _getBGP('', k)
        return s
    elif tt == 'triples':
        return n
    elif (n.name == 'BGP'):
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
            # print(i)
            raise ValueError(i)
    else:
        return i


def getBGP(n):
    # return _getBGP('root',n)
    bgp = _getBGP('root', n)
    nbgp = []
    # pprint(bgp)
    for (s, p, o) in bgp:
        # print(s,p,o)
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
    n = 0
    for (s, p, o) in bgp:
        if (s == q) or (p == q) or (o == q):
            n += 1
    return n


#==================================================


def valid(bgp):
    ok = True
    for (s, p, o) in bgp:
        ok = (
            isinstance(
                s,
                Variable) or isinstance(
                s,
                URIRef) or isinstance(
                s,
                BNode)) and (
                    isinstance(
                        o,
                        Variable) or isinstance(
                            o,
                            URIRef) or isinstance(
                                o,
                                BNode) or isinstance(
                                    o,
                                    Literal)) and (
                                        (isinstance(
                                            p,
                                            Variable) and (
                                                count(
                                                    p,
                                                    bgp) == 1)) or isinstance(
                                                        p,
                                                        URIRef) or isinstance(
                                                            p,
                                            BNode))
        if not (ok):
            break
    return ok


#==================================================


def nm(n1, n2):
    t1 = n1['type']
    t2 = n2['type']
    if (isinstance(t1, URIRef) and isinstance(t2, URIRef)):
        ok = t1 == t2
    elif (isinstance(t1, Variable) and isinstance(t2, Variable)):
        ok = True
    elif (isinstance(t1, Literal) and isinstance(t2, Literal)):
        ok = t1 == t2
    else:
        ok = False
    #print ('nm:',ok,'(',t1,'%%',t2,')')
    return ok


#==================================================


def em2(e1, e2):
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
    #print ('em2:', ok)
    #print ('\t',e1)
    #print ('\t',e2)
    return ok


#==================================================

def toRDFLibGraph(bgp):
  g = Graph()
  for (s, p, o) in bgp:
    g.add((s, p, o))
  return g 


#==================================================


def BGPtoGraph(bgp):
    g = nx.MultiDiGraph()
    for (s, p, o) in bgp:
        if not (s in g):
            g.add_node(s, type=s)
        if not (o in g):
            g.add_node(o, type=o)
        if isinstance(p, Variable):
            g.add_edge(s, o, prop='?Var')
        else:
            g.add_edge(s, o, prop=p.__str__())
    return g


#==================================================


def equals(g1, g2):
    return nx.isomorphism.GraphMatcher(
        g1, g2, node_match=nm, edge_match=em2).is_isomorphic()


#==================================================
#==================================================
#==================================================

if __name__ == "__main__":
    print("main")
