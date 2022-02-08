#!/usr/bin/env python
# -*- coding: utf-8 -*-
import copy
from collections import OrderedDict

import networkx as nx

from .op_manager import Op, finish_op, start_op


class Node(object):
    def __init__(self, name, op, conf=None):
        assert isinstance(op, Op), 'Input op is not Op class.'
        self.name = name
        self.op = op
        self.conf = conf


start_node = Node('start_node', start_op)
finish_node = Node('finish_node', finish_op)


class Graph(nx.DiGraph):
    def __init__(self, name='default'):
        self.node_collection = OrderedDict()
        self.tr = None
        self._frozen = False
        super().__init__(name=name)

    # 添加node
    def add_node(self, node):
        assert self._frozen == False, 'Graph has _frozen, when add node.'
        assert isinstance(node, Node), 'Input node is not Node class.'
        assert not node.name in self.node_collection, 'Node name has been in graph.'
        super().add_node(node.name)
        self.node_collection[node.name] = node
        return node.name

    def add_nodes_from(self, nodes):
        return [self.add_node(node) for node in nodes]

    # 添加edge
    def add_edge(self, node_name_a, node_name_b):
        assert self._frozen == False, 'Graph has _frozen, when add edge.'
        super().add_edge(node_name_a, node_name_b)

    def add_edges_from(self, edges):
        [self.add_edge(*edge) for edge in edges]

    # 全部父节点
    def ancestors(self, node_name):
        if self._frozen:
            return nx.ancestors(self.tr, node_name)
        else:
            return nx.ancestors(self, node_name)

    # 相邻父节点
    def in_nodes(self, node_name):
        if self._frozen:
            in_edges = self.tr.in_edges(node_name)
        else:
            in_edges = self.in_edges(node_name)
        return [edge[0] for edge in in_edges]

    # 相邻子节点
    def out_nodes(self, node_name):
        if self._frozen:
            out_edges = self.tr.out_edges(node_name)
        else:
            out_edges = self.out_edges(node_name)
        return [edge[1] for edge in out_edges]

    # 获取node对象
    def get_node(self, node_name):
        node = self.node_collection.get(node_name)
        assert node != None, 'Get None with node name: %s.' % node_name
        return node

    # 是否dag
    def is_dag(self):
        return nx.is_directed_acyclic_graph(self)

    # 冻结graph
    def froze(self):
        node_names = copy.deepcopy(self.nodes)
        self.add_nodes_from([start_node, finish_node])
        for node_name in node_names:
            self.add_edge(start_node.name, node_name)
            self.add_edge(node_name, finish_node.name)
        assert self.is_dag(), 'Graph is not a dag.'
        self.tr = nx.transitive_reduction(self)
        self._frozen = True

    @property
    def frozen(self):
        return self._frozen

    # 禁用method
    def add_weighted_edges_from(self):
        assert False, 'No method.'

    def remove_node(self):
        assert False, 'No method.'

    def remove_nodes_from(self):
        assert False, 'No method.'

    def remove_edge(self):
        assert False, 'No method.'

    def remove_edges_from(self):
        assert False, 'No method.'

    def clear_edges(self):
        assert False, 'No method.'

    def clear(self):
        assert False, 'No method.'

    def update(self):
        assert False, 'No method.'

    def copy(self):
        assert False, 'No method.'

    def reverse(self):
        assert False, 'No method.'

    def to_directed(self):
        assert False, 'No method.'

    def to_undirected(self):
        assert False, 'No method.'


if __name__ == '__main__':
    import logging
    import time
    g = Graph()
    print(id(g))
    print(g)
    print(g.__dict__)

    sleep_3_op = Op('sleep_3_op', lambda x, y, z: logging.info(
        'Sleep 3s. ' + str(time.sleep(3))))
    sleep_5_op = Op('sleep_5_op', lambda x, y, z: logging.info(
        'Sleep 5s. ' + str(time.sleep(5))))
    sleep_node_a = Node('sleep_node_a', sleep_5_op)
    sleep_node_b = Node('sleep_node_b', sleep_3_op)
    sleep_node_c = Node('sleep_node_c', sleep_3_op)
    g.add_nodes_from([sleep_node_a, sleep_node_b, sleep_node_c])
    g.add_edges_from([(sleep_node_b.name, sleep_node_c.name)])
    g.froze()
    print(g.frozen)

    print(g.nodes)
    print(g.edges)
    print(g.tr.edges)

    for node_name in g.nodes:
        print(g.get_node(node_name).__dict__)

    print(id(g))
    print(g)
    print(g.__dict__)

    print(g.ancestors('finish_node'))
    print(g.in_nodes('finish_node'))
    print(g.out_nodes('start_node'))
