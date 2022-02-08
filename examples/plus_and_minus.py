#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import time
from collections import OrderedDict

import dag_executor as de

logging.basicConfig(level=logging.INFO)


# 初始化资源
def plus(name, data, conf):
    l = data[conf['l']]
    r = data[conf['r']]
    data[name + '_res'] = l + r
    time.sleep(3)


def minus(name, data, conf):
    l = data[conf['l']]
    r = data[conf['r']]
    data[name + '_res'] = l - r
    time.sleep(3)


op_manager = de.OpManager()
op_manager.add(de.Op('plus_op', plus))
op_manager.add(de.Op('minus_op', minus))
serial_engine = de.Engine(1)
parallel_engine = de.Engine()


# 运行时
tic = time.time()
plus_node_m = de.Node('plus_node_m', op_manager.get(
    'plus_op'), {'l': 'a', 'r': 'b'})
plus_node_n = de.Node('plus_node_n', op_manager.get(
    'plus_op'), {'l': 'c', 'r': 'd'})
minus_node = de.Node('minus_node', op_manager.get(
    'minus_op'), {'l': 'plus_node_m_res', 'r': 'plus_node_n_res'})
g = de.Graph()
g.add_nodes_from([plus_node_m, plus_node_n, minus_node])
g.add_edges_from([(plus_node_m.name, minus_node.name),
                 (plus_node_n.name, minus_node.name)])
g.froze()  # compute '($a + $b) - ($c + $d)'
print('build graph time: %s' % str(time.time() - tic))

# first run
data = OrderedDict({'a': 1, 'b': 2, 'c': 3, 'd': 4})
tic = time.time()
print(serial_engine.eval(g, data))
print('run graph time with serial_engine: %s' % str(time.time() - tic))

tic = time.time()
print(parallel_engine.eval(g, data))
print('run graph time with parallel_engine: %s' % str(time.time() - tic))

# second run
data = OrderedDict({'a': 4, 'b': 3, 'c': 2, 'd': 1})
tic = time.time()
print(serial_engine.eval(g, data))
print('run graph time with serial_engine: %s' % str(time.time() - tic))

tic = time.time()
print(parallel_engine.eval(g, data))
print('run graph time with parallel_engine: %s' % str(time.time() - tic))
