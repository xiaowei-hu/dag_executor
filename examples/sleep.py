#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import time

import dag_executor as de

logging.basicConfig(level=logging.INFO)


# 初始化资源
op_manager = de.OpManager()
op_manager.add(de.Op('sleep_3_op', lambda x, y: logging.info(
    'Sleep 3s. ' + str(time.sleep(3)))))
op_manager.add(de.Op('sleep_5_op', lambda x, y: logging.info(
    'Sleep 5s. ' + str(time.sleep(5)))))
serial_engine = de.Engine(1)
parallel_engine = de.Engine()


# 运行时
tic = time.time()
sleep_node_a = de.Node('sleep_node_a', op_manager.get('sleep_5_op'))
sleep_node_b = de.Node('sleep_node_b', op_manager.get('sleep_3_op'))
sleep_node_c = de.Node('sleep_node_c', op_manager.get('sleep_3_op'))
g = de.Graph()
g.add_nodes_from([sleep_node_a, sleep_node_b, sleep_node_c])
g.add_edges_from([(sleep_node_b.name, sleep_node_c.name)])
g.froze()
print('build graph time: %s' % str(time.time() - tic))

tic = time.time()
print(serial_engine.eval(g))
print('run graph time with serial_engine: %s' % str(time.time() - tic))

tic = time.time()
print(parallel_engine.eval(g))
print('run graph time with parallel_engine: %s' % str(time.time() - tic))
