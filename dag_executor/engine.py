#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import time
from collections import OrderedDict
from enum import Enum

import gevent
from gevent import pool

from .graph import Graph


def func_wraper(name, func, data, conf):
    tic = time.time()
    logging.info('Node ' + name + ' start.')
    result = func(data, conf)
    toc = time.time()
    logging.info('Node ' + name + ' finish. It took %s secs.' % str(toc - tic))
    return result


class RunStatus(Enum):
    PENDING = 0
    START = 1
    FINISH = 2
    ERROR = -1


class Engine(object):
    def __init__(self, pool_size=100):
        self.p = pool.Pool(pool_size)

    def submit(self, node, data):
        name = node.name
        func = node.op.func
        conf = node.conf
        return self.p.spawn(func_wraper, name, func, data, conf)

    def eval(self, g, data=OrderedDict(), timeout=None):
        assert isinstance(g, Graph), 'Input g is not Graph class.'
        assert g.frozen == True, 'Graph should be frozen.'
        run_status = dict([(node, RunStatus.PENDING) for node in g.nodes])
        ready = gevent.event.Event()
        ready.clear()

        def update(job):
            # 更新当前job状态
            run_status[job.name] = RunStatus.FINISH if job.successful(
            ) else RunStatus.ERROR
            if job.name == 'finish_node':
                ready.set()
            else:
                # 尝试提交子节点
                for out_node_name in g.out_nodes(job.name):
                    if run_status[out_node_name] == RunStatus.PENDING:
                        # 判断子节点的父节点是否完成
                        completed = True
                        for in_node_name in g.in_nodes(out_node_name):
                            if run_status[in_node_name] in (RunStatus.PENDING, RunStatus.START):
                                completed = False
                                break
                        if completed:
                            out_node = g.get_node(out_node_name)
                            next_job = self.submit(out_node, data)
                            next_job.name = out_node_name
                            next_job.link(update)
                            run_status[out_node_name] = RunStatus.START

        # 提交start
        start_node = g.get_node('start_node')
        start_job = self.submit(start_node, data)
        start_job.name = 'start_node'
        start_job.link(update)
        run_status['start_node'] = RunStatus.START
        ready.wait(timeout=timeout)
        return run_status, data


if __name__ == '__main__':
    from .graph import Node, Op

    tic = time.time()
    g = Graph()
    sleep_3_op = Op('sleep_3_op', lambda x, y: logging.info(
        'Sleep 3s. ' + str(time.sleep(3))))
    sleep_5_op = Op('sleep_5_op', lambda x, y: logging.info(
        'Sleep 5s. ' + str(time.sleep(5))))
    sleep_node_a = Node('sleep_node_a', sleep_5_op)
    sleep_node_b = Node('sleep_node_b', sleep_3_op)
    sleep_node_c = Node('sleep_node_c', sleep_3_op)
    g.add_nodes_from([sleep_node_a, sleep_node_b, sleep_node_c])
    g.add_edges_from([(sleep_node_b.name, sleep_node_c.name)])
    g.froze()
    print('build graph time: %s' % str(time.time() - tic))

    tic = time.time()
    engine = Engine()
    print('build engine time: %s' % str(time.time() - tic))

    tic = time.time()
    print(engine.eval(g))
    print('run graph time: %s' % str(time.time() - tic))
