#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from collections import OrderedDict


class Op(object):
    def __init__(self, name, func):
        self.name = name
        self.func = func


start_op = Op('start_op', lambda x, y: logging.info('Start!'))
finish_op = Op('finish_op', lambda x, y: logging.info('Finish!'))
join_op = Op('join_op', lambda x, y: logging.info(
    'Join %s!' % str(y.get('node'))))


class OpManager(object):
    def __init__(self):
        self.op_collection = OrderedDict({
            'start_op': start_op, 'finish_op': finish_op})

    def add(self, op):
        assert isinstance(op, Op), 'Input op is not Op class.'
        assert not op.name in self.op_collection, 'Op name has been in manager.'
        self.op_collection[op.name] = op
        return True

    def get(self, name):
        op = self.op_collection.get(name)
        assert op != None, 'Get None with op name: %s.' % name
        return op

    def list(self):
        return list(self.op_collection.keys())


if __name__ == '__main__':
    op_manager = OpManager()
    print(op_manager.list())
    print(op_manager.get('start_op').__dict__)
    print(op_manager.get('finish_op').__dict__)
