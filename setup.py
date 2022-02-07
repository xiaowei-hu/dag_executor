# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

try:
    long_description = open("README.md").read()
except IOError:
    long_description = ""

setup(
    name="dag_executor",
    version="0.1.0",
    description="a tiny DAG execution engine.",
    license="MIT",
    author="huxiaowei",
    author_email="xiaowei.hu@outlook.com",
    packages=find_packages(),
    install_requires=["networkx~=2.6.3",
                      "gevent~=1.4.0"],
    long_description=long_description,
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
    ]
)
