#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='kafka-to-hbase',
    version='1.0',
    description='Simple Kafka to Hbase importer',
    author='Craig Andrews',
    author_email='craig.andrews@infinityworks.com',
    url='https://github.com/dwp/kafka-to-python',
    packages=find_packages(),
    scripts=['scripts/kafka-to-hbase'],
    install_requires=[
        'happybase==1.2.0',
        'kafka-python==1.4.6',
    ],
    extras_require={
        'dev': [
            'docker-compose==1.24.0',
            'invoke==1.2.0',
            'pytest==4.6.3',
        ]
    }
)
