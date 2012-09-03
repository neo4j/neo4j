#!/usr/bin/python
from setuptools import setup, find_packages

setup(
    name='sauron',
    version='0.0.1',
    author='Jacob Hansson',
    author_email='jakewins@gmail.com',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'py-trello',
        'docopt',
        'oauth2',
        'config==0.3.7',
    ],
    
    entry_points = {
        'console_scripts': [
            'sauron = sauron.__main__:main',
        ],
    },
    
    url='http://github.com/neo4j/community',
    description='One sauron to bring them all, and in the darkness bind them.',
    long_description='',
)