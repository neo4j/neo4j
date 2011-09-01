Python bindings for embedded Neo4j
==================================

These are Python bindings for the embedded Neo4j Graph Database.

Prerequisites
-------------

The neo4j embedded database is a java application, which means you have to provide an interface to communicate with java-land. You need:

 - CPython with JPype installed http://jpype.sourceforge.net/

Installation from source
------------------------

This is the source distribution of this project. To install into a python environment, use the maven build tool to produce the python distribution, then install the distribution normally:

::
    mvn package
    unzip target/neo4j-python-embedded-[VERSION]-    python-dist.zip
    cd target/neo4j-embedded
    python setup.py install

