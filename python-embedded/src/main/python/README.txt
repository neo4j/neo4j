Python bindings for embedded Neo4j
==================================

These are Python bindings for the embedded Neo4j Graph Database.

Prerequisites
-------------

The neo4j embedded database is a java application, which means you have to provide an interface to communicate with java-land. You can use:

 - CPython with JPype installed http://jpype.sourceforge.net/
 - [Coming in the next few days] Jython http://www.jython.org/
 

Installation
------------

Check that you have the prerequisites, then just:

    python setup.py install
