Python bindings for embedded Neo4j
==================================

These are Python bindings for the embedded Neo4j Graph Database.

Source: https://github.com/neo4j/python-embedded

Prerequisites
-------------

The neo4j embedded database is a java application, which means you have to provide an interface to communicate with java-land. You can use:

 - CPython with JPype installed http://jpype.sourceforge.net/
 

Installation
------------

Check that you have the prerequisites, then just::

  pip install neo4j-embedded
    
Or, if you prefer the manual download::

  python setup.py install
  

Versions
--------

The version number used for neo4j-embedded matches the version of neo4j it comes shipped with. The following examples should serve as a guide to determine what neo4j version is under the hood:

1.5 : 
  Neo4j version 1.5
1.5.dev1 :
  Neo4j version 1.5-SNAPSHOT
1.5.b1 :
  Neo4j version 1.5.M01
1.5.r1 :
  Neo4j version 1.5.RC1
  
