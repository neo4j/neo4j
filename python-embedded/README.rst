Python bindings for embedded Neo4j
==================================

These are Python bindings for the embedded Neo4j Graph Database.

Prerequisites
-------------

The neo4j embedded database is a java application, which means you have two options for using these bindings:

 - Use Jython http://www.jython.org/
 - Use Python with JPype installed http://jpype.sourceforge.net/

Installation from source
------------------------

This is the source distribution of this project. To install into a python environment, use the maven build tool to produce the python distribution:

    mvn package
    
You will find the finished package in target/neo4j-python-embedded-[VERSION]-python-dist

See the README bundled with the produced distribution for next installation steps.

