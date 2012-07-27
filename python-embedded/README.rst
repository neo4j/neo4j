Python bindings for embedded Neo4j
==================================

These are Python bindings for the embedded Neo4j Graph Database.

Prerequisites
-------------

The neo4j embedded database is a java application, which means you have to provide an interface to communicate with java-land. You need:

- CPython with JPype installed http://jpype.sourceforge.net/

Documentation
-------------

The documentation contains more detailed help with installation, as well as examples and reference documentation.

See http://docs.neo4j.org/

Installation
------------

::

  pip install neo4j-embedded

Installation from source
------------------------

To install neo4j-embedded from this source tree, use the maven build tool to produce the python distribution, then install the distribution normally:

::

  mvn package
  unzip target/neo4j-python-embedded-[VERSION]-python-dist.zip
  cd neo4j-embedded
  python setup.py install

Releasing
------------------------

Make sure you have a .pypirc file in your home folder with correct login information for the neo4j-embedded package on pypi.python.org. The release builds windows installers, so it needs to run on a windows machine.

::
  
  python release.py

