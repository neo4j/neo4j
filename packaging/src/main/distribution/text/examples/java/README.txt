Neo4j in Java
==================

This package provides an easy-to-get-going distribution of the Neo4j graph
database, with a collection of essential components and helpful examples. 
These components are the core of any application needing to model, persist
and explore arbitrarily complex data. 

Included in this release of Neo4j are:

   neo4j-kernel          the neo4j graph database engine
   neo4j-index           indexing and data structures
   neo4j-remote-graphdb  thin layer to enable remote access to a neo4j instance
   neo4j-shell           command shell for browsing and manipulating the graph
   neo4j-online-backup   create backups of a running neo4j graph database
   neo4j-graph-algo      graph algorithms (such as shortest path algorithm)
   neo4j-udc             usage data collector
   neo4j-lucene-index    integrated index implementation

The lib/ directory contains the binary jar files for all components as well as 
required third-party libraries. Javadocs are in the site/apidocs directory.

The included usage data collector component will send basic information like     
for example version number to udc.neo4j.org once a day and can easily be disabled,
see more information on the wiki:                                                 
http://wiki.neo4j.org/content/UDC


Getting started
---------------

For a quick introduction try the Neo4j Shell, an interactive environment for 
exploring and manipulating a graph. Check the bin/ directory for Unix and 
Windows start scripts. The `shell-example` script starts a Neo4j Shell with
example data loaded.

Try this:

1. run the `shell-example` script
2. type 'l' to start a local shell instance
3. welcome to the graph, type 'ls' to see the current node
4. 'cd 1' to jump to the related node with id '1'
5. 'cd user96' then hit tab, for tab completion (and then return)
6. 'ls -p' to list just the properties of the current node
7. 'cd' by itself will return to the reference node (your starting point)
8. when in doubt, type 'help' to find other commands

For more details, refer to the wiki at http://wiki.neo4j.org/content/Shell,
or follow the steps at http://wiki.neo4j.org/content/Shell_Matrix_Example.

To start a normal Neo4j Shell (without example data) use the neo4j-shell script.


Example code
------------

You'll find some source code in the examples/ directory. These examples 
progress from setting up a graph, to indexing, to using algorithms. 
Scripts in the bin/ directory run the source code against pre-compiled 
classes in the lib/ directory.

The scripts and related source code are:

  * embedded-neo4j                org.neo4j.examples.EmbeddedNeo4j 
  * embedded-neo4j-with-indexing  org.neo4j.examples.EmbeddedNeo4jWithIndexing 
  * calculate-shortest-path       org.neo4j.examples.CalculateShortestPath


Learn more
----------

For documentation, see also:

  * http://wiki.neo4j.org/        comprehensive documentation                
  * http://components.neo4j.org/  documentation about each component


License(s)
----------
Various licenses apply. Please refer to the LICENSE and NOTICE files for more
detailed information. A full report regarding the licenses of all included
dependencies is found in site/dependencies.html.
