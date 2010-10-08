Neo4j with Basic Examples

The purpose of this package is to provide an easy-to-get-going distribution
of the Neo4j graph database and a handful of commonly used components. It
includes the binary jar files for these components as well as example code
that shows basic usage.

Included in this release of Neo4j is:

   neo4j-kernel:         the neo4j graph database engine
   neo4j-index:          indexing and data structures
   neo4j-remote-graphdb: thin layer to enable remote access to a neo4j instance
   neo4j-shell:          text command shell for browsing the graph
   neo4j-online-backup:  create backups of a running neo4j graph database
   neo4j-graph-algo:     graph algorithms (such as shortest path algorithm)
   neo4j-udc:            usage data collection
   
You can run the examples from the Unix and Windows start scripts in the bin/
directory. The shell-client script starts a neo4j-shell instance.

You'll find the source code to the examples in the examples/ directory.

The components are found in the lib/ directory, and the javadocs for the
included components are found in the site/apidocs directory.

For documentation, see also:
http://components.neo4j.org/
http://wiki.neo4j.org

Various licenses apply. Please refer to the LICENSE and NOTICE files for more
detailed information. A full report regarding the licenses of all included
dependencies is found in site/dependencies.html.
