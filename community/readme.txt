Neo4j Stand-alone Project
=========================

This project assembles the Neo4j stand-alone distribution, pulling together all
the deliverable artifacts and packaging them into a downloadable installer.

Deliverable artifacts included:

* neo4j server - start/stop-able standalone neo4j server
* neo4j shell - text based shell for accessing the server
* neo4j libs - java library files

Building
--------

Running `mvn clean install` will produce packages for Windows
and Linux.


Directories
-----------

* ./src/main/assemblies - maven-assembly-plugin descriptors
* ./src/main/distribution/binary - distributable binary files
  * maps to root output directory (as if `cp -r src/main/binary/* $NEO4J_HOME}`
  * contents should be copied with no processing
* ./src/main/distribution/text - distributable text files
  * maps to root output directory (as if `cp -r src/main/binary/* $NEO4J_HOME}`
  * contents should be filtered and processed for the destination platform
* ./src/main/distribution/shell-scripts - distributable script files
  * maps to root output directory (as if `cp -r src/main/binary/* $NEO4J_HOME}`
  * contents should be filtered and processed for the destination platform
  * file mode of some files will be changed to execute

Note that the "binary", "text" and "shell-scripts" directories should be identical
in structure, differing only in content.


