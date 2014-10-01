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

Running `mvn clean package` will produce packages for Windows
and Linux. Look in the target/ directory.


Directories
-----------

* `src/main/assemblies` - maven-assembly-plugin descriptors
* `src/main/distribution/binary` - distributable binary files
  * contents should be copied with no processing
* `src/main/distribution/text` - distributable text files
  * contents should be filtered and processed for the destination platform
* `src/main/distribution/shell-scripts` - distributable script files
  * contents should be filtered and processed for the destination platform
  * file mode of some files will be changed to execute

Note that the "binary", "text" and "shell-scripts" directories should be identical
in structure, differing only in content. Each of these subdirecories are merged
into the final output, but undergo different processing.


Authoring
---------

Text content that will be packaged up from within this assembly
should be composed in either markdown, restructured-text or some
other very human readable plain text format. Ideally, we'll have
awesome tooling for producing useful output from any of that,
but any source text file here must be presentable without any
processing. 

Except, every text file can use simple property substition for
very common terms defined in the pom.xml (or document.properties
file) which processed by maven.

### Links - file system

Links to other documents within the filesystem can assume a `/`
root that is the top-level directory of the distribution. Filenames
should anticipate the final name of the relevant file. For example,
for a markdown document doc/foo.md to refer to a doc/bar.md, you
could use either of the following forms:

* Within foo, use a relative path to refer to [bar](bar.txt) 
* Or, foo could use an absolute path for [bar](/doc/bar.txt)

### Links - internets

Links out to the internet should endeavor to use "*permalinks*" --
very well known, unlikely to change URLs. These are also good
candidates to be added to the document.properties file. 

