Neo4j Stand-alone Project
=========================

This project assembles the Neo4j stand-alone distribution, pulling together all
the deliverable artifacts and packaging them into a downloadable installer.

Building
--------

Running `mvn clean package` will produce packages for Windows
and Linux. Look in the target/ directory.


Directories
-----------

* `src/main/assemblies` - maven-assembly-plugin descriptors
* `src/main/distribution/text` - distributable text files
  * contents should be filtered and processed for the destination platform
* `src/main/distribution/shell-scripts` - distributable script files
  * contents should be filtered and processed for the destination platform
  * file mode of some files will be changed to execute

Note that the "text" and "shell-scripts" directories should be identical
in structure, differing only in content. Each of these subdirecories are merged
into the final output, but undergo different processing.
