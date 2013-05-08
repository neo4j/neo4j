Neo4j - The World's Leading Graph Database
==========================================

*Neo4j is a high-performance, NOSQL graph database with all the features of a mature and robust database.* The programmer works with an object-oriented, flexible network structure rather than with strict and static tables — yet enjoys all the benefits of a fully transactional, enterprise-strength database. For many applications, Neo4j offers performance improvements on the order of 1000x or more compared to relational DBs.

Neo4j is an open source (you're reading it) project available in a GPLv3 Community edition, with Advanced and Enterprise editions available under both the AGPLv3 and commercial licenses, supported by [Neo Technology](http://neotechnology.com/).

More about [Neo4j](http://neo4j.org/).

Using Neo4j
-----------

Neo4j is available both as a standalone server, or an embedded component.  You can [download](http://neo4j.org/download/) or [try online](http://console.neo4j.org/).

We also supply commercial licenses. Please contact [Neo Technology](sales@neotechnology.com) for more information.

Extending Neo4j
---------------

We encourage everyone to experiment with Neo4j. You can build extensions to Neo4j, or make contributions to the product itself.  Please note that you'll need to sign a Contributor License Agreement in order for us to accept your patches.

*Please note* that this GitHub repository contains mixed GPL and AGPL code.  Our community edition (in the community/ directory) is GPL. Our advanced and enterprise editions (advanced/ and enterprise/, you get the drill) are Affero GPL and so have different licensing implications.

Building Neo4j
--------------

Neo4j is built using [Apache Maven](http://maven.apache.org/) version 3.

A plain `mvn clean install` will build the individual jar files, but not assemble them into product packages.
The documentation won't get built either, nor will the Python embedded bindings.
Execution of unit test is included in the build.

To add execution of integration tests, use: `mvn clean install -DrunITs`

In case you just want the jars, not even compiling the tests, this is for you: `mvn clean install -DminimalBuild`

To build everything, including running all tests, producing documentation and assembling product packages, use `mvn clean install -DfullBuild`

The Python part of the build requires a working JPype installation.
For how to make the documentation build go, see: [manual/](https://github.com/neo4j/neo4j/tree/master/manual)

When building on Windows, use `-Dlicensing.skip` to avoid problems related to line endings.

When building code from a different year, you are likely to see failures due to outdated license headers.
The license header check can be skipped by appending the following to the command line: `-Dlicense.skip=true`

For further details on building, please consult community/README.md.

We also supply commercial licenses. Please contact [Neo Technology](mailto:sales@neotechnology.com) for more information.
