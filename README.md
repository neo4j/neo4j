Neo4j - The World's Leading Graph Database
==========================================

Neo4j is the world's leading Graph Database. It is a high performance graph store with all the features expected of a mature and robust database, like a friendly query language and ACID transactions. The programmer works with a flexible network structure of nodes and relationships rather than static tables — yet enjoys all the benefits of enterprise-quality database. For many applications, Neo4j offers orders of magnitude performance benefits compared to relational DBs.

Neo4j is an open source product available in a GPLv3 Community edition, with Advanced and Enterprise editions available under both the AGPLv3 and commercial licenses, supported by [Neo Technology](http://neotechnology.com/).

Read more on the [Neo4j website](http://neo4j.org/).

Using Neo4j
-----------

Neo4j is available both as a standalone server, or an embeddable component. You can [download](http://neo4j.org/download/) or [try online](http://console.neo4j.org/).

Extending Neo4j
---------------

We encourage experimentation with Neo4j. You can build extensions to Neo4j, develop library or drivers atop the product, or make contributions directly to the product core. You'll need to sign a Contributor License Agreement in order for us to accept your patches.

*Please note* that this GitHub repository contains mixed GPL and AGPL code. Our Community edition (in the [community/](community/) directory) is GPL. Our Advanced and Enterprise editions ([advanced/](advanced/) and [enterprise/](enterprise/)) are differently licensed under the AGPL.

Building Neo4j
--------------

Neo4j is built using [Apache Maven](http://maven.apache.org/) version 3.

o A plain `mvn clean install` will only build the individual jar files. 

o Test execution is, of course, part of the build.

o To add execution of integration tests, use: `mvn clean install -DrunITs`

o In case you just want the jars, without compiling the tests, this is for you: `mvn clean install -DminimalBuild`

o To build everything, including running all tests, producing documentation and assembling product packages, use `mvn clean install -DfullBuild`

o To build the documentation see the [Neo4j manual](manual)

o When building on Windows, use `-Dlicensing.skip` to avoid problems related to line endings.

o The license header check can be skipped by appending the following to the command line: `-Dlicense.skip=true`

For further details on building, please consult the [community docs](community/README.md).

Licensing
---------

Neo4j can be commercially licensed for non-open source deployments. Please contact [Neo Technology](mailto:sales@neotechnology.com) for more information.
