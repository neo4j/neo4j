#Neo4j Graph Database

For more information, visit:
http://neo4j.org/

# Building
Neo4j is built using Maven - http://maven.apache.org/

*Maven 3 is now supported*


Requirements
============

At least as of Neo4j 1.7 and OpenJDK 1.7.0_147-icedtea,
[OpenJDK 7 is unable to build Neo4j](https://groups.google.com/group/neo4j/msg/e208be9ee1c101d7) with
following error: `java.lang.AssertionError: Missing type variable in where clause T`.
Please use JDK 6 (OpenJDK is fine) to build Neo4j.

In addition, you need to allow enough memory to the Maven build process,
for example by adding before launching Maven:

    export JAVA_OPTS='-Xms384M -Xmx512M -XX:MaxPermSize=256M'

At least on OpenJDK 1.6.0_23, building Neo4j with the default settings
throws misleading exceptions such as: (reproduced here for your information)

    [ERROR] Specifications.java:[178,33] cannot find symbol
    symbol  : method and(java.lang.Iterable<java.lang.Object>)
    location: class org.neo4j.helpers.Specifications
    [ERROR] Iterables.java:[342,63] <FROM,TO>map(
		org.neo4j.helpers.Function<? super FROM,? extends TO>,
		java.lang.Iterable<FROM>) in org.neo4j.helpers.collection.Iterables
		cannot be applied to (<anonymous org.neo4j.helpers.Function<java.lang.Iterable<T>,
		java.util.Iterator<T>>>,java.lang.Iterable<java.lang.Object>)


Build Steps Community:
======================

    git clone git://github.com/neo4j/community.git
    mvn clean install


To build all of the Neo4j Distribution
======================================


These instructions are for OS X, with https://github.com/mxcl/homebrew installed.

prepare the repo:

    rm -rf ~/.m2/repository/org/neo4j
    mkdir build
    cd build

build community:

    git clone git@github.com:neo4j/community.git
    cd community
    git pull origin master
    mvn clean install -Dmaven.test.skip=true
    cd ..

build advanced

    git clone git@github.com:neo4j/advanced.git
    cd advanced
    git pull origin master
    mvn clean install -Dmaven.test.skip=true
    cd ..

build enterprise

    git clone git@github.com:neo4j/enterprise.git
    cd enterprise
    git pull origin master
    mvn clean install -Dmaven.test.skip=true
    cd ..

install the ASCIIDOC toolchain

    brew install docbook asciidoc w3m fop graphviz && sudo docbook-register

build the manual

    git clone git@github.com:neo4j/manual.git
    cd manual
    git pull origin master
    mvn clean install -Dmaven.test.skip=true
    cd ..

Build the standalone distributions

    #git clone git@github.com:neo4j/packaging.git
    cd packaging
    git pull origin master
    cd standalone
    mvn clean package


Working with the source code in Eclipse IDE
===========================================

Have a look at the readme file in cypher/ for how to get that project to work in Eclipse IDE.
If it isn't setup properly, other projects (submodules) that depend on it will not get built by Eclipse.

