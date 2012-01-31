#Neo4j Graph Database

For more information, visit:
http://neo4j.org/

# Building
Neo4j is built using Maven - http://maven.apache.org/

*Maven 3 is now supported*

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

