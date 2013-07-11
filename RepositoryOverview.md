# Development view
This document provides a quick introduction to the organization of the development process of Neo4j.
It is intended for new developers, to help them get acquainted with the system.

First, the section Module Structure will discuss how Neo4j is comprised of
several components and modules,
which work together to provide Neo4j's functionalities.
Then the organization of the source code is discussed.
This includes an explanation of the directory structure of the source code repository,
instructions on how to build and test the system
and an overview of the configuration management of the source code.

## Module Structure
This model will describe how the source code of Neo4j is split up into smaller pieces 
called components and modules.
Note that some components or modules were skipped, 
because they are deprecated (e.g. `cypher-plugin`) 
or we regard them as not required for the initial understanding of the main system
(e.g. `testing-utils` or the code examples used in the manual).

### Main Components
Neo4j is comprised of several main components.
Each such component is a [maven](http://maven.apache.org/) component and may be comprised of one or more modules.
The main components are:

- *community-build*  
  The main neo4j database, which can be used under a GPL license.
  It corresponds to the community version described [here](http://www.neotechnology.com/price-list/).
  This component can be found in the `community` directory of the source repository.

  If you are looking to fix a bug or add a feature to the community edition,
  this is the place to be.
- *advanced-build*  
  This component adds extra functionality to the community-build and corresponds to the advanced version
  described [here](http://www.neotechnology.com/price-list/).
  The code for the extra functionality can be found in the `advanced` directory of the repository.
- *enterprise-build*  
  This enterprise build adds extra features to the advanced build and corresponds to the enterprise version
  described [here](http://www.neotechnology.com/price-list/).
  The source code for these extra features can be found in the `enterprise` directory of the repository.
- *packaging-build*  
  This component is responsible for packaging all the other components.
  It consists of the `neo4j-server-qa` module, containing tests,
  and the `neo4j-standalone` module, 
  which is responsible for creating standalone installers from the other components.
  If the standalone installers are not working (like discussed [here](https://github.com/neo4j/neo4j/issues/391))
  this component is the place to go.
- *neo4j-manual*  
  This component pulls together the documentation of the other components
  and generates a single manual from it.

  This component is only important if you want to change the main outline of the manual.
  For example, to add a new section or make a new top-level chapter.
  If you are looking to fix or expand existing documentation,
  the specific manual files can be found in the directory of the component that they document.

### Main Modules
![module-overview](docs/images/module-overview.png?raw=true)  
This figure gives an overview of the modules of which the main components are comprised.
The arrows indicate dependencies between modules.
Some modules and their dependencies are colored to help keep crossing arrows apart.

We will now briefly describe the modules and their roles.

#### Community Modules

- *server-api*  
  This module provides classes which can be used to create plugins on the Neo4j server.
  More information about its usage can be found [here](http://docs.neo4j.org/chunked/milestone/server-plugins.html).
- *neo4j-udc*  
  This module is a Usage Data Collector which can gather usage data to help improve Neo4j.
  For more information on what data it gathers and how it can be disabled,
  see [the manual](http://docs.neo4j.org/chunked/milestone/usage-data-collector.html).
- *neo4j-graphviz*  
  The graphviz module is a library that allows visualization of Neo4j graph data 
  using [graphviz](http://www.graphviz.org/).
  For more information about its usage, 
  see [this blogpost](http://blog.neo4j.org/2012/05/graph-this-rendering-your-graph-with.html)
  by Peter Neubauer.
- *neo4j-jmx*  
  The jmx module offers a JMX interface to different Neo4j modules.
  Its details and usage information can be found [here](http://docs.neo4j.org/chunked/milestone/jmx-mxbeans.html).
- *neo4j-graph-algo*  
  This module offers some graph algorithms which can be performed on your graphs.
  They can be called using any of the interfaces to the system, 
  like the REST api as documented [here](http://docs.neo4j.org/chunked/milestone/rest-api-graph-algos.html),
  or the traversal API as documented [here](http://docs.neo4j.org/chunked/milestone/tutorials-java-embedded-graph-algo.html).
  Finally, the graph algorithms are integrated into [the Cypher Query Language](http://docs.neo4j.org/chunked/milestone/cypher-query-lang.html)
  as is illustrated [here](http://docs.neo4j.org/chunked/milestone/query-match.html#match-shortest-path).
- *neo4j-graph-matching*  
  This module provides an [API](http://components.neo4j.org/neo4j-graph-matching/snapshot/apidocs/index.html)
  to perform pattern matching on graphs.
  It is mainly intended for internal use by the Cypher Execution Engine.
  Although its API can be used when using an embedded version of Neo4j,
  this is not recommended.
- *neo4j-lucene-index*
  This module provides indexing capabilities to allow users to lookup nodes based on their properties.
  As described in [the manual](http://docs.neo4j.org/chunked/milestone/indexing.html),
  this way of indexing is no longer recommended if [schema indexing](http://docs.neo4j.org/chunked/milestone/graphdb-neo4j-schema.html) would suffice.
- *neo4j-server*  
  The community version of [the Neo4j server](http://docs.neo4j.org/chunked/milestone/server.html).
  It contains the functionality of the [REST API](http://docs.neo4j.org/chunked/milestone/rest-api.html)
  and [the WebAdmin tool](http://docs.neo4j.org/chunked/milestone/tools-webadmin.html).
- *neo4j/neo4j-community*  
  The neo4j module and the neo4j-community module are pretty much the same.
  The overview only shows the dependencies of neo4j-community,
  but these are the same as those of the neo4j module.
  Both modules are dependencies of other modules and therefore in use.
  They both still exist for historical reasons.
  It is important to note, however, that only the neo4j module contains documentation.

  Both modules are simply a meta-package which allows you to include all its dependencies at once.
- *neo4j-shell*  
  The shell module provides a simple shell to monitor a Neo4j database.
  It is documented [here](http://docs.neo4j.org/chunked/milestone/shell.html)
  and its manpage can be found in the [manpages section](http://docs.neo4j.org/chunked/milestone/manpages.html) of the manual.
- *neo4j-cypher*  
  This module provides the Cypher Execution Engine, which allows users to query using the Cypher Query Language.
  As it is partly written in Scala, 
  working with this module requires some extra setup as discussed [here](community/cypher/README.txt).
- *neo4j-kernel*  
  The kernel is the core of Neo4j.
  It contains the custom storage system, the embedded API of Neo4j and transaction support as listed [here](community/kernel/README.sources.txt).

#### Advanced Modules

- *neo4j-server-advanced*  
  This module contains the extra monitoring features of the advanced version of the Neo4j server.
- *neo4j-advanced*  
  Just like the neo4j-community module, this is simply a meta package to allow easy inclusion of the 
  other maven modules.
  This meta package includes modules from the advanced version and also includes neo4j-community.
- *neo4j-management*  
  The management module extends the neo4j-jmx module with extra monitoring features.
  These features are documented [here](http://docs.neo4j.org/chunked/milestone/operations-monitoring.html).

#### Enterprise Modules

- *neo4j-ha*  
  The high availability (ha) module allows the Neo4j server to be clustered,
  to allow for fault-tolerance and read-scalability as discussed [here](http://docs.neo4j.org/chunked/milestone/ha.html).
- *neo4j-cluster*  
  This module is a library to provide [Heartbeat](http://en.wikipedia.org/wiki/Heartbeat_network)
  and [Paxos][paxoslink] implementations,
  which are used by the high availability cluster.
  [paxoslink]: http://en.wikipedia.org/wiki/Paxos_(computer_science) "Paxos"
- *neo4j-backup*  
  This modules provides the possibility of easily creating backups, even from remote machines.
  The features of this module and its usage are documented [here](http://docs.neo4j.org/chunked/milestone/operations-backup.html)
  and the manpage can be found in the [manpages section](http://docs.neo4j.org/chunked/milestone/manpages.html)
  of the manual.
- *neo4j-com*  
  The communication module supports the communication between the nodes in the high availability cluster.
- *neo4j-consistency-check*  
  This module contains a tool to check the consistency of a Neo4j data store.
  It is used by the backup module.
- *neo4j-server-enterprise*  
  This version of the Neo4j server incorporates the high availability and clustering features into the Neo4j server.
  It also contains all the features of the advanced and community server.
- *neo4j-enterprise*  
  This meta package can be used to easily include a lot of the other modules of Neo4j.

## Codeline Model
This section will cover the codeline organization of Neo4j.
The code is currently hosted on [Github](https://github.com) and is mainly located in
[the Neo4j repository](https://github.com/neo4j/neo4j).

First, an overview of the directory structure of the repository is given.
Then, the build and test approach is discussed.
Finally, the use of git and Github for source code configuration management is discussed.

### Overview of the directory structure
The [main repository](https://github.com/neo4j/neo4j)
reflects the structure of components and modules
as discussed in the Module Structure section.

The top-level directories in the repository contain the main components:

- `community` contains the community-build component
- `advanced` contains the advanced-build component
- `enterprise` contains the enterprise-build component
- `packaging` contains the packaging-build component
- `manual` contains the neo4j-manual component

Inside these component directories, you will find a subdirectory for each module.
For example, the `community/cypher` directory contains the neo4j-cypher module contained in the community component.
The directory names may differ a bit from the module names (cypher versus neo4j-cypher),
but it should not be a problem to figure out where to find a specific module.

Each module is organized according to maven conventions.
So source code can be found in `src/main/java/` for Java code
and `src/main/scala` for Scala code.
Tests are located in the `src/test/java` and `src/test/scala` directories.
More information about the maven conventions can be found [here](http://maven.apache.org/guides/introduction/introduction-to-the-standard-directory-layout.html).

Finally, each module can have documentation.
This documentation is located in the `src/docs` folder,
which is organized as described [here](http://docs.neo4j.org/chunked/milestone/community-docs.html#_file_structure_in_emphasis_docs_jar_emphasis).
These documentation files can be incorporated into [the manual](http://docs.neo4j.org/chunked/milestone/index.html)
by including them in the neo4j-manual component.

### Build, Integration, Test approach 
The source code of Neo4j can be built and tested using [Apache Maven](http://maven.apache.org/),
a build automation tool used primarily for Java projects.

To build from the sources and run the unit tests,
a simple `mvn clean install` in the main repository should suffice.
This will also run the unit tests.
If you don't want to run the unit tests,
add `-DskipTests` to the maven call,
which will skip the execution of the unit tests.
If you don't even want to compile the tests,
use `-Dmaven.test.skip=true` instead.
For more information about building Neo4j,
please consult the [main readme](README.md).
For further instructions on building the manual,
please refer to the [manual component's readme](manual/README.asciidoc).

The test cases are named according to the configuration of the maven [surefire plugin](http://maven.apache.org/surefire/maven-surefire-plugin/).
At the time of writing, this configuration can be found in the [grandparent pom file](http://mvnrepository.com/artifact/org.neo4j.build/grandparent/)
and the following names are allowed (using \* as wildcard for any number of characters):

- Test\*.java
- \*Test.java
- \*Tests.java
- \*TestCase.java

For unit tests related to the documentation there is addition configuration in the
[main repository's pom file](pom.xml),
which allows the following names:

- DocTest\*.java
- \*DocTest.java
- \*DocTests.java
- \*DocTestCase.java

Integration tests are run using the [failsafe plugin](http://maven.apache.org/surefire/maven-failsafe-plugin/).
So these should be named according to the configuration of the failsafe plugin.
At the time of writing the [default configuration](http://maven.apache.org/surefire/maven-failsafe-plugin/examples/inclusion-exclusion.html)
is used.
So please name your integration tests accordingly:

- IT\*.java
- \*IT.java
- \*ITCase.java

Again, there is extra configuration for the test cases related to documentation.
This also allows the following names:

- DocIT\*.java
- \*DocIT.java
- \*DocITCase.java

### Contributing Process
If you want to contribute to the system,
please read [this section](http://docs.neo4j.org/chunked/milestone/community-contributing-code.html)
of the manual, which describes some general guidelines for contributing.
Note that test-driven development (write tests first, code later) is recommended.
Your contribution should adhere to the structure as described
in section Overview of the directory structure.

### Configuration management
[Git](http://git-scm.com/) is used as the version control system for the source code.
Work can be done on different releases at the same time,
as they are located on their own branch.

Configuration files for the [Eclipse](http://www.eclipse.org/)
and [Intellij](http://www.jetbrains.com/idea/)
IDE's can be found [here](code-style).
These files will configure your IDE to use the Neo4j coding style.
There is also a configuration file for [PMD](http://pmd.sourceforge.net/) located [here](code-analysis/pmd.xml),
which can help you find bugs or code smells in your contribution.

