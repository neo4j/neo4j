Neo4j #{neo4j.version}
=======================================

Welcome to Neo4j release #{neo4j.version}, a high-performance graph database.
This is the community distribution of Neo4j, including everything you need to
start building applications that can model, persist and explore graph-like data.

In the box
----------

Neo4j runs as a server application, exposing a Web-based management interface.

Here in the installation directory, you'll find:

* bin - scripts and other executables
* conf - server configuration
* data - databases
* lib - libraries
* plugins - user extensions
* logs - log files
* import - location of files for LOAD CSV

Installing and running Neo4j
----------------------------

For full instructions, see https://neo4j.com/docs/operations-manual/current/installation/

To get started with Neo4j:

1. Open a console and navigate to the installation directory.
2. Start the server:
   * Windows, use: bin\neo4j-admin server console
   * Linux/Mac, use: ./bin/neo4j-admin server console
3. Shutdown the server by typing Ctrl-C in the console.

Accessing Neo4j graph tools
---------------------------

You can access Neo4j graph tools for querying, exploring, and visualizing data by going to - no subscription required.

1. Sign up or log in to https://console-preview.neo4j.io/self-managed
2. Register a self-managed instance and connect to http://localhost:7474 using the default password which is documented in installation instructions: https://neo4j.com/docs/operations-manual/current/installation/

Alternatively, you can navigate to http://localhost:7474 in your local browser to access the Query application.

Learn more
----------

* Neo4j: https://neo4j.com/
* Getting Started: https://neo4j.com/docs/getting-started/
* Neo4j Documentation: https://neo4j.com/docs/

License(s)
----------
Various licenses apply. Please refer to the LICENSE and NOTICE files for more
detailed information.
