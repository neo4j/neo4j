Neo4j #{neo4j.version}
=======================================

Welcome to Neo4j release #{neo4j.version}, a high-performance graph database.
This is the community distribution of Neo4j, including everything you need to
start building applications that can model, persist and explore graph-like data.

In the box
----------

Neo4j runs as a server application, exposing a Web-based management
interface and RESTful endpoints for data access.

Here in the installation directory, you'll find:

* bin - scripts and other executables
* conf - server configuration
* data - databases
* lib - libraries
* plugins - user extensions
* logs - log files
* import - location of files for LOAD CSV

Make it go
----------

For full instructions, see https://neo4j.com/docs/operations-manual/current/installation/

To get started with Neo4j, let's start the server and take a
look at the web interface ...

1. Open a console and navigate to the install directory.
2. Start the server:
   * Windows, use: bin\neo4j console
   * Linux/Mac, use: ./bin/neo4j console
3. In a browser, open http://localhost:#{default.http.port}/
4. From any REST client or browser, open http://localhost:#{default.http.port}/db/data
   in order to get a REST starting point, e.g.
   curl -v http://localhost:#{default.http.port}/db/data
5. Shutdown the server by typing Ctrl-C in the console.

Learn more
----------

* Neo4j Home: https://neo4j.com/
* Getting Started: https://neo4j.com/docs/developer-manual/current/introduction/
* Neo4j Documentation: https://neo4j.com/docs/

License(s)
----------
Various licenses apply. Please refer to the LICENSE and NOTICE files for more
detailed information.
