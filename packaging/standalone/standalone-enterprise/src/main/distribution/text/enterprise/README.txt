#{product.fullname} #{neo4j.version}
=======================================

Welcome to #{product.fullname} release #{neo4j.version}, a high-performance graph database.
This is the enterprise distribution of #{product.fullname}, including everything you need to
start building applications that can model, persist and explore graph-like data.

In the box
----------

#{product.fullname} runs as a server application, exposing a Web-based management
interface and RESTful endpoints for data access, along with logging, capabilities
for participating in a database cluster and JMX remote monitoring.

Here in the installation directory, you'll find:

* bin - scripts and other executables
* conf - server configuration
* data - database, log, and other variable files
* lib - core libraries
* plugins - user extensions
* system - super-secret server stuff

Make it go
----------

For full instructions, see #{getting-started-server.url}

To get started with #{product.fullname}, let's start the server and take a
look at the web interface ...

1. Open a console and navigate to the install directory.
2. Start the server:
   * Windows, use: bin\Neo4j.bat
   * Linux/Mac, use: ./bin/neo4j console
3. In a browser, open http://localhost:#{org.neo4j.webserver.port}/
4. From any REST client or browser, open http://localhost:#{org.neo4j.webserver.port}/db/data
   in order to get a REST starting point, e.g.
   curl -v http://localhost:#{org.neo4j.webserver.port}/db/data
5. Shutdown the server by typing Ctrl-C in the console.

Learn more
----------

* #{neo4j-home.url.title}: #{neo4j-home.url}
* #{getting-started.url.title}: #{getting-started.url}
* #{neo4j-manual.title}: #{neo4j-manual.url}

License(s)
----------
Various licenses apply. Please refer to the LICENSE and NOTICE files for more
detailed information.
