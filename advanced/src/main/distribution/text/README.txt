${server.fullname} ${neo4j.version}
=======================================

Welcome to ${server.fullname} release ${neo4j.version}, a high-performance graph database.
This is the standard distribution of ${server.fullname}, including everything you need to
start building world-class applications that can model, persist and explore arbitrarily
complex data.

In the box
----------

${server.fullname} runs as a server application, exposing a Web-based management
interface and RESTful endpoints for data access, along with logging and JMX

Here in the installation directory, you'll find:

* bin - scripts and other executables
* conf - server configuration
* data - database, log, and other variable files
* doc - more light reading
* examples - real code
* lib - core libraries
* plugins - user extensions
* system - super-secret server stuff

Make it go
----------

To get started with ${server.fullname}, let's start the server and take a
look at the web interface...

1. open a console and navigate to the install directory
2. start the server
  * Windows: use `bin/Neo4j.bat`
  * Linux: use `bin/neo4j start`
3. in a browser, open [webadmin](http://localhost:${org.neo4j.webserver.port}/webadmin/)
4. from any REST client or browser, open (http://localhost:${org.neo4j.webserver.port}/db/data) in order to get a REST starting point, e.g.
   `curl -v http://localhost:${org.neo4j.webserver.port}/db/data`
5. shutdown the server
  * Windows: type Ctrl-C to terminate the batch script
  * Linux: use `bin/neo4j stop`

Learn more
----------

There is a humble user guide available in the doc directory.
Just open the [index.html](doc/index.html) in a browser 




