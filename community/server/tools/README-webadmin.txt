
Rapid development tools for webadmin
------------------------------------

The server build process currently forces you to do a fairly long round-trip
when doing webadmin development. Until that is fixed, the tools directory contains
a set of scripts that help you do webadmin development without having to redo
the full server build for each change you make in source code.

IMPORTANT
---------
These tools do not produce the aggregated and minified webadmin-min.js file
that is used in production. These tools simply compile coffeescript and haml 1 to 1
to javascript. To use the compiled files like these, use "dev.html" (eg. go to http://localhost:7474/webadmin/dev.html)

To produce the webadmin-min.js file, run the server build like this:
mvn package -Pwebadmin-build

Which will include the brew javascript build plugin in the build process.


Scripts
-------

requirehaml    - A wrapper for the haml.js compiler that monitors changes to haml files 
                 and auto-compiles them. Also wraps the compiled template in require.js 
                 boilerplate for easy dependency management.

neo-haml       - Bash script for executing requirehaml with args matching the neo4j 
                 folder structure.

neo-coffee     - Bash script for executing coffeescript with args matching the neo4j
                 folder structure.


Prequisites
-----------

Node.js (http://nodejs.org/#download)
Node package manager (http://npmjs.org/)

coffeescript (command: npm install coffee-script)
haml-js (command: npm install haml)


Move files into place
---------------------

ln -s server/tools/neo-haml /usr/local/sbin/
ln -s server/tools/neo-coffee /usr/local/sbin/
ln -s server/tools/requirehaml /usr/local/sbin/

chmod +x /usr/local/sbin/neo-haml
chmod +x /usr/local/sbin/neo-coffee
chmod +x /usr/local/sbin/requirehaml


Start up build environment
--------------------------

All the below commands should be executed in the "server" subproject of the "server" project.
Either execute these with "&", or in separate consoles.

  mvn clean package antrun:run -P webdev-exec,neodev
  neo-haml
  neo-coffee

Launch a browser, and go to http://localhost:7474/webadmin/dev.html

Notice that we are using the dev.html file, not index.html. Index.html loads the pre-compiled 
and minified webadmin version (webadmin-min.js), dev.html loads files on the fly which is slower, but is extremely
helpful for debugging. See the IMPORTANT section at the top for how to produce the webadmin-min.js file.


