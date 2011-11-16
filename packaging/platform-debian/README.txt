Platform specific distributions for Neo4 server
-----------------------------------------------

Currenty only builds debian distributions.

To build this, you need a debian (or ubuntu) computer, and need debuild installed:

  sudo apt-get install devscripts

Then just do:

  mvn clean package

And you will have a .deb file in "target/"

