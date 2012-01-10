Debian installer project
-------------------------

Builds .deb installers for all neo4j server editions. 

To build this, you need a debian (or ubuntu) computer, and need debuild installed:

  sudo apt-get install devscripts

To build:

  mvn clean package

Installers will be located under target/
