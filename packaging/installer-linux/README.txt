Debian installer project
-------------------------

Builds .deb installers for all neo4j server editions. 

To build this, you need a debian (or ubuntu) computer, and need debuild installed:

  sudo apt-get install devscripts

To build:

  mvn clean package

Installers will be located under target/


Where does it find the standalone resources?
--------------------------------------------

This project expects unix standalone artifacts, as produced by the standalone project,
to be located under /target.

To help development, there is a profile, active by default, that will pull in these artifacts
from the standalone project in ../standalone/target. To deactivate that and provide these
artifacts "externally" (eg. via a build system), invoke the build like so:

  mvn clean package -DpullArtifacts=false
