Linux installers project
------------------------

Builds linux installers for all neo4j server editions.

Current installers:
-------------------

  * Debian

To build this, you need debuild installed:

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


Testing installing the built packages on Debian/Ubuntu
------------------------------------------------------

`gdebi` is a useful tool for installing deb files. It will
automatatically install the dependencies of the package. Install gdebi
with:

  sudo apt-get install gdebi-core

Then install neo4j with:

  sudo gdebi /path/to/neo4j_<version>_all.deb

This should install a compatible JRE if none is installed. On both
Debian and Ubuntu this will be OpenJDK by default. In order to run
Neo4j with OracleJDK it has to be downloaded, packaged, and installed
manually.

Start by downloading the JDK or JRE from:

  http://www.oracle.com/technetwork/java/javase/downloads

If the downloaded file does not end in `.tar.gz` it has to be renamed,
the Oracle servers seems to serve up the file with an ending of only
`.gz`, which the packaging scripts do not accept, so you will have to
rename the file like so:

  mv jdk-7u25-linux-x64.gz jdk-7u25-linux-x64.tar.gz

Packaging the JDK requires the `java-package` package to be
installed. `java-package` is a "contrib" package, so "contrib" has
to be added to:

  /etc/apt/sources.list

Then you can install `java-package`:

  sudo apt-get update
  sudo apt-get install java-package

Now you are ready to package the JRE or JDK:

  make-jpkg jdk-7u25-linux-x64.tar.gz

Now you can install the Oracle JDK or JRE, preferably with `gdebi`:

  sudo gdebi oracle-j2sdk1.7_1.7.0+update25_amd64.deb
