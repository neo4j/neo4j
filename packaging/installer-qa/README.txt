Installer QA
------------

Tests installers (community, advanced, enterprise and coordinator) using VMs.

Required to run:

  * Virtual Box
  * Vagrant (gem install vagrant)
  * sahara (gem install sahara) <-- provides rollback capability for vagrant

To run all the tests:

  mvn clean test

You can specify to run only some specific platforms:

  mvn clean test -Dtest-platforms=windows,ubuntu-deb

See what platforms are available here: src/test/java/org/neo4j/qa/Platforms.java
