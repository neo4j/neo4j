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


Working on this project
-----------------------

This project attempts to create an abstract set of tests that should be applied to all of, 
or a subset of, our editions, on all platforms. Each platform is defined as an operating system
and a type of packaging, for example Ubuntu + Debian installer (ubuntu-deb), or Ubuntu + Generic
Unix packaging (ubuntu-generic).

Each such platform pair has a driver implementation, that actually contains the code for fulfilling
the requirements of the abstract tests.

Abstract tests can be found here:
src/test/java/org/neo4j/qa/CommonQualityAssuranceTest.java
src/test/java/org/neo4j/qa/EnterpriseQualityAssuranceTest.java

Examples of driver implementations:
src/test/java/org/neo4j/qa/driver/UbuntuDebCommunityDriver.java
src/test/java/org/neo4j/qa/driver/WindowsAdvancedDriver.java


What the hell is this sahara rollback business?
-----------------------------------------------

Because it takes a long time to boot and provision a VM, we use the sahara library. It lets us
start and provision a VM, and then take a snapshot. After we are done with a test, we roll back
the VM to the snapshot and leave it running. The next time we need a VM with those capabilities,
we don't have to re-import, re-boot or re-provision. Rolling back takes a few seconds, so this 
significantly speeds up the tests.
