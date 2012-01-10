Windows installer QA
--------------------

Tests all windows installers (community, advanced, enterprise and coordinator) on windows VMs.

Required to run:

  * Virtual Box
  * Vagrant (gem install vagrant)
  * sahara (gem install sahara) <-- provides rollback capability for vagrant

To run the tests:

  mvn clean test
