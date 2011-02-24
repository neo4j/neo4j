HA-Ops By Hand
==============

Installing and managing a Neo4j High-Availability cluster by hand involves 
running a lot of shell commands to get everything configured properly.

The feature descriptions in the `features` subdirectory describe a sequence 
of scenerios for installing, configuring, then running a Neo4j cluster 
using just bash scripts.

Each scenario includes an inline bash script that performs one part of the
process - install, create (instances), start, stop. The scripts are executed
and the results verified when you "run" the feature.

The scenerios explain pre-conditions, run a script, then check for expected 
results.

Running with Cucumber
---------------------

The scenerios can be run using a rake task, or the `cucumber` command that
was installed with the Cucumber Gem. 

* `rake features` - to run all scenarios in all features
* `cucumber --dry-run` - to show all scenarios without running anything
* `cucumber features/<feature-name>` - to run a particular feature

Using the GraphDB Cluster
-------------------------

By default, the features work with three Neo4j GraphDB instances. To use 
them, just install then start the cluster:

1. `cucumber features/install-localhost-cluster.feature`
2. `cucumber features/start-localhost-cluster.feature`

Then you'll have Neo4j GraphDBs available at:

* (http://localhost:7474)
* (http://localhost:7475)
* (http://localhost:7476)

To stop the cluster:

1. `cucumber features/stop-localhost-cluster.feature`

Running Literally by Hand
-------------------------

Reading through the [features](by-hand/features/) you can see exactly what tools 
are required and what has to be configured to set up a cluster on a local machine. 

The`defaults.cfg` file in this directory sets up the environment variables in 
the same way as used by the features. You can source that, then copy-and-paste 
each line of shell commands to install, configure and manage the localhost cluster.

To work with a group of actual machines (to which you have ssh access), the 
addresses would change from `localhost` to the actual machine addresses. Then, 
each `for` means "do this to each machine" for the appropriate cluster type.

References
----------

* [HA-Ops By Hand](https://github.com/akollegger/ha-ops/wiki/By-hand)

* [Cucumber BDD](http://cukes.info)

