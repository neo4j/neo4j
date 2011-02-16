Neo4j High-Availability Ops
===========================

HA-Ops provides tooling for managing a Neo4j installation, graduating from
a single server into a High-Availability cluster.

Ha-Ops Three Ways
-----------------

Installing and managing a cluster of machines is a time-honored task. Usually,
an ops will start with doing everything by hand, then looking for tools to
improve the repetitive tasks, then maybe writing their own customized tool.

So, we'll do that...

1. by-hand - bash scripting for terminal madness
2. with-chef - provisioning with Chef recipes
3. with-hops - Neo4j aware tooling

Each approach is documented and driven by executable feature descriptions,
written using Cucumber BDD. Read the READMEs, look for the `*.feature` files,
then try them out (which will require some Ruby tools). 

Where to start?
---------------

The `by-hand` tooling is the most complete and provides a suitable reference
for doing things manually, assuming you're comfortable with bash scripts.

After that try deploying `with-chef` using virtual machines to simulate
a cluster.

The `with-hops` tooling is not yet available, so you'll have to wait to try that
out. 

Cucumber what?
--------------

Cucumber is a Behaviour Driven Development tool. In use here, it's like 
following along with a blog post that actually performs the steps being
described. 

Requirements
------------

The tooling itself and even the feature descriptions are written in Ruby,
so you'll need a full Ruby development environment:

* ruby 1.8.7+
* rubygems
* rvm (recommended, but optional)

Ruby Gems:

* cucumber - for running integration tests
* chef - for provisioning the instances
* vagrant - for simulating a machine cluster with VirtualBox

References
----------

* [HA-Ops Wiki](https://github.com/akollegger/ha-ops/wiki)
* [HA-Ops By Hand](https://github.com/akollegger/ha-ops/wiki/By-hand)
* [HA-Ops With Chef](https://github.com/akollegger/ha-ops/wiki/With-chef)

* [Cucumber BDD](http://cukes.info)
* [Chef Systems Integration](http://www.opscode.com/)
* [rvm](http://rvm.beginrescueend.com/)

