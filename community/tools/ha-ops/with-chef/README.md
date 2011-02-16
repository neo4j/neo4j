HA-Ops With Chef
================

The HA-Ops Chef cookbook for Neo4j can provision any Debian-like host 
machine. To demonstrate the operation, the included scripts and
configuration use `vagrant` to simulate a cluster of machines.

Requirements
------------

Full Ruby development environment:

* ruby 1.8.7+
* rubygems 
* rvm (recommended, but optional)

Virtual Box:

* VirtualBox 4.0.2+

Gems:

* chef - for provisioning the instances
* vagrant - for launching VirtualBox instances
* cucumber - for running integration tests

Simulated Deployment with Vagrant
---------------------------------

With Vagrant installed, you can simulate an HA cluster by launching
VirtualBox VMs configured in the Vagrantfile. Running `vagrant` will
create, provision and start up a collection of Virtual Machines 
according to the specfications in the `Vagrantfile`.

To provision the VMs, notice that the Vagrantfile has the `chef.add_recipe`
lines indicate the recipe to be used for installing software. The recipes
themselves are contained under the `chef/cookbooks/` directory.

To run the simulated deployment:

1. Add a "box" to use for creating VM instances
   * `vagrant box add lucid32 http://files.vagrantup.com/lucid32.box`
2. Edit the `Vagrantfile` to change the VM instance counts
   * `zookeeper_instance_count` for number of Zookeeper VMs
   * `neo4j_instance_count` for number of Neo4j VMs
   * (optional step. By default, the spec will launch 1 of each)
3. Launch the VMs
   * `vagrant up`
4. Check that Neo4j is runnning
   * `http://localhost:7474`

Look for the "Forwarding ports..." lines in the deployment transcript
to see which local ports have been forward to a virtual machine port.

For instance...

    [neo4j_1] -- ssh: 22 => 2200 (adapter 1)

...indicates that local port 2222 is forwarded to virtual port 22
of the virtual machine named `neo4j_1`.

Build & Test
------------

The above steps can be reduced to just running the Cucumber BDD
feature description:

`rake features`

Running the full feature takes a long time. Please be patient while
the virtual machines are created, started and provisioned.


References
----------

* [Chef Systems Integration](http://www.opscode.com/chef)
* [VirtualBox](http://www.virtualbox.org/)
* [rubygems](http://rubygems.org/)
* [rvm](http://rvm.beginrescueend.com/)
* [vagrant](http://vagrantup.com/)

