# Steps for working with vagrant to simulate a cluster deployment
#


Given /^a prepared Vagrant simulation$/ do 
  vbox_version.should match(/Oracle VM VirtualBox Manager 4.0/),
    "VirtualBox 4.0+ required. Please check your version."
  vagrant_file_exists.should be_true,
    "Vagrantfile is missing"
end

Given /^a cluster with (\d+) Zookeeper and (\d+) Neo4j instances?$/ do |expected_zookeepers, expected_neo4j|
  specified_instances = vagrant_cluster_counts
  specified_instances[:zoo].should eq(expected_zookeepers),
    "#{specified_instances[:zoo]} Zookeeper(s) specified in Vagrantfile, but expected (#{expected_zookeepers})"
  specified_instances[:neo4j].should eq(expected_neo4j),
    "#{specified_instances[:neo4j]} Neo4j server(s) specified in Vagrantfile, but expected (#{expected_neo4j})"
end

Given /^no running VM instances$/ do
  vagrant_status = `vagrant status`
  vagrant_status.should_not match(/running/),
    "At least one vagrant instance is already running. Stop instances with `vagrant halt`"
end

When /^I launch the simulation$/ do
  puts "Launching the simulation. This may take some time..."
  vagrant_up
end

Then /^I should have (\d+) Neo4j instances?$/ do |expected_neo4j|
  running_instances_of("neo4j_").should == expected_neo4j.to_i
end

Then /^(\d+) [Zz]ookeeper instances?$/ do |expected_zookeepers|
  running_instances_of("zoo_").should == expected_zookeepers.to_i
end

