module VagrantHelpers

  def vagrant_file_exists
    return FileTest.exists?("Vagrantfile")
  end

  def vbox_version
    virtualbox_version = `virtualbox --help`
    virtualbox_version.lines.first
  end

  def vagrant_cluster_counts
    found_instances = {}
    File.open( "Vagrantfile" ).each {|line|
      if (m = /zookeeper_instance_count = (\d+)/.match(line)) then found_instances[:zoo] = m[1] end
      if (m = /neo4j_instance_count = (\d+)/.match(line)) then found_instances[:neo4j] = m[1] end
    }
    return found_instances
  end

  def vagrant_up
    `vagrant up`
  end

  def vagrant_status
    `vagrant status`
  end

  def running_instances_of(type_prefix)
    return vagrant_status.lines.count {|line| /#{type_prefix}\d+\s+running/ =~ line }
  end
end

World(VagrantHelpers)


