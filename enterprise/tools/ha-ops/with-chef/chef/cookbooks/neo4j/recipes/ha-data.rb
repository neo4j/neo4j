#
# Cookbook Name:: neo4j
# Recipe:: default
#
# Copyright 2011, Neo Technology
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#

# TODO: hack up a check for java. ohai doe not handle this well
# require_recipe "java"

# Neo4j "ha" Server - data node in an HA cluster

require_recipe "apt"

package "default-jre-headless" do
  action :install
  
end

neo4j_version=node[:neo4j][:version]||"1.3-SNAPSHOT"
tarball = "neo4j-#{neo4j_version}-unix.tar.gz"
downloaded_tarball = "/tmp/#{tarball}"
installation_dir = "/opt"
exploded_tarball = "#{installation_dir}/neo4j-#{neo4j_version}"
installed_app_dir = "#{installation_dir}/neo4j"
public_address = node[:neo4j][:public_address]||node[:ipaddress]
bind_address = node[:neo4j][:ha][:bind_address]||node[:ipaddress]
is_coordinator_node = (node[:neo4j][:mode] == "coordinator") 

# download remote file
remote_file "#{downloaded_tarball}" do
  source "http://dist.neo4j.org/#{tarball}"
  mode "0644"
end

# unpack the downloaded file
execute "tar" do
 user "root"
 group "root"

 cwd installation_dir
 command "tar zxf #{downloaded_tarball}"
 creates exploded_tarball
 action :run
end

# rename the directory to plain ol' neo4j
execute "mv #{exploded_tarball} #{installed_app_dir}" do
  user "root"
  group "root"

  creates installed_app_dir
end

# create teh data directory 
directory "#{node[:neo4j][:database_location]}" do
  owner "root"
  group "root"
  mode "0755"
  action :create
  recursive true
end

template "#{installed_app_dir}/conf/neo4j-server.properties" do
  source "neo4j-server.erb"
  mode 0444
  owner "root"
  group "root"
  variables(
    :mode => node[:neo4j][:mode],
    :database_location => node[:neo4j][:database_location],
    :webserver_port => node[:neo4j][:webserver_port],
    :conf_dir => "#{installed_app_dir}/conf",
    :public_address => public_address
  )
end


template "#{installed_app_dir}/conf/neo4j.properties" do
  source "neo4j.erb"
  mode 0444
  owner "root"
  group "root"
  variables(
    :enable_ha => node[:neo4j][:ha][:enable],
    :ha_server => "#{bind_address}:#{node[:neo4j][:ha][:port]}",
    :ha_machine_id => node[:neo4j][:ha][:machine_id],
    :coordinator_port => node[:neo4j][:coordinator][:port],
    :coordinator_addresses => node[:neo4j][:coordinator][:cluster]
  )
end

template "#{installed_app_dir}/conf/coord.cfg" do
  source "coord_cfg.erb"
  mode 0644
  owner "root"
  group "root"
  variables(
    :cluster_addresses => node[:neo4j][:coordinator][:cluster],
    :client_port =>node[:neo4j][:coordinator][:client_port],
    :sync_limit => node[:neo4j][:coordinator][:sync_limit],
    :init_limit => node[:neo4j][:coordinator][:init_limit],
    :tick_time => node[:neo4j][:coordinator][:tick_time],
    :data_dir => node[:neo4j][:coordinator][:data_dir]
  )
end

directory "#{node[:neo4j][:coordinator][:data_dir]}" do
  owner "root"
  group "root"
  mode 0755
  action :create
  recursive true
end

template "#{node[:neo4j][:coordinator][:data_dir]}/myid" do
  source "myid.erb"
  mode 0644
  owner "root"
  group "root"
  variables(
    :machine_id => node[:neo4j][:coordinator][:machine_id]
  )
end

# install Neo4j Server as a service, if not a coordinator node
execute "./neo4j install" do
  user "root"
  group "root"

  cwd installed_app_dir + "/bin"
  creates "/etc/init.d/neo4j-server"

  not_if { is_coordinator_node }
end

# install Neo4j Coordinator as a service, if enabled
execute "./neo4j-coordinator install" do
  user "root"
  group "root"

  cwd installed_app_dir + "/bin"
  creates "/etc/init.d/neo4j-coordinator"

  only_if { is_coordinator_node }
end

# start the Neo4j Server, if this isn't a coordinator node
execute "./neo4j-server start" do
  user "root"
  group "root"

  cwd "/etc/init.d"
  not_if do
    File.exists?("#{installed_app_dir}/data/neo4j-server.pid")
  end

  not_if { is_coordinator_node }
end

# start the Neo4j Coordinator, if this is a coordinator node
execute "./neo4j-coordinator start" do
  user "root"
  group "root"

  cwd "/etc/init.d"
  not_if do
    File.exists?("#{installed_app_dir}/data/neo4j-coordinator.pid")
  end

  only_if { is_coordinator_node }
end

