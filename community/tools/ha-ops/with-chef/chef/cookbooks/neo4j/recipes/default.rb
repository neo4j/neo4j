#
# Cookbook Name:: neo4j
# Recipe:: default
#
# Copyright 2011, Example Com
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
is_coordinator_node = node[:neo4j][:coordinator][:enable]||false

# create Neo4j Home
include_recipe "neo4j::home"

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

