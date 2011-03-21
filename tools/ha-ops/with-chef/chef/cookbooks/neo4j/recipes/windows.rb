#
# Cookbook Name:: neo4j::windows
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


execute "java -version" do
  action :run
end

neo4j_version=node[:neo4j][:version]||"1.3-SNAPSHOT"
tarball="neo4j-#{neo4j_version}-windows.zip"
neo4j_home = ENV['ProgramFiles'] + "\\" + "Neo4j"
downloaded_tarball = ENV['TMP'] + "\\" + tarball
exploded_tarball = ENV['TMP'] + "\\" + "neo4j-#{neo4j_version}"
public_address = node[:neo4j][:public_address]||node[:ipaddress]
bind_address = node[:neo4j][:ha][:bind_address]||node[:ipaddress]
is_coordinator_node = (node[:neo4j][:mode] == "coordinator") 

# install gem needed for unarchiving
g = gem_package "archive-zip" do
  action :nothing
end

g.run_action(:install)
   
require 'rubygems'
Gem.clear_paths
require 'archive/zip'

# download remote file
remote_file "#{downloaded_tarball}" do
  source "http://dist.neo4j.org/#{tarball}"
  not_if { File.exists?(downloaded_tarball) }
end

# unpack the downloaded file
ruby_block "unzip_neo4j_archive" do
  block do
    begin
      Archive::Zip.extract(downloaded_tarball, ENV['TMP'])
    end
  end
  action :create
  not_if { 
    File.exists?(exploded_tarball) &&
    (File.mtime(downloaded_tarball) < File.mtime(exploded_tarball))
  }
end

# copy exploded tarball to home directory
ruby_block "copy_to_neo4j_home" do
  block do
    begin
      Chef::Log.debug("copy #{exploded_tarball} to #{neo4j_home})")
      FileUtils.cp_r exploded_tarball, neo4j_home 
    end
  end
  action :create
end

# create teh data directory 
directory "#{node[:neo4j][:database_location]}" do
  mode "0755"
  action :create
  recursive true
end

template "#{neo4j_home}\\conf\\neo4j-server.properties" do
  source "neo4j-server.erb"
  mode 0444
  variables(
    :mode => node[:neo4j][:mode],
    :database_location => node[:neo4j][:database_location],
    :webserver_port => node[:neo4j][:webserver_port],
    :conf_dir => "#{neo4j_home}/conf",
    :public_address => public_address
  )
end

# install Neo4j Server as a service, if not a coordinator node
execute "InstallNeo4j.bat" do
  cwd neo4j_home + "\\bin"

  not_if { is_coordinator_node }
end


#service "example_service" do
#  supports :status => true, :restart => true, :reload => true
#  action [ :enable, :start ]
#end

# install Neo4j Coordinator as a service, if enabled

# start the Neo4j Server, if this isn't a coordinator node

# start the Neo4j Coordinator, if this is a coordinator node

