#
# Cookbook Name:: zookeeper
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

require_recipe "apt"

package "zookeeper" do
  action :install
end

package "zookeeperd" do
  action :install
end

template "/etc/zookeeper/conf/zoo.cfg" do
  source "zoo_cfg.erb"
  mode 0644
  owner "root"
  group "root"
  variables(
    :cluster_addresses => node[:zookeeper][:cluster_addresses],
    :client_port =>node[:zookeeper][:client_port],
    :sync_limit => node[:zookeeper][:sync_limit],
    :init_limit => node[:zookeeper][:init_limit],
    :tick_time => node[:zookeeper][:tick_time],
    :data_dir => node[:zookeeper][:data_dir]
  )
end

directory "#{node[:zookeeper][:data_dir]}" do
  owner "zookeeper"
  group "zookeeper"
  mode 0755
  action :create
  recursive true
end

template "#{node[:zookeeper][:data_dir]}/myid" do
  source "myid.erb"
  mode 0644
  owner "zookeeper"
  group "zookeeper"
  variables(
    :zookeeper_id => node[:zookeeper][:zookeeper_id]
  )
end


