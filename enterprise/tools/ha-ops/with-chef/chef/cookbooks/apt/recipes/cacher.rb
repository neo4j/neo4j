#
# Cookbook Name:: apt
# Recipe:: cacher
#
# Copyright 2008-2009, Opscode, Inc.
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
package "apt-cacher" do
  action :install
end

service "apt-cacher" do
  supports :restart => true, :status => false
  action [ :enable, :start ]
end

cookbook_file "/etc/apt-cacher/apt-cacher.conf" do
  source "apt-cacher.conf"
  owner "root"
  group "root"
  mode 0644
  notifies :restart, resources(:service => "apt-cacher")
end

cookbook_file "/etc/default/apt-cacher" do
  source "apt-cacher"
  owner "root"
  group "root"
  mode 0644
  notifies :restart, resources(:service => "apt-cacher")
end
