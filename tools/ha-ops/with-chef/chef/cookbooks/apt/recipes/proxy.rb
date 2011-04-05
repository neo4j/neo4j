#
# Cookbook Name:: apt
# Recipe:: proxy
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
package "apt-proxy" do 
  action :install
end

service "apt-proxy" do
  supports :restart => true, :status => false
  action [ :enable, :start ]
end

cookbook_file "/etc/apt-proxy/apt-proxy-v2.conf" do
  source "apt-proxy-v2.conf"
  owner "root"
  group "root"
  mode 0644
  notifies :restart, resources(:service => "apt-proxy")
end
