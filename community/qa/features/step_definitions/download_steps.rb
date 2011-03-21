require 'net/http'

Given /^a platform supported by Neo4j$/ do
  fail "unsupported platform #{current_platform}" unless current_platform.supported?
end

Given /^Neo4j version based on system property "([^"]*)"$/ do |env_name|
  neo4j.version = ENV[env_name]
  fail "missing property #{env_name}" if neo4j.version == nil
end

Given /^set Neo4j Home to "([^"]*)"$/ do |home|
  neo4j.home = File.expand_path(home)
  puts "using neo4j.home "+neo4j.home
  ENV["NEO4J_HOME"] = neo4j.home
  Dir.mkdir(neo4j.home) unless File.exists?(neo4j.home)
end

Given /^a web site at host "([^"]*)"$/ do |host|
  Net::HTTP.get(URI.parse("http://#{host}"))
  neo4j.download_host=host
end

When /^I download Neo4j \(if I haven't already\)$/ do
  `wget -c -O #{archive_name} #{neo4j.download_host}/#{archive_name}`
  fail 'download did not succeed' unless $?.to_i == 0
end

Then /^the working directory should contain a Neo4j archive$/ do
  fail "#{archive_name} does not exists" unless File.exists?(archive_name)
end

When /^I unpack the archive into Neo4j Home$/ do
  full_archive_name= File.expand_path(archive_name)
  pushd neo4j.home
  `tar xzf #{full_archive_name} --strip-components 1`
  fail 'unpacking failed' unless $?.to_i == 0
  popd
end

Then /^Neo4j Home should contain a Neo4j Server installation$/ do
## TODO
# pending # express the regexp above with the code you wish you had
end

Then /^the Neo4j version of the installation should be correct$/ do
## TODO
##  pending # express the regexp above with the code you wish you had
end

