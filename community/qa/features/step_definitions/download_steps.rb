require 'net/http'
require 'time'

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
  server = Net::HTTP.new(neo4j.download_host, 80)
  head = server.head("/"+archive_name)
  server_time = Time.httpdate(head['last-modified'])
  if (!File.exists?(archive_name) || server_time != File.mtime(archive_name))
    puts archive_name+" missing or newer version on server - downloading"
    server.get2("/"+archive_name) do |res|
      open(archive_name, "wb") do |file|
        file.write(res.body)
      end
    end
    File.utime(0, server_time, archive_name)
  else
    puts archive_name+" not modified - download skipped"
  end
end

Then /^the working directory should contain a Neo4j archive$/ do
  fail "#{archive_name} does not exists" unless File.exists?(archive_name)
end

When /^I unpack the archive into Neo4j Home$/ do
  full_archive_name= File.expand_path(archive_name)
  pushd neo4j.home

  if (current_platform.unix?)
    `tar xzf #{full_archive_name} --strip-components 1`
    fail 'unpacking failed' unless $?.to_i == 0
  elsif  current_platform.windows?
    unzip= File.expand_path("../../support/unzip.vbs", __FILE__)
    puts `#{unzip} #{full_archive_name} #{neo4j.home}`
    fail 'unpacking failed' unless $?.to_i == 0
  else
    fail 'platform not supported'
  end
  popd
end

Then /^Neo4j Home should contain a Neo4j Server installation$/ do
  fail "file "+neo4j.home+"/bin/neo4j not found" unless File.exists?(neo4j.home+"/bin/neo4j")
end

Then /^the Neo4j version of the installation should be correct$/ do
  (Dir.entries(neo4j.home+"/lib") + Dir.entries(neo4j.home+"/system/lib")).each do |lib|
    if lib =~ /^neo4j.*\.jar$/
      fail lib+" does not contain the Neo4j-version" unless lib =~ /#{neo4j.version}/;
    end
  end
end

