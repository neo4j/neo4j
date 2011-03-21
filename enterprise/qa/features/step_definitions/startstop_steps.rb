And /^Neo4j Home based on system property "([^"]*)"$/ do |home|
  pending # express the regexp above with the code you wish you had
end

And /^Neo4j Server installed in "([^"]*)"$/ do |home|
#puts home
#Dir.exist(home)
#  pending # express the regexp above with the code you wish you had
end

Given /^Neo4j Server is (not )?running$/ do |negate|
  puts `#{neo4j.home}/bin/neo4j status`
  $? == (negate == 'not ' ? 256 : 0)

end


Then /^I (start|stop) Neo4j Server$/ do |action|
  puts `#{neo4j.home}/bin/neo4j #{action}`
  fail "already running" if $? == 256
  fail "unknown return code #{$?} " if $?!= 0
  sleep 5
#  puts `ps -o ppid,pid, xau`
end


Then /^"([^"]*)" should (not )?provide the Neo4j REST interface$/ do |uri, negate|
  response=`curl -q #{uri}`
  fail response if negate ? $? == 0 : $? != 0
end

