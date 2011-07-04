Given /^Neo4j Server is (not )?running$/ do |negate|
  if (current_platform.unix?)
    puts `#{neo4j.home}/bin/neo4j status`
    $? == (negate == 'not ' ? 256 : 0)
  elsif (current_platform.windows?)
    Dir.chdir("#{neo4j.home}\\bin")
    puts `Neo4j.bat query`
    puts "====> result #{$?} "
    fail "failed #{$?} " if $?!= 0
  else
    fail 'platform not supported'
  end

end


When /^I (start|stop) Neo4j Server$/ do |action|
  puts "=====> stop/start "
  if (current_platform.unix?)
    #IO.popen("#{neo4j.home}/bin/neo4j #{action}", close_fds=1).close
    puts `#{neo4j.home}/bin/neo4j #{action}`
  elsif (current_platform.windows?)
    Dir.chdir("#{neo4j.home}\\bin")
    if (action == "start")
      puts `Neo4j.bat install`
      puts `Neo4j.bat start`
    else
      puts `Neo4j.bat stop`
      puts `Neo4j.bat remove`
    end
  else
    fail 'platform not supported'
  end
end


When /^wait for Server (started|stopped) at "([^\"]*)"$/ do |state, uri|
  i = 0
  puts "====> wait"
  while i<60 do
    puts i
    begin
      response = Net::HTTP.get_response(URI.parse(uri))
      puts response.to_s
      break if (response.code.to_i == 200) && state == "started"
    rescue Exception=>e
      puts e.to_s
      break if (state == "stopped")
    end
    sleep 1
    i += 1
  end
end


Then /^"([^"]*)" should (not)? ?provide the Neo4j REST interface$/ do |uri, negate|
  begin
    response = Net::HTTP.get_response(URI.parse(uri))
  rescue Exception=>e
    fail "REST-interface is not running #{e}" if e && negate != 'not'
  end
  fail 'REST-interface is not running' if negate == nil && response && response.code.to_i != 200
  fail 'REST-interface is running' if negate == 'not' && response && response.code.to_i == 200
end

