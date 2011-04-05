Given /^Neo4j Server is (not )?running$/ do |negate|
  if (current_platform.unix?)
    puts `#{neo4j.home}/bin/neo4j status`
    $? == (negate == 'not ' ? 256 : 0)
  elsif (current_platform.windows?)
    puts `#{neo4j.home}\\bin\\wrapper-windows-x86-32.exe -q ..\\conf\\neo4j-wrapper.conf`
    puts "result #{$?} "
    # fail "failed #{$?} " if $?!= 0
    fail "not implemented"
  else
    fail 'platform not supported'
  end

end


Then /^I (start|stop) Neo4j Server$/ do |action|
  if (current_platform.unix?)
    puts `#{neo4j.home}/bin/neo4j #{action} >/dev/null 2&>1 `   ### todo reenable stdout
  elsif (current_platform.windows?)
    puts `#{neo4j.home}\\bin\\wrapper-windows-x86-32.exe #{ action == 'start' ? '-it' : '-r' } ..\\conf\\neo4j-wrapper.conf`
  else
    fail 'platform not supported'
  end
  fail "failed #{$?} " if $?!= 0
end


When /^wait for Server started at "([^\"]*)"$/ do |uri|
  i = 0
  while i<30 do
    p i
    begin
      response = Net::HTTP.get_response(URI.parse(uri))
      p response
      return if (response.code.to_i == 200)
    rescue Exception=>e
      p e
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

