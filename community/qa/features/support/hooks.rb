After do |scenario|
  # Do something after each scenario.
  # The +scenario+ argument is optional, but
  # if you use it, you can inspect status with
  # the #failed?, #passed? and #exception methods.

  if neo4j.home && current_platform.unix?
    puts "*** cleanup: get status: " + `#{neo4j.home}/bin/neo4j status`
    if ($? == 0) ## is running
      print "*** cleanup: stop ..."
      `#{neo4j.home}/bin/neo4j stop`
      puts ($? == 0 ? "OK" :"FAIL")
    end
  end

  if neo4j.home && current_platform.windows?
    print "*** cleanup: stop and remove ..."
    puts `#{neo4j.home}\\bin\\wrapper-windows-x86-32.exe -r ..\\conf\\neo4j-wrapper.conf`
    puts ($? == 0 ? "... OK" :"... FAIL")
  end
end


