After do |scenario|
  # Do something after each scenario.
  # The +scenario+ argument is optional, but
  # if you use it, you can inspect status with
  # the #failed?, #passed? and #exception methods.

  if neo4j.home
    puts "*** cleanup: get status: " + `#{neo4j.home}/bin/neo4j status`
    if ($? == 0) ## is running
      print "*** cleanup: stop ..."
      `#{neo4j.home}/bin/neo4j stop`
      puts ($? == 0 ? "OK" :"FAIL")
    end
  end
end


