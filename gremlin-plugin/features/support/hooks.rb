After do |scenario|
  # Do something after each scenario.
  # The +scenario+ argument is optional, but
  # if you use it, you can inspect status with
  # the #failed?, #passed? and #exception methods.

  if neo4j.home && current_platform.unix?
    `#{neo4j.home}/bin/neo4j status`
    if ($? == 0) ## is running
      `#{neo4j.home}/bin/neo4j stop`
    end
  end

  if neo4j.home && current_platform.windows?
     IO.popen("#{neo4j.home}\\bin\\Neo4j.bat stop")
     sleep 12
     IO.popen("#{neo4j.home}\\bin\\Neo4j.bat remove")
  end
end


