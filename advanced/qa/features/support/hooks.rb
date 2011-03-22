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
    `#{neo4j.home}\\bin\\wrapper-windows-x86-32.exe -r ..\\conf\\neo4j-wrapper.conf`
  end
end


