Given /^Neo4j Coordinator cluster is running$/ do
  ENV["NEO4J_COORDINATOR_SERVERS"].split(',').all? do |address|
    host_port = address.split(':')
    is_port_open?(host_port[0], host_port[1].to_i)
  end
end

Given /at address ([\w.]+:\d+)$/ do |address|
    host_port = address.split(':')
    is_port_open?(host_port[0], host_port[1].to_i).should be_true
end

