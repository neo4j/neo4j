Then /^port (\d+) on ([^\s]+) should be (open|closed)$/ do |port, host, status|
  puts "checking port #{port} on #{host} to be #{status}"
  case status
  when "open"
    is_port_open?(host, port.to_i).should be_true
  when "closed"
    is_port_open?(host, port.to_i).should be_false
  end
end

