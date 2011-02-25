set_unless[:neo4j][:version] = "1.3.M02"
set_unless[:neo4j][:mode] = "solo"
if (platform == "windows")
  set_unless[:neo4j][:database_location] = ENV['APPDATA'] + "\\Neo4j\\Data"
else 
  set_unless[:neo4j][:database_location] = "/srv/neo4j/graphdb"
end
set_unless[:neo4j][:public_address] = "localhost"
set_unless[:neo4j][:webserver_port] = 7474
set_unless[:neo4j][:ha][:bind_address] = "localhost"
set_unless[:neo4j][:ha][:port] = 6001
set_unless[:neo4j][:ha][:machine_id] = 1
set_unless[:neo4j][:coordinator][:client_port] = 2181
set_unless[:neo4j][:coordinator][:machine_id] = 1
set_unless[:neo4j][:coordinator][:sync_limit] = 5
set_unless[:neo4j][:coordinator][:init_limit] = 10
set_unless[:neo4j][:coordinator][:tick_time] = 2000
set_unless[:neo4j][:coordinator][:data_dir] = "/srv/neo4j/coordinator"

