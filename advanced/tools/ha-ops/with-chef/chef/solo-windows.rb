# chef-solo -c solo-windows.rb -j neo4j-windows.json
file_cache_path  ENV["NEO4J_CHEF"]
cookbook_path    ENV["NEO4J_CHEF"]+"\\cookbooks"
log_level        :debug
log_location     STDOUT
ssl_verify_mode  :verify_none

