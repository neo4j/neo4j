# 1. edit NEO_CHEF to be the full path to this directory
# 2. run `chef-solo -c solo.rb -j neo4j.json`
file_cache_path  "$NEO_CHEF"
cookbook_path    "$NEO_CHEF/cookbooks"
log_level        :debug
log_location     STDOUT
ssl_verify_mode  :verify_none

