# chef-solo -c $NEO_CHEF/solo.rb -j $NEO_CHEF/dna.json
file_cache_path  "$NEO_CHEF"
cookbook_path    "$NEO_CHEF/cookbooks"
log_level        :debug
log_location     STDOUT
ssl_verify_mode  :verify_none

