Given /^that Neo(\d+)j is installed in "(.*?)"$/ do |arg1, arg2|
  copy_file(URI.parse("../../standalone/target/neo4j-community-1.9-SNAPSHOT-unix.tar.gz"), "#{neo4j.home}/neo4j-community-1.9-SNAPSHOT-unix.tar.gz")
  untar("#{neo4j.home}/neo4j-community-1.9-SNAPSHOT-unix.tar.gz", "#{neo4j.home}")
end