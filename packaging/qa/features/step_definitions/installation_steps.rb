Given /^that Neo(\d+)j is installed in "(.*?)"$/ do |arg1, arg2|
  copy_file(URI.parse("../../standalone/target/#{archive_name}"), "#{neo4j.home}/#{archive_name}")
  untar("#{neo4j.home}/#{archive_name}", "#{neo4j.home}")
end
