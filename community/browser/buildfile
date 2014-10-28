require 'buildr/scala'

# This is just for fun.  You should probably start with the Maven build...

project_layout = Layout.new
project_layout[:source, :test, :scala] = 'tools/src/test/scala'
project_layout[:target, :test, :scala] = 'tools/target/classes'
project_layout[:source, :main, :resources] = 'dist'

repositories.remote << 'http://mirrors.ibiblio.org/pub/mirrors/maven2'

NEO4J_VERSION = "2.0.0-M04"

define "neo4j-browser", :layout=>project_layout do
  project.version = '0.0.2'
  package :jar


  test.with transitive("org.neo4j:neo4j:jar:#{NEO4J_VERSION}"),
    transitive("org.neo4j.app:neo4j-server:jar:#{NEO4J_VERSION}"),
    "org.neo4j.app:neo4j-server:jar:static-web:#{NEO4J_VERSION}"
end
