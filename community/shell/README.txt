To get started and for more information about Neo4j and Shell go to http://neo4j.org.

Use this component to connect to a
* local Neo4j graph database
* remote already running Neo4j instance (NOTE: 
  GraphDatabaseService.enableRemoteShell() must have been invoked)

Quick start: 

Issue the following command:

    java -jar neo4j-shell-{version}.jar 

Make sure to replace {version} with the actual version.
The neo4j-kernel and geronimo jta jar files need to be in the
same directory or on the classpath.
