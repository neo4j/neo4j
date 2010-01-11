To get started and for more information about Neo4j and Shell go to http://neo4j.org.

Quick start: 

o Connecting to a local already running Neo4j instance (NOTE: 
  GraphDatabaseService.enableRemoteShell() must have been invoked):

	java -jar shell-1.0-rc.jar 

o Connecting to an offline Neo4j store you will need kernel-1.0-rc.jar, 
  jta-1_1.jar and shell-1.0-rc in your classpath. Type:

	java -cp kernel-1.0-rc.jar:jta-1_1.jar:shell-1.0-rc.jar \
		org.neo4j.util.shell.StartLocalClient <path to Neo4j store>


