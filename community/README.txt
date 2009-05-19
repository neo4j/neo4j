To get started and for more information about Neo4j and Shell go to http://neo4j.org.

Quick start: 

o Connecting to a local already running Neo4j instance (NOTE: 
  NeoService.enableRemoteShell() must have been invoked):

	java -jar shell-1.0-b8.jar 

o Connecting to an offline Neo4j store you will need neo-1.0-b8.jar, 
  jta-1_1.jar and shell-1.0-b8 in your classpath. Type:

	java -cp neo-1.0-b8.jar:jta-1_1.jar:shell-1.0-b8.jar \
		org.neo4j.util.shell.StartLocalClient <path to Neo4j store>


