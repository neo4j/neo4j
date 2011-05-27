Feature: Start and stop Neo4j Server
  The Neo4j server should start and stop using a command line script

  Background:
    Given a platform supported by Neo4j
    And a working directory at relative path "target"
    And set Neo4j Home to "neo4j_home"
    And Neo4j Home should contain a Neo4j Server installation

  Scenario: Start Neo4j Server
    When I start Neo4j Server
    And wait for Server started at "http://localhost:7474"
    Then "http://localhost:7474" should provide the Neo4j REST interface
    Then requesting "http://localhost:7474/db/data/ext/" should contain "GremlinPlugin"
    Then sending "script=g.V" to "http://localhost:7474/db/data/ext/GremlinPlugin/graphdb/execute_script" should contain "node"
    Then sending "script=for (i in 1..2){g.v(0);1};1" to "http://localhost:7474/db/data/ext/GremlinPlugin/graphdb/execute_script" should contain "1"    
    Then sending "script=GraphMLReader.inputGraph(g, new URL('https://github.com/tinkerpop/gremlin/raw/master/data/graph-example-1.xml').openStream());g.v(1).outE.inV.name.paths" to "http://localhost:7474/db/data/ext/GremlinPlugin/graphdb/execute_script" should contain "1"    
    
    