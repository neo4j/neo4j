Feature: Start and stop Neo4j Server
  The Neo4j server should start and stop using a command line script

  Background:
    Given a platform supported by Neo4j
    And Neo4j version based on system property "NEO4JVERSION"
    And Neo4j product based on system property "NEO4J_PRODUCT" 
    And a working directory at relative path "target"
    And Neo4j Home is set to "neo4j_home"
    And that Neo4j is installed in "neo4j_home"

  Scenario: Start Neo4j Server
    When I start Neo4j Server
    And wait for Server started at "http://localhost:7474"
    Then "http://localhost:7474" should provide the Neo4j REST interface
    When I stop Neo4j Server
    And wait for Server stopped at "http://localhost:7474"
    Then "http://localhost:7474" should not provide the Neo4j REST interface
