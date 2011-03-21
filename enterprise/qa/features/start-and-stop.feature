Feature: Start and stop Neo4j Server
  The Neo4j server should start and stop using a command line script

  Background:
    Given a platform supported by Neo4j
    And a working directory at relative path "target"
    And set Neo4j Home to "neo4j_home"
    And Neo4j Server installed in "neo4j_home"
    When Neo4j Server is running
    Then I stop Neo4j Server


  Scenario: Start Neo4j Server
    When I start Neo4j Server
    Then "http://localhost:7474" should provide the Neo4j REST interface
    When I stop Neo4j Server
    Then "http://localhost:7474" should not provide the Neo4j REST interface
