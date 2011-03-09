Feature: Start and stop Neo4j Server
  The Neo4j server should start and stop using a command line script

  Background:
    Given a platform supported by Neo4j
    And Neo4j Home based on system property "neo4j.home"
    And Neo4j Server installed in Neo4j Home

  Scenario: Start Neo4j Server
    When I start Neo4j Server
    Then "http:\\localhost:7474" should provide the Neo4j REST interface

