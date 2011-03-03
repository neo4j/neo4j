Feature: Download and unpack Neo4j Server 
  In order to get Neo4j Server
  A user
  Should be able to download and unpack a platform appropriate archive from the web

  Background:
    Given a platform supported by Neo4j
    And Neo4j version based on system property "neo4j.version"
    And a web site at host "dist.neo4j.org"

  Scenario: Download Neo4j 
    When I download Neo4j (if I haven't already)
    Then the current directory should contain a Neo4j archive

  Scenario: Unpack downloaded archive
    When I unpack the archive into "neo4j_home"
    Then "neo4j_home" should contain a Neo4j Server installation
    And the Neo4j version of the installation should be correct

