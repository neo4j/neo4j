Feature: Download and unpack Neo4j Server
  In order to get Neo4j Server
  A user
  Should be able to download and unpack a platform appropriate archive from the web

  Background:
    Given a platform supported by Neo4j
    And a working directory at relative path "target"
    And set Neo4j Home to "neo4j_home"
    And Neo4j version based on system property "NEO4J_VERSION"
    And Neo4j product based on system property "NEO4J_PRODUCT"
    And a web site at host "dist.neo4j.org" or environment variable "DOWNLOAD_LOCATION"

  Scenario: Download Neo4j
    When I download Neo4j (if I haven't already)
    Then the working directory should contain a Neo4j archive

  Scenario: Unpack downloaded archive
    When I unpack the archive into Neo4j Home
    Then Neo4j Home should contain a Neo4j Server installation
    And the Neo4j version of the installation files except plugins should be correct
    And in Windows I will patch the "neo4j_home/conf/neo4j-wrapper.conf" adding "wrapper.java.command" to "#{ENV['JAVA_HOME']}\\bin\\java.exe"
