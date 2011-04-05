Feature: Download and unpack Neo4j
  In order to install Neo4j locally
  An Ops should
  Use Hops to download and unpack a distribution of Neo4j

  Scenario: Download a specific version of Neo4j
       When the user runs hops "get neo4j_home" 
       Then "neo4j_home" will have Neo4j installed in a subdirectory

