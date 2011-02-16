Feature: Manage a Neo4j HA Server cluster running on localhost
In order to run a Neo4j-based application that scales and has high availability
An Ops
Should use standard unix utilities
To manage both a Neo4j-HA Data cluster and a Neo4j Coordinator cluster

  Background:
    Given a bash compatible shell
    And Java 1.6
    And a working directory at relative path "./installation"
    And environment variable "NEO4J_COORDINATOR_INSTANCE_DIR" set to "./coord-instances"
    And environment variable "NEO4J_COORDINATOR_INSTANCE_COUNT" set to "3"
    And environment variable "NEO4J_COORDINATOR_SERVERS" set to "localhost:2181,localhost:2182,localhost:2183"
    And environment variable "NEO4J_DATA_INSTANCE_DIR" set to "./neo4j-instances"
    And environment variable "NEO4J_DATA_INSTANCE_COUNT" set to "3"

  @stop-neo4j-cluster
  Scenario: Stop local Neo4j processes
    When I run these shell commands:
    """
    for (( i=1; i<=${NEO4J_DATA_INSTANCE_COUNT}; i++ )); do $NEO4J_DATA_INSTANCE_DIR/neo4j-$i/bin/neo4j stop ; done
    """
    Then port 7474 on localhost should be closed
    And port 7475 on localhost should be closed
    And port 7476 on localhost should be closed

  @stop-coordinator-cluster
  Scenario: Stop local Neo4j Coordinator processes
    When I run these shell commands:
    """
    for (( i=1; i<=${NEO4J_COORDINATOR_INSTANCE_COUNT}; i++ )); do $NEO4J_COORDINATOR_INSTANCE_DIR/coord-$i/bin/neo4j-coordinator stop ; done
    """
    Then port 2181 on localhost should be closed
    And port 2182 on localhost should be closed
    And port 2183 on localhost should be closed

