Feature: Neo4j Server distribution includes working plugin and extension examples

  Background:
    Given a platform supported by Neo4j
    And environment variable "NEO4J_HOME" pointing to a Neo4j Server installation

  @server-examples
  Scenario: Find server examples source code
    When I look in the "examples/java/server/examples" directory under NEO4J_HOME
    Then it should have at least 1 java file that contains /extends ServerPlugin/
    And it should have at least 1 java file that contains /import javax.ws.rs.Path/

  @server-examples
  Scenario: Find server examples library
    When I look in the "examples/java/server/lib" directory under NEO4J_HOME
    Then it should contain a "neo4j-server-examples" jar

  @server-examples
  Scenario: Start server with example plugins
    Given that Neo4j Server is not running
    When I install the "neo4j-server-examples" jar from examples/java/server/lib into "plugins"
    And I start the server
    And I browse the REST API to the database extensions
    Then they should be non-empty

