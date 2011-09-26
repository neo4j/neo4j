Feature: Neo4j has a web-based administration application
  In order to manage my Neo4j server
  As a Neo4j user
  I want to use a web-based application

  Scenario: Use a web browser to open webadmin
    Given I have a neo4j server running
    When I look at the root server page with a web browser
    Then The browser should be re-directed to http://.+:7474/webadmin/
