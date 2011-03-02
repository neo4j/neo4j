Feature: Neo4j has a web-based administration application
  In order to manage my Neo4j server
  As a Neo4j user
  I want to use a web-based application

  Scenario: Use a web browser to open webadmin
    When I look at the root page with a web browser
    Then I should be re-directed to the web administration page

