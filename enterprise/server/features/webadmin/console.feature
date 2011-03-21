Feature: Webadmin has a gremlin console
  In order to perform advanced data operations with webadmin
  As a Neo4j user
  I want a console in webadmin that allows me to use the gremlin graph language to manipulate the database

  Scenario: Go to console tab
    Given I have a neo4j server running
    When I look at webadmin in a web browser
    And I click on the Console tab in webadmin
    Then An element should appear that can be found by the xpath //li[contains(.,"gremlin")]

  Scenario: Get the root node through the console
    Given I have a neo4j server running
    When I look at webadmin in a web browser
    And I click on the Console tab in webadmin
    And I type g.v(0) into the element found by the xpath //input[@id="console-input"]
    And I hit return in the element found by the xpath //input[@id="console-input"]
    Then An element should appear that can be found by the xpath //li[contains(.,"v[0]")]
