Feature: Webadmin loading screen on slow operations
  In order to know webadmin has not hung during slow operations
  As a Neo4j user
  I want to see a loading screen when server calls take a long time

  Scenario: Perform slow operation
    Given I have a neo4j server running

    When I look at webadmin in a web browser
    And I click on the Console tab in webadmin
    And I type Thread.sleep(3000); into the element found by the xpath //input[@id="console-input"]
    And I hit return in the element found by the xpath //input[@id="console-input"]

    Then An element should appear that can be found by the xpath //div[@class="loading-spinner"]
