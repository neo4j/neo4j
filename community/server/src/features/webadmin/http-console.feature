Feature: Webadmin has a http console
  In order to prototype REST calls to the server
  As a Neo4j user
  I want a console in webadmin that allows me to use a simple HTTP DSL to execute HTTP calls to the server.

  Scenario: Go to console tab
    Given I have a neo4j server running
    When I look at webadmin in a web browser
    And I click on the Console tab in webadmin
    And I click on the "HTTP" link in webadmin
    
    Then An element should appear that can be found by the xpath //p[contains(.,"HTTP Console")]
    
  Scenario: Get data url
    Given I have a neo4j server running
    When I look at webadmin in a web browser
    And I click on the Console tab in webadmin
    And I click on the "HTTP" link in webadmin
    And I type "GET /db/data/" into the element found by the xpath //input[@id="console-input"]
    And I hit return in the element found by the xpath //input[@id="console-input"]
    Then An element should appear that can be found by the xpath //li[contains(.,"200")]
    
  Scenario: Get missing URL
    Given I have a neo4j server running
    When I look at webadmin in a web browser
    And I click on the Console tab in webadmin
    And I click on the "HTTP" link in webadmin
    And I type "GET /asd/asd/" into the element found by the xpath //input[@id="console-input"]
    And I hit return in the element found by the xpath //input[@id="console-input"]
    Then An element should appear that can be found by the xpath //li[contains(.,"404")]
    
  Scenario: Enter invalid statement
    Given I have a neo4j server running
    When I look at webadmin in a web browser
    And I click on the Console tab in webadmin
    And I click on the "HTTP" link in webadmin
    And I type "blus 12" into the element found by the xpath //input[@id="console-input"]
    And I hit return in the element found by the xpath //input[@id="console-input"]
    Then An element should appear that can be found by the xpath //li[contains(.,"Invalid")]
    
  Scenario: Enter invalid json
    Given I have a neo4j server running
    When I look at webadmin in a web browser
    And I click on the Console tab in webadmin
    And I click on the "HTTP" link in webadmin
    And I type "POST / {blah}" into the element found by the xpath //input[@id="console-input"]
    And I hit return in the element found by the xpath //input[@id="console-input"]
    Then An element should appear that can be found by the xpath //li[contains(.,"Invalid")]
