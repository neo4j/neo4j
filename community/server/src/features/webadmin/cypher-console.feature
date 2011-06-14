Feature: Webadmin has a cypher console
  In order to perform advanced data operations with webadmin
  As a Neo4j user
  I want a console in webadmin that allows me to use the cypher graph language to manipulate the database

  Scenario: View cypher console
    Given I have a neo4j server running
    When I look at webadmin in a web browser
    And I click on the Console tab in webadmin
    And I click on the Cypher link in webadmin
    
    Then An element should appear that can be found by the xpath //p[contains(.,"Cypher query language")]

  Scenario: Cypher remains after switching pages
    Given I have a neo4j server running
    When I look at webadmin in a web browser
    And I click on the Console tab in webadmin
    And I click on the Cypher link in webadmin
    
    And I click on the Console tab in webadmin

    Then An element should appear that can be found by the xpath //p[contains(.,"Cypher query language")]
