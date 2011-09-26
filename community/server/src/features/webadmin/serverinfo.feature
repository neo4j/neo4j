Feature: Webadmin provides a JMX browser for looking at detailed server information
  In order to gain detailed knowledge of the internal state of the neo4j server
  As a Neo4j user
  I want a JMX browser in webadmin

  Scenario: Look at JMX bean detail
    Given I have a neo4j server running

    When I look at webadmin in a web browser
    And I click on the Server info tab in webadmin
    And I click on the "Primitive count" link in webadmin

    Then An element should appear that can be found by the xpath //h2[contains(.,"Primitive count")]
