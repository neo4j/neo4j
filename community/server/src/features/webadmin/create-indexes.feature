Feature: Webadmin can create node and relationship indexes
  In order to easily create indexes
  As a Neo4j user
  I want to be able to create node and relationship indexes with webadmin.

  Scenario: Create node index
    Given I have a neo4j server running

    When I look at webadmin in a web browser
    And I click on the Index manager tab in webadmin
    And I type mynodeindex into the element found by the xpath //input[@id="create-node-index-name"]
    And I click on the button found by the xpath //button[@class="create-node-index button"]

    Then A single element should appear that can be found by the xpath //*[@id="node-indexes"]//td[contains(.,'mynodeindex')]
  
  Scenario: Create relationship index
    Given I have a neo4j server running

    When I look at webadmin in a web browser
    And I click on the Index manager tab in webadmin
    And I type myrelindex into the element found by the xpath //input[@id="create-rel-index-name"]
    And I click on the button found by the xpath //button[@class="create-rel-index button"]

    Then A single element should appear that can be found by the xpath //*[@id="rel-indexes"]//td[contains(.,'myrelindex')]
