Feature: Webadmin can remove node and relationship indexes
  In order to easily remove indexes
  As a Neo4j user
  I want to be able to remove node and relationship indexes with webadmin.

  Scenario: Remove node index
    Given I have a neo4j server running

    When I look at webadmin in a web browser
    And I click on the Index manager tab in webadmin
    And I type mynodeindex into the element found by the xpath //input[@id="create-node-index-name"]
    And I click on the button found by the xpath //button[@class="create-node-index button"]

    Then A single element should appear that can be found by the xpath //*[@id="node-indexes"]//td[contains(.,'mynodeindex')]
  
    When I click yes on all upcoming confirmation boxes 
    And I click on the button found by the xpath //*[@id='node-indexes']//tr[contains(.,"mynodeindex")]//button[contains(.,"Delete")]
    Then The element found by xpath //*[@id="node-indexes"]//td[contains(.,'mynodeindex')] should disappear

  Scenario: Remove relationship index
    Given I have a neo4j server running

    When I look at webadmin in a web browser
    And I click on the Index manager tab in webadmin
    And I type myrelindex into the element found by the xpath //input[@id="create-rel-index-name"]
    And I click on the button found by the xpath //button[@class="create-rel-index button"]

    Then A single element should appear that can be found by the xpath //*[@id="rel-indexes"]//td[contains(.,'myrelindex')]

    When I click yes on all upcoming confirmation boxes 
    And I click on the button found by the xpath //*[@id='rel-indexes']//tr[contains(.,"myrelindex")]//button[contains(.,"Delete")]
    Then The element found by xpath //*[@id="rel-indexes"]//td[contains(.,'myrelindex')] should disappear
