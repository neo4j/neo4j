Feature: Webadmin data browser allows me to see and change details of a relationship
  In order to gain insight and maintain my neo4j data
  As a Neo4j user
  I want to use the webadmin data browser to see and modify relationship details

  Scenario: Create relationhips in webadmin
    Given I have a neo4j server running
    And I have created a node through webadmin

    When I look at the webadmin data browser in a web browser
    And I click on the "Relationship" button in webadmin
    And I type "1" into the element found by the xpath //input[@id="create-relationship-to"]
    And I click on the "Create" button in webadmin
    
    Then The data browser item subtitle should change to http://.+:7474/db/data/relationship/[0-9]+

  Scenario: Find relationnship by id in webadmin
    Given I have a neo4j server running
    And I have created a relationship through webadmin
    When I look at the webadmin data browser in a web browser
    And I enter rel:0 into the data browser search field
    Then The data browser item subtitle should be http://.+:7474/db/data/relationship/0

  Scenario: Create relationship properties in webadmin
    Given I have a neo4j server running
    And I have created a relationship through webadmin
    
    When I look at the webadmin data browser in a web browser
    And I enter rel:0 into the data browser search field
    Then The data browser item subtitle should be http://.+:7474/db/data/relationship/0
    
    When I click on the "Add property" button in webadmin
    Then An element should appear that can be found by the xpath //li[1]/ul/li//input[@class="property-key"]
    
    When I type "mykey" into the element found by the xpath //li[1]/ul/li//input[@class="property-key"]
    And I type "12" into the element found by the xpath //li[1]/ul/li//input[@class="property-value"]
    And I hit return in the element found by the xpath //li[1]/ul/li//input[@class="property-value"]
    Then The databrowser save button should change to saying Saved
    And The currently visible relationship in webadmin should have a property mykey with the value 12


  
    
