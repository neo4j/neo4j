Feature: Webadmin data browser allows me to see and change details for a node
  In order to gain insight and maintain my neo4j data
  As a Neo4j user
  I want to use the webadmin data browser to see and modify node details

  Scenario: Reference node is shown as default view in webadmins data browser
    Given I have a neo4j server running
    When I look at webadmin in a web browser
    And I click on the Data browser tab in webadmin
    Then The data browser item subtitle should be http://.+:7474/db/data/node/0

  Scenario: Find node by id in webadmin
    Given I have a neo4j server running
    And I have created a node through webadmin
    When I look at the webadmin data browser in a web browser
    And I enter node:1 into the data browser search field
    Then The data browser item subtitle should be http://.+:7474/db/data/node/1

  Scenario: Find node by plain id in webadmin
    Given I have a neo4j server running
    And I have created a node through webadmin
    When I look at the webadmin data browser in a web browser
    And I enter 1 into the data browser search field
    Then The data browser item subtitle should be http://.+:7474/db/data/node/1

  Scenario: Create nodes in webadmin
    Given I have a neo4j server running
    When I look at the webadmin data browser in a web browser
    And I click on the "Node" button in webadmin
    Then The data browser item subtitle should change from http://0.0.0.0:7474/db/data/node/0
    And The data browser item subtitle should be http://.+:7474/db/data/node/[0-9]+

  Scenario: Create node properties in webadmin
    Given I have a neo4j server running
    When I look at the webadmin data browser in a web browser
    When I click on the "Add property" button in webadmin
    Then An element should appear that can be found by the xpath //li[1]/ul/li//input[@class="property-key"]
    
    When I type "mykey" into the element found by the xpath //li[1]/ul/li//input[@class="property-key"]
    And I type "12" into the element found by the xpath //li[1]/ul/li//input[@class="property-value"]
    And I hit return in the element found by the xpath //li[1]/ul/li//input[@class="property-value"]
    Then The databrowser save button should change to saying Saved
    And The currently visible node in webadmin should have a property mykey with the value 12

    
