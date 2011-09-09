Feature: Webadmin offers user-defined profiles to control visualization look-and-feel
  In order to get an easy overview of my data
  As a Neo4j user
  I want to be able to define profiles for the visualization that determine how nodes and relationships are rendered.

  Scenario: Create a new profile
    Given I have a neo4j server running

    When I look at the webadmin data browser in a web browser
    And I click on the button found by the xpath //div[@id="data-switch-view"]
    And I click on the button found by the xpath //a[@id="visualization-show-settings"]
    
    And I click on the "New profile" link in webadmin
    And I type "myprofile" into the element found by the xpath //input[@id="profile-name"]
    
    And I click on the button found by the xpath //button[contains(.,'Save')]
    
    Then An element should appear that can be found by the xpath //td[contains(.,'myprofile')]

  Scenario: Abort profile changes
    Given I have a neo4j server running

    When I look at the webadmin data browser in a web browser
    And I click on the button found by the xpath //div[@id="data-switch-view"]
    And I click on the button found by the xpath //a[@id="visualization-show-settings"]
    
    And I click on the "New profile" link in webadmin
    And I type "myprofile" into the element found by the xpath //input[@id="profile-name"]
    
    And I click yes on all upcoming confirmation boxes
    
    And I click on the button found by the xpath //button[contains(.,'Cancel')]
    
    Then An element should appear that can be found by the xpath //h1[contains(.,'Visualization settings')]

  Scenario: Delete a profile
    
    Given I have a neo4j server running

    When I look at the webadmin data browser in a web browser
    And I click on the button found by the xpath //div[@id="data-switch-view"]
    And I click on the button found by the xpath //a[@id="visualization-show-settings"]
    
    And I click on the "New profile" link in webadmin
    And I type "myprofile" into the element found by the xpath //input[@id="profile-name"]
    
    And I click on the button found by the xpath //button[contains(.,'Save')]
    
    And I click on the button found by the xpath //button[contains(../../td,'myprofile') and contains(.,'Delete')]
    
    Then The element found by xpath //td[contains(.,'myprofile')] should disappear
    
  
