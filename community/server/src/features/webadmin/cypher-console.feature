Feature: Webadmin has a cypher console
  In order to perform advanced data operations with webadmin
  As a Neo4j user
  I want a console in webadmin that allows me to use the cypher graph language to manipulate the database

  Scenario: View cypher console
    Given I have a neo4j server running
    When I look at webadmin in a web browser
    And I click on the Console tab in webadmin
    And I click on the "Cypher" link in webadmin
    
    Then An element should appear that can be found by the xpath //p[contains(.,"Cypher query language")]

  Scenario: Cypher remains after switching pages
    Given I have a neo4j server running
    When I look at webadmin in a web browser
    And I click on the Console tab in webadmin
    And I click on the "Cypher" link in webadmin
    
    And I click on the Console tab in webadmin

    Then An element should appear that can be found by the xpath //p[contains(.,"Cypher query language")]
  # We cant currently run this, parenthesis end up
  # as return keys when picked up in js land. Not sure
  # why.
  #Scenario: Multi line input
  #  Given I have a neo4j server running
  #  When I look at webadmin in a web browser
  #  And I click on the Console tab in webadmin
  #  And I click on the "Cypher" link in webadmin
  #  
  # 
  #  And I type "start a=(0)" into the element found by the xpath //input[@id="console-input"]
  #  And I hit return in the element found by the xpath //input[@id="console-input"]
  #
  #  And I type "return a" into the element found by the xpath //input[@id="console-input"]
  #  And I hit return in the element found by the xpath //input[@id="console-input"]
  #  
  #  # Hit enter again, triggers execution of statement
  #  And I hit return in the element found by the xpath //input[@id="console-input"]
  #  
  #  Then An element should appear that can be found by the xpath //li[contains(.,"Node[0]")]
