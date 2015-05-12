Feature: The First

  Scenario: Simple match
    Given init: CREATE ({name: "Andres"});
    When running: MATCH (n) RETURN n.name AS name;
    Then result:
      | name |
      | Andres |
