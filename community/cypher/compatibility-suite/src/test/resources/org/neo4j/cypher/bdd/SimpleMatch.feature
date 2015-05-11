Feature: The First

  Scenario: Simple match
    Given init: CREATE ({name: "apa"});
    When running: MATCH (n) RETURN n.name AS name;
    Then result:
      | name |
      | apa  |
