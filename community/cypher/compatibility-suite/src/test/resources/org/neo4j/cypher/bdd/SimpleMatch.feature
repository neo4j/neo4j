@db:cineast
Feature: The First

  Scenario: Simple match
    Given using: cineast
    When running: MATCH (u:User {login: 'emileifrem'}) RETURN u.name AS name;
    Then result:
      | name |
      | Emil Eifrem |
