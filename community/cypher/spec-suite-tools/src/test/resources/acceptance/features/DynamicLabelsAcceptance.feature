#
# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
#
# Neo4j is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

#encoding: utf-8
@EnableSemanticFeature(DynamicProperties)
Feature: DynamicLabelsAcceptance

  Scenario Outline: Set dynamic labels
    Given an empty graph
    And having executed:
      """
      CREATE <existing_node>
      """
    When executing query:
      """
      WITH <label_definitions>
      MATCH (n)
      SET <query>
      RETURN labels(n) as newLabels
      """
    Then the result should be, in any order:
      | newLabels |
      | <labels>  |
    And the side effects should be:
      | +labels | <new_labels_count> |
    Examples:
      | existing_node | label_definitions           | query                            | labels     | new_labels_count |
      | ()            | "A" AS a                    | n:$(a)                           | ['A']      | 1                |
      | ()            | "A" AS a                    | n IS $(a)                        | ['A']      | 1                |
      | ()            | "A" AS a, "B" as b          | n:$(a):$(b)                      | ['A', 'B'] | 2                |
      | ()            | "A" AS a, "B" as b          | n IS $(a), n IS $(b)             | ['A', 'B'] | 2                |
      | ()            | 1 as numericLabel, "A" as a | n:$(a):$(toString(numericLabel)) | ['A','1']  | 2                |
      | (:A)          | "B" as b                    | n:$(b)                           | ['A', 'B'] | 1                |

  Scenario: Support parameters in dynamic labels
    Given an empty graph
    And parameters are:
      | a | 'A' |

    When executing query:
      """
       CREATE (n)
       SET n:$($a)
       RETURN labels(n) as newLabels
      """
    Then the result should be, in any order:
      | newLabels |
      | ['A']     |
    And the side effects should be:
      | +labels | 1 |
      | +nodes  | 1 |

  Scenario: Use property value as dynamic label
    Given an empty graph
    And having executed:
      """
      CREATE (:Movie {genre:"Horror"})
      """
    When executing query:
      """
      MATCH (n:Movie)
      SET n:$(n.genre)
      REMOVE n.genre
      RETURN labels(n) as newLabels
      """
    Then the result should be, in any order:
      | newLabels          |
      | ['Movie','Horror'] |
    And the side effects should be:
      | +labels     | 1 |
      | -properties | 1 |

  Scenario Outline: Set labels in a merge
    Given an empty graph
    And having executed:
      """
      CREATE <existing_node>
      """
    When executing query:
      """
      WITH "NewLabel" AS label, "OldLabel" AS label2
      MERGE (david:Person {name: "David"})
      ON CREATE
        SET david:$(label)
      ON MATCH
        SET david:$(label2)
      RETURN labels(david) AS newLabels
      """
    Then the result should be, in any order:
      | newLabels                   |
      | ['Person',<expected_label>] |
    And the side effects should be:
      | +labels     | <new_labels_count> |
      | +nodes      | <new_nodes_count>  |
      | +properties | <new_nodes_count>  |
    Examples:
      | existing_node             | expected_label | new_labels_count | new_nodes_count |
      | ()                        | 'NewLabel'     | 2                | 1               |
      | (:Person {name: "David"}) | 'OldLabel'     | 1                | 0               |

  Scenario: Take labels from the CSV file
    Given an empty graph
    And there exists a CSV file with URL as $param, with rows:
      | name       | role        |
      | 'David'    | 'ADMIN'     |
      | 'Tim'      | 'READ_ONLY' |
      | 'Gareth'   | 'READ_ONLY' |
      | 'Dawn'     | 'READ_ONLY' |
      | 'Jennifer' | 'ADMIN'     |

    When  executing query:
      """
      LOAD CSV WITH HEADERS FROM $param AS line
      CREATE (n {name: line.name})
      SET n:$(line.role)
      RETURN n
      """
    Then the result should be, in order:
      | n                             |
      | (:ADMIN {name: 'David'})      |
      | (:READ_ONLY {name: 'Tim'})    |
      | (:READ_ONLY {name: 'Gareth'}) |
      | (:READ_ONLY {name: 'Dawn'})   |
      | (:ADMIN {name: 'Jennifer'})   |
    And the side effects should be:
      | +nodes      | 5 |
      | +properties | 5 |
      | +labels     | 2 |

  Scenario Outline: Should throw syntax errors for setting labels using invalid constant expressions
    Given an empty graph
    When executing query:
      """
      MATCH (n)
      SET n:$(<invalid_expr>)
      RETURN labels(n) AS labels
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | invalid_expr |
      | 1 + 2        |
      | null         |
      | ''           |

  Scenario Outline: Should throw syntax errors when setting labels are variables with invalid values
    Given an empty graph
    When executing query:
      """
      WITH <invalid_value> AS a
      MATCH (n)
      SET n:$(a)
      RETURN labels(n) AS labels
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | invalid_value |
      | 1 + 2         |
      | true          |
      | {x : 1}       |

  Scenario: Should throw type error if labels missing in the CSV file
    Given an empty graph
    And there exists a CSV file with URL as $param, with rows:
      | name    | role |
      | 'David' | ''   |

    When  executing query:
    """
    LOAD CSV WITH HEADERS FROM $param AS line
    CREATE (n {name: line.name})
    SET n:$(line.role)
    RETURN n
    """
    Then a TypeError should be raised at runtime: *

  Scenario Outline: Should throw syntax errors for setting labels where parameters evaluate to invalid values
    Given an empty graph
    And parameters are:
      | a | <invalid_param> |
    When executing query:
      """
      MATCH (n)
      SET n:$($a)
      RETURN labels(n) AS labels
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | invalid_param |
      | 1             |
      | true          |
      | {x: 1}        |

  Scenario Outline: Should throw type errors when a node property being set as a dynamic label is invalid
    Given an empty graph
    And having executed:
      """
      CREATE (:A{prop:<invalid_value>})
      """
    When executing query:
      """
      MATCH (n)
      SET n:$(n.prop)
      RETURN labels(n) AS labels
      """
    Then a TypeError should be raised at runtime: *
    Examples:
      | invalid_value |
      | 1             |
      | null          |
      | false         |

  Scenario Outline: Should remove dynamic labels
    Given an empty graph
    And having executed:
      """
      CREATE <existing_node>
      """
    When executing query:
      """
      WITH <label_definitions>
      MATCH (n)
      REMOVE <query>
      RETURN labels(n) as newLabels
      """
    Then the result should be, in any order:
      | newLabels |
      | <labels>  |
    And the side effects should be:
      | -labels | <removed_labels_count> |
    Examples:
      | existing_node | label_definitions  | query                | labels | removed_labels_count |
      | (:A)          | "A" AS a           | n:$(a)               | []     | 1                    |
      | (:A)          | "A" AS a           | n IS $(a)            | []     | 1                    |
      | (:A:B)        | "A" AS a, "B" as b | n:$(a):$(b)          | []     | 2                    |
      | (:A:B)        | "A" AS a, "B" as b | n IS $(a), n IS $(b) | []     | 2                    |
      | (:A)          | "B" as b           | n:$(b)               | ['A']  | 0                    |

  Scenario: Set a list of labels
    Given an empty graph
    And having executed:
      """
      CREATE ({name:'Dave'})
      """
    When executing query:
      """
      WITH ["Person", "READ_ONLY"] AS labels
      MATCH (p)
      SET p:$(labels)
      RETURN p;
      """
    Then the result should be, in any order:
              | p                      |
              | (:Person:READ_ONLY{name:'Dave'}) |
            And the side effects should be:
              | +labels | 2 |

  Scenario: Remove a list of labels
    Given an empty graph
    And having executed:
      """
      CREATE (:Person:ADMIN{name:'Dave'})
      CREATE (:Person:READ_ONLY:EXTERNAL{name:'John'})
      """
    When executing query:
      """
      WITH ["ADMIN", "READ_ONLY", "EXTERNAL"] AS labels
      MATCH (p:Person)
      REMOVE p:$(labels)
      RETURN p;
      """
    Then the result should be, in any order:
          | p                      |
          | (:Person{name:'Dave'}) |
          | (:Person{name:'John'}) |
        And the side effects should be:
          | -labels | 3 |

  Scenario: Update labels
    Given an empty graph
    And having executed:
      """
      CREATE (:Person{name:'Dave'})
      """
    When executing query:
      """
      MATCH (n)
      WITH n, labels(n)[0] AS label
      REMOVE n:$(label)
      SET n:$(upper(label))
      RETURN n;
      """
    Then the result should be, in any order:
      | n                      |
      | (:PERSON{name:'Dave'}) |
    And the side effects should be:
      | -labels | 1 |
      | +labels | 1 |

  Scenario Outline: Should throw syntax errors for removing labels using invalid constant expressions
    Given an empty graph
    When executing query:
      """
      MATCH (n)
      REMOVE n:$(<invalid_expr>)
      RETURN labels(n) AS labels
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | invalid_expr |
      | 1 + 2        |
      | null         |
      | ''           |

  Scenario Outline: Should throw syntax errors when removing labels are variables with invalid values
    Given an empty graph
    When executing query:
      """
      WITH <invalid_value> AS a
      MATCH (n)
      REMOVE n:$(a)
      RETURN labels(n) AS labels
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | invalid_value  |
      | 1 + 2          |
      | true           |
      | {x : 1}        |

  Scenario Outline: Should throw syntax errors for removing labels where parameters evaluate to invalid values
    Given an empty graph
    And parameters are:
      | a | <invalid_param> |
    When executing query:
      """
      MATCH (n)
      REMOVE n:$($a)
      RETURN labels(n) AS labels
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | invalid_param    |
      | 1                |
      | {x: 2.3, y: 4.5} |
      | true             |

  Scenario Outline: Should throw type errors when a node property being removed as a dynamic label is invalid
    Given an empty graph
    And having executed:
      """
      CREATE (:A{prop:<invalid_value>})
      """
    When executing query:
      """
      MATCH (n)
      REMOVE n:$(n.prop)
      RETURN labels(n) AS labels
      """
    Then a TypeError should be raised at runtime: *
    Examples:
      | invalid_value |
      | 1             |
      | null          |
      | true          |

  @allowCustomErrors
  Scenario: Should throw token error for settings labels where parameters evaluate to invalid token values
    Given an empty graph
    And having executed:
      """
      CREATE (:A {prop:''})
      """
    When executing query:
      """
      MATCH (n)
      SET n:$(n.prop)
      RETURN labels(n) AS labels
      """
    Then a TokenNameError should be raised at runtime: *