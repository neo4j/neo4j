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
Feature: DynamicPropertiesAcceptance

  Scenario: Set single dynamic property on nodes
    Given an empty graph
    And having executed:
       """
       CREATE (:A)
       """
    When executing query:
       """
       WITH "foo" AS a
       MATCH (n)
       SET n[a] = 1
       RETURN n.foo
       """
    Then the result should be, in any order:
      | n.foo |
      | 1     |
    And the side effects should be:
      | +properties | 1 |

  Scenario: Set single dynamic property on relationships
    Given an empty graph
    And having executed:
       """
       CREATE (:A)-[:R]->(:B)
       """
    When executing query:
       """
       WITH "foo" AS a
       MATCH ()-[r]->()
       SET r[a] = 1
       RETURN r.foo
       """
    Then the result should be, in any order:
      | r.foo |
      | 1     |
    And the side effects should be:
      | +properties | 1 |

  Scenario: Support parameters in setting dynamic properties
    Given an empty graph
    And parameters are:
      | a   | 'foo' |
      | val | 42    |

    When executing query:
      """
       CREATE (n)
       SET n[$a]=$val
       RETURN n.foo
      """
    Then the result should be, in any order:
      | n.foo |
      | 42    |
    And the side effects should be:
      | +properties | 1 |
      | +nodes      | 1 |

  Scenario: Copy all properties on an existing node
    Given an empty graph
    And having executed:
       """
       CREATE (:A{prop1: 1, prop2: 2})
       """
    When executing query:
       """
       MATCH (n)
       FOREACH (k IN keys(n) | SET n[k+"Copy"] = n[k])
       RETURN n
       """
    Then the result should be, in any order:
      | n                                                    |
      | (:A{prop1: 1, prop2: 2, prop1Copy: 1, prop2Copy: 2}) |
    And the side effects should be:
      | +properties | 2 |

  Scenario: Remove single dynamic property on nodes
    Given an empty graph
    And having executed:
       """
       CREATE (:A{foo: 1})
       """
    When executing query:
       """
       WITH "foo" AS a
       MATCH (n)
       REMOVE n[a]
       RETURN n.foo
       """
    Then the result should be, in any order:
      | n.foo |
      | null  |
    And the side effects should be:
      | -properties | 1 |

  Scenario: Remove single dynamic property on relationships
    Given an empty graph
    And having executed:
       """
       CREATE (:A)-[:R{foo:1}]->(:B)
       """
    When executing query:
       """
       WITH "foo" AS a
       MATCH ()-[r]->()
       REMOVE r[a]
       RETURN r.foo
       """
    Then the result should be, in any order:
      | r.foo |
      | null  |
    And the side effects should be:
      | -properties | 1 |

  Scenario: Rename all properties on an existing node
    Given an empty graph
    And having executed:
       """
       CREATE (:A{prop1: 1, prop2: 2})
       """
    When executing query:
       """
       MATCH (n)
       FOREACH (k IN keys(n) |
          SET n[upper(k)] = n[k]
          REMOVE n[k])
       RETURN n
       """
    Then the result should be, in any order:
      | n                        |
      | (:A{PROP1: 1, PROP2: 2}) |
    And the side effects should be:
      | +properties | 2 |
      | -properties | 2 |

  Scenario: Remove all matching properties on an existing node
    Given an empty graph
    And having executed:
       """
       CREATE (:A{prop1: 1, prop2: 2, foo: 42})
       """
    When executing query:
       """
       MATCH (n)
       WITH [k IN keys(n) WHERE k CONTAINS "prop" | k] AS propertyKeys, n
       FOREACH (i IN propertyKeys | REMOVE n[i])
       RETURN n
       """
    Then the result should be, in any order:
      | n             |
      | (:A{foo: 42}) |
    And the side effects should be:
      | -properties | 2 |

  Scenario Outline: Should throw syntax errors when setting properties using invalid constant expressions
    Given an empty graph
    When executing query:
      """
      MATCH (n)
      SET n[<invalid_expr>] = 1
      RETURN n
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | invalid_expr                      |
      | 1 + 2                             |
      | localdatetime("2024185T19:32:24") |
      | point({x:3,y:0})                  |
      | null                              |

  Scenario Outline: Should throw syntax errors when setting properties whose names are variables with invalid values
    Given an empty graph
    When executing query:
      """
      WITH <invalid_value> AS a
      MATCH (n)
      SET n[a] = 1
      RETURN n
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | invalid_value                     |
      | 1 + 2                             |
      | point({x: 2.3, y: 4.5})           |
      | true                              |
      | localdatetime("2024185T19:32:24") |
      | point({x:3,y:0})                  |

  Scenario: Should throw type errors when setting properties whose names are variables that evaluate to NULL
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      WITH NULL AS a
      MATCH (n)
      SET n[a] = 1
      RETURN n
      """
    Then a TypeError should be raised at runtime: *

  @allowCustomErrors
  Scenario: Should throw token errors when setting dynamic properties with variables with empty strings
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      WITH '' AS a
      MATCH (n)
      SET n[a] = 1
      RETURN n
      """
    Then a TokenNameError should be raised at runtime: *

  Scenario: Should throw type error if property name missing in the CSV file
    Given an empty graph
    And there exists a CSV file with URL as $param, with rows:
      | name    | role |
      | 'David' | ''   |

    When  executing query:
    """
    LOAD CSV WITH HEADERS FROM $param AS line
    CREATE (n {name: line.name})
    SET n[line.role] = true
    RETURN n
    """
    Then a TypeError should be raised at runtime: *

  Scenario Outline: Should throw syntax errors when setting properties where parameters evaluate to invalid values
    Given an empty graph
    And parameters are:
      | a | <invalid_param> |
    When executing query:
      """
      MATCH (n)
      SET n[$a]=1
      RETURN n
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | invalid_param    |
      | 1                |
      | {x: 2.3, y: 4.5} |
      | true             |

  Scenario: Should throw type errors when setting properties where parameters evaluate to null
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    And parameters are:
      | a | null |
    When executing query:
      """
      MATCH (n)
      SET n[$a]=1
      RETURN n
      """
    Then a TypeError should be raised at runtime: *

  @allowCustomErrors
  Scenario: Should throw token name errors when setting properties where parameters evaluate to empty strings
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    And parameters are:
      | a | '' |
    When executing query:
      """
      MATCH (n)
      SET n[$a]=1
      RETURN n
      """
    Then a TokenNameError should be raised at runtime: *

  Scenario Outline: Should throw syntax errors when removing properties using invalid constant expressions
    Given an empty graph
    When executing query:
      """
      MATCH (n)
      REMOVE n[<invalid_expr>]
      RETURN n
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | invalid_expr                      |
      | 1 + 2                             |
      | localdatetime("2024185T19:32:24") |
      | point({x:3,y:0})                  |

  Scenario: Should throw type errors when removing null dynamic properties
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      WITH NULL AS a
      MATCH (n)
      REMOVE n[a]
      RETURN n
      """
    Then a TypeError should be raised at runtime: *

  Scenario Outline: Should throw syntax errors when removing properties whose names are variables with invalid values
    Given an empty graph
    When executing query:
      """
      WITH <invalid_value> AS a
      MATCH (n)
      REMOVE n[a]
      RETURN n
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | invalid_value                     |
      | 1 + 2                             |
      | point({x: 2.3, y: 4.5})           |
      | true                              |
      | localdatetime("2024185T19:32:24") |

  Scenario Outline: Should throw syntax errors when removing properties where parameters evaluate to invalid values
    Given an empty graph
    And parameters are:
      | a | <invalid_param> |
    When executing query:
      """
      MATCH (n)
      REMOVE n[$a]
      RETURN n
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | invalid_param    |
      | 1                |
      | {x: 2.3, y: 4.5} |
      | true             |

  @allowCustomErrors
  Scenario: Should throw token error for setting properties where variables evaluate to invalid token values
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      WITH '' AS a
      MATCH (n)
      SET n[a] = 1
      RETURN n.a
      """
    Then a TokenNameError should be raised at runtime: *

  Scenario: Should throw type errors when removing properties where parameters evaluate to null
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    And parameters are:
      | a | null |
    When executing query:
      """
      MATCH (n)
      REMOVE n[$a]
      RETURN n
      """
    Then a TypeError should be raised at runtime: *