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

Feature: InsertAcceptance

  // These scenarios are a subset of the openCypher TCK tests for CREATE

  Scenario: [1] Insert a single node
    Given any graph
    When executing query:
      """
      INSERT ()
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes | 1 |

  Scenario: [2] Insert two nodes
    Given any graph
    When executing query:
      """
      INSERT (), ()
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes | 2 |

  Scenario: [3] Insert a single node with a label
    Given an empty graph
    When executing query:
      """
      INSERT (:Label)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 1 |
      | +labels | 1 |

  Scenario: [4] Insert a single node with multiple labels
    Given an empty graph
    When executing query:
      """
      INSERT (:A&B&C&D)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 1 |
      | +labels | 4 |

  Scenario: [5] Insert three nodes with multiple labels
    Given an empty graph
    When executing query:
      """
      INSERT (:B&A&D), (:B&C), (:D&E&B)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 3 |
      | +labels | 5 |

  Scenario: [6] Insert a single node with two properties and return them
    Given any graph
    When executing query:
      """
      INSERT (n {id: 12, name: 'foo'})
      RETURN n.id AS id, n.name AS p
      """
    Then the result should be, in any order:
      | id | p     |
      | 12 | 'foo' |
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 2 |

  Scenario: [7] Insert a single node with null properties should not return those properties
    Given any graph
    When executing query:
      """
      INSERT (n {id: 12, name: null})
      RETURN n.id AS id, n.name AS p
      """
    Then the result should be, in any order:
      | id | p    |
      | 12 | null |
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 1 |

  Scenario: [8] Insert two nodes and a single relationship in a single pattern
    Given any graph
    When executing query:
      """
      INSERT ()-[:R]->()
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |

  Scenario: [9] Insert two nodes and a single relationship in separate patterns
    Given any graph
    When executing query:
      """
      INSERT (a), (b),
             (a)-[:R]->(b)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |

  Scenario: [10] Insert two nodes and a single relationship in separate clauses
    Given any graph
    When executing query:
      """
      INSERT (a)
      INSERT (b)
      INSERT (a)-[:R]->(b)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |


  Scenario: [11] Insert two nodes and a single relationship in the reverse direction
    Given an empty graph
    When executing query:
      """
      INSERT (:A)<-[:R]-(:B)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |
      | +labels        | 2 |
    When executing control query:
      """
      MATCH (a:A)<-[:R]-(b:B)
      RETURN a, b
      """
    Then the result should be, in any order:
      | a    | b    |
      | (:A) | (:B) |

  Scenario: [12] Insert a single relationship between two existing nodes
    Given an empty graph
    And having executed:
      """
      INSERT (:X)
      INSERT (:Y)
      """
    When executing query:
      """
      MATCH (x:X), (y:Y)
      INSERT (x)-[:R]->(y)
      """
    Then the result should be empty
    And the side effects should be:
      | +relationships | 1 |

    Scenario: [13] Insert a single relationship with two properties and return them
    Given any graph
    When executing query:
      """
      INSERT ()-[r:R {id: 12, name: 'foo'}]->()
      RETURN r.id AS id, r.name AS name
      """
    Then the result should be, in any order:
      | id | name  |
      | 12 | 'foo' |
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |
      | +properties    | 2 |

  Scenario: [14] Insert a single node and a single self loop in a single pattern
    Given any graph
    When executing query:
      """
      INSERT (root)-[:LINK]->(root)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 1 |
      | +relationships | 1 |

  Scenario: [15] Insert a pattern with multiple hops and varying directions
    Given an empty graph
    When executing query:
      """
      INSERT (:A)<-[:R1]-(:B)-[:R2]->(:C)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 3 |
      | +relationships | 2 |
      | +labels        | 3 |
    When executing control query:
      """
      MATCH (a:A)<-[r1:R1]-(b:B)-[r2:R2]->(c:C)
      RETURN *
      """
    Then the result should be, in any order:
      | a    | b    | c    | r1    | r2    |
      | (:A) | (:B) | (:C) | [:R1] | [:R2] |

  Scenario: [16] WITH-INSERT
    Given an empty graph
    And having executed:
      """
      INSERT (), ()
      """
    When executing query:
      """
      MATCH ()
      INSERT ()
      WITH *
      INSERT ()
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes | 4 |


  Scenario: [17] WITH-UNWIND-INSERT: A bound node should be recognized after projection with WITH + UNWIND
    Given any graph
    When executing query:
      """
      INSERT (a)
      WITH a
      UNWIND [0] AS i
      INSERT (b)
      INSERT (a)<-[:T]-(b)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |

  Scenario: [18] Merge followed by multiple inserts
    Given an empty graph
    When executing query:
      """
      MERGE (t:T {id: 42})
      INSERT (f:R)
      INSERT (t)-[:REL]->(f)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |
      | +labels        | 2 |
      | +properties    | 1 |


  Scenario: [19] Fail when inserting a relationship without a direction
    Given any graph
    When executing query:
      """
      INSERT (a)-[:FOO]-(b)
      """
    Then a SyntaxError should be raised at compile time: RequiresDirectedRelationship

  Scenario: [20] Fail when inserting a relationship with two directions
    Given any graph
    When executing query:
      """
      INSERT (a)<-[:FOO]->(b)
      """
    Then a SyntaxError should be raised at compile time: RequiresDirectedRelationship


  Scenario: [21] Fail when inserting a node that is already bound
    Given any graph
    When executing query:
      """
      MATCH (a)
      INSERT (a)
      """
    Then a SyntaxError should be raised at compile time: VariableAlreadyBound

  Scenario: [22] Fail when adding a new label predicate on a node that is already bound
    Given an empty graph
    When executing query:
      """
      INSERT (n:Foo)-[:T1]->(),
             (n:Bar)-[:T2]->()
      """
    Then a SyntaxError should be raised at compile time: VariableAlreadyBound

  Scenario: [23] Fail when inserting a relationship that is already bound
    Given any graph
    When executing query:
      """
      MATCH ()-[r]->()
      INSERT ()-[r:R]->()
      """
    Then a SyntaxError should be raised at compile time: VariableAlreadyBound

  Scenario: [24] Fail when inserting a node using undefined variable in pattern
    Given any graph
    When executing query:
      """
      INSERT (b {name: missing})
      RETURN b
      """
    Then a SyntaxError should be raised at compile time: UndefinedVariable
