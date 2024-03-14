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

Feature: Finish

  Scenario: FINISH alone
    Given an empty graph
    When executing query:
      """
      FINISH
      """
    Then the result should be empty
    And no side effects

  Scenario: FINISH after non-reading with cardinality equal zero and one variable
    Given an empty graph
    When executing query:
      """
      UNWIND [] AS x
      FINISH
      """
    Then the result should be empty
    And no side effects

  Scenario: FINISH after non-reading with cardinality equal zero and multiple variables
    Given an empty graph
    When executing query:
      """
      UNWIND [] AS x
      UNWIND [1, 2, 3] AS y
      FINISH
      """
    Then the result should be empty
    And no side effects

  Scenario: FINISH after non-reading with cardinality greater one and one variable
    Given an empty graph
    When executing query:
      """
      UNWIND [1, 2, 3] AS x
      FINISH
      """
    Then the result should be empty
    And no side effects

  Scenario: FINISH after non-reading with cardinality greater one and multiple variables
    Given an empty graph
    When executing query:
      """
      UNWIND [1, 2, 3] AS x
      UNWIND [1, 2, 3] AS y
      FINISH
      """
    Then the result should be empty
    And no side effects

  Scenario: FINISH after reading with cardinality equal zero and one variable
    Given having executed:
      """
      CREATE (:A), (:B), (:C)
      """
    When executing query:
      """
      MATCH (n) WHERE 0 = 1
      FINISH
      """
    Then the result should be empty
    And no side effects

  Scenario: FINISH after reading with cardinality equal zero and multiple variables
    Given having executed:
      """
      CREATE (:A), (:B), (:C)
      """
    When executing query:
      """
      MATCH (n), (m)
      MATCH (o) WHERE 0 = 1
      FINISH
      """
    Then the result should be empty
    And no side effects

  Scenario: FINISH after reading with cardinality greater one and one variable
    Given having executed:
      """
      CREATE (:A), (:B), (:C)
      """
    When executing query:
      """
      MATCH (n)
      FINISH
      """
    Then the result should be empty
    And no side effects

  Scenario: FINISH after reading with cardinality greater one and multiple variables
    Given having executed:
      """
      CREATE (:A), (:B), (:C)
      """
    When executing query:
      """
      UNWIND [1, 2, 3] AS x
      MATCH (n)
      MATCH (m)
      FINISH
      """
    Then the result should be empty
    And no side effects

  Scenario: FINISH after side effect-free updating
    Given an empty graph
    When executing query:
      """
      DELETE null
      FINISH
      """
    Then the result should be empty
    And no side effects

  Scenario: FINISH after updating with cardinality one and one variable
    Given an empty graph
    When executing query:
      """
      CREATE (a:A)
      FINISH
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 1 |
      | +labels | 1 |

  Scenario: FINISH after updating with cardinality one and multiple variables
    Given an empty graph
    When executing query:
      """
      CREATE (a:A), (b:B), (c:C)
      FINISH
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 3 |
      | +labels | 3 |

  Scenario: FINISH after reading and updating
    Given having executed:
      """
      CREATE (:A), (:B), (:C)
      """
    When executing query:
      """
      MATCH (n)
      CREATE (n)-[:R {p: 1}]->(x:X)
      FINISH
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 3 |
      | +relationships | 3 |
      | +properties    | 3 |
      | +labels        | 1 |

  Scenario: FINISH after updating and reading in multiple part query
    Given an empty graph
    When executing query:
      """
      CREATE (a:A), (b:B), (c:C)
      WITH COUNT(*) AS cnt
      MATCH (n)
      FINISH
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 3 |
      | +labels | 3 |

  Scenario: FINISH immediately after WITH only
    Given an empty graph
    When executing query:
      """
      WITH 123 AS foo
      FINISH
      """
    Then the result should be empty
    And no side effects

  Scenario: FINISH immediately after WITH after updating and reading
    Given an empty graph
    When executing query:
      """
      CREATE (a:A), (b:B), (c:C)
      WITH COUNT(*) AS cnt
      MATCH (n)
      WITH labels(n) AS l
      FINISH
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 3 |
      | +labels | 3 |

  Scenario: FINISH in UNION operands alone
    Given an empty graph
    When executing query:
      """
      FINISH
      UNION
      FINISH
      """
    Then the result should be empty
    And no side effects

  Scenario: FINISH in UNION reading operands
    Given having executed:
      """
      CREATE (:A), (:B), (:C)
      """
    When executing query:
      """
      MATCH (a)
      FINISH
      UNION
      MATCH (b), (c)
      FINISH
      """
    Then the result should be empty
    And no side effects

  Scenario Outline: FINISH in UNION updating operands
    Given an empty graph
    When executing query:
      """
      <lhs>
      UNION
      <rhs>
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes | 4 |
    Examples:
      | lhs                                       | rhs                                       |
      | UNWIND [1, 2] AS x CREATE (a), (b) FINISH | UNWIND [1, 2] AS x FINISH                 |
      | UNWIND [1, 2] AS x FINISH                 | UNWIND [1, 2] AS x CREATE (a), (b) FINISH |
      | CREATE (a), (b) FINISH                    | UNWIND [1, 2] AS x CREATE (c) FINISH      |
      | UNWIND [1, 2] AS x CREATE (c) FINISH      | CREATE (a), (b) FINISH                    |

  Scenario Outline: FINISH in UNION updating operands in combination with DELETE null
    Given an empty graph
    When executing query:
      """
      <lhs>
      UNION
      <rhs>
      """
    Then the result should be empty
    And no side effects
    Examples:
      | lhs                   | rhs                   |
      | FINISH                | DELETE null           |
      | DELETE null           | FINISH                |
      | FINISH                | MATCH (n) DELETE null |
      | MATCH (n) DELETE null | FINISH                |
      | MATCH (n) FINISH      | DELETE null           |
      | DELETE null           | MATCH (n) FINISH      |

  Scenario Outline: FINISH in UNION updating operands in combination with DELETE null and side effects
    Given an empty graph
    When executing query:
      """
      <lhs>
      UNION
      <rhs>
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes | 1 |
    Examples:
      | lhs                    | rhs                    |
      | FINISH                 | CREATE (n) DELETE null |
      | CREATE (n) DELETE null | FINISH                 |
      | CREATE (n) FINISH      | DELETE null            |
      | DELETE null            | CREATE (n) FINISH      |

  Scenario Outline: FINISH in only on UNION operand
    Given an empty graph
    When executing query:
      """
      <lhs>
      UNION
      <rhs>
      """
    Then a SyntaxError should be raised at compile time: *
    And no side effects
    Examples:
      | lhs                                      | rhs                                      |
      | FINISH                                   | RETURN 1 AS a                            |
      | RETURN 1 AS a                            | FINISH                                   |
      | MATCH (n) FINISH                         | MATCH (n) RETURN n                       |
      | MATCH (n) RETURN n                       | MATCH (n) FINISH                         |
      | MATCH (n) UNWIND [1, 2] AS a FINISH      | MATCH (n) UNWIND [1, 2] AS a RETURN n, a |
      | MATCH (n) UNWIND [1, 2] AS a RETURN n, a | MATCH (n) UNWIND [1, 2] AS a FINISH      |
