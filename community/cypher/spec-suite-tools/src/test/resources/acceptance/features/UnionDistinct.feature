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

Feature: UnionDistinct

  # The following scenarios mirror
  # openCypher/tck/features/clauses/union/Union1.feature
  # with explicit DISTINCT added

  Scenario Outline: Two elements, both unique, distinct
    Given an empty graph
    When executing query:
      """
      RETURN 1 AS x
      UNION <distinct>
      RETURN 2 AS x
      """
    Then the result should be, in any order:
      | x |
      | 1 |
      | 2 |
    And no side effects
    Examples:
      | distinct |
      | DISTINCT |
      |          |

  Scenario Outline: Three elements, two unique, distinct
    Given an empty graph
    When executing query:
      """
      RETURN 2 AS x
      UNION <distinct1>
      RETURN 1 AS x
      UNION <distinct2>
      RETURN 2 AS x
      """
    Then the result should be, in any order:
      | x |
      | 2 |
      | 1 |
    And no side effects
    Examples:
      | distinct1 | distinct2 |
      | DISTINCT  | DISTINCT  |
      | DISTINCT  |           |
      |           | DISTINCT  |
      |           |           |

  Scenario Outline: Two single-column inputs, one with duplicates, distinct
    Given an empty graph
    When executing query:
      """
      UNWIND [2, 1, 2, 3] AS x
      RETURN x
      UNION <distinct>
      UNWIND [3, 4] AS x
      RETURN x
      """
    Then the result should be, in any order:
      | x |
      | 2 |
      | 1 |
      | 3 |
      | 4 |
    And no side effects
    Examples:
      | distinct |
      | DISTINCT |
      |          |

  Scenario Outline: Should be able to create text output from union queries
    Given an empty graph
    And having executed:
      """
      CREATE (:A), (:B)
      """
    When executing query:
      """
      MATCH (a:A)
      RETURN a AS a
      UNION <distinct>
      MATCH (b:B)
      RETURN b AS a
      """
    Then the result should be, in any order:
      | a    |
      | (:A) |
      | (:B) |
    And no side effects
    Examples:
      | distinct |
      | DISTINCT |
      |          |

  Scenario Outline: Failing when UNION has different columns
    Given any graph
    When executing query:
      """
      RETURN 1 AS a
      UNION <distinct>
      RETURN 2 AS b
      """
    Then a SyntaxError should be raised at compile time: DifferentColumnsInUnion
    Examples:
      | distinct |
      | DISTINCT |
      |          |

  # The following scenario mirrors
  # openCypher/tck/features/clauses/union/Union3.feature
  # with explicit DISTINCT added

  Scenario Outline: Failing when mixing UNION ALL and UNION
    Given any graph
    When executing query:
      """
      RETURN 1 AS a
      UNION <setQuantifier1>
      RETURN 2 AS a
      UNION <setQuantifier2>
      RETURN 3 AS a
      """
    Then a SyntaxError should be raised at compile time: InvalidClauseComposition
    Examples:
      | setQuantifier1 | setQuantifier2 |
      | ALL            | DISTINCT       |
      | ALL            |                |
      | DISTINCT       | ALL            |
      |                | ALL            |
