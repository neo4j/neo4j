#
# Copyright (c) 2002-2017 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

Feature: OrderByAcceptance

  Scenario: ORDER BY nodes should return null results last in ascending order
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:REL]->(:B),
             (:A)
      """
    When executing query:
      """
      MATCH (a:A)
      OPTIONAL MATCH (a)-[:REL]->(b:B)
      RETURN b
      ORDER BY b
      """
    Then the result should be, in order:
      | b    |
      | (:B) |
      | null |
    And no side effects

  Scenario: ORDER BY relationships should return null results last in ascending order
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:REL]->(:B),
             (:A)
      """
    When executing query:
      """
      MATCH (a:A)
      OPTIONAL MATCH (a)-[r:REL]->()
      RETURN r
      ORDER BY r
      """
    Then the result should be, in order:
      | r      |
      | [:REL] |
      | null   |
    And no side effects

  # The ORDER BY should work even if the "WITH x" will not become a `Projection` in the logical plan
  Scenario: ORDER BY with unwind primitive integer
    Given any graph
    When executing query:
      """
      WITH [4, 3, 1, 2] AS lst
      UNWIND lst AS x
      WITH x
      ORDER BY x
      RETURN x
      """
    Then the result should be, in order:
      | x |
      | 1 |
      | 2 |
      | 3 |
      | 4 |

    And no side effects

  Scenario: ORDER BY two node properties
    Given an empty graph
    And having executed:
      """
      CREATE (:L {a: 3, b: "a"}),
             (:L {a: 1, b: "b"}),
             (:L {a: 3, b: "c"}),
             (:L {a: 4, b: "d"}),
             (:L {a: 2, b: "e"})
      """
    When executing query:
      """
      MATCH (n:L)
      WITH n.a AS a, n.b AS b
      ORDER BY a, b DESC
      RETURN a, b
      """
    Then the result should be, in order:
      |  a  |  b  |
      |  1  | 'b' |
      |  2  | 'e' |
      |  3  | 'c' |
      |  3  | 'a' |
      |  4  | 'd' |

    And no side effects
