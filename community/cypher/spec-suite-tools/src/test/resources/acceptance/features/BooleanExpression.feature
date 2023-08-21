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

Feature: BooleanExpression

  Scenario: Combining IN predicates intersected with OR
    Given any graph
    When executing query:
      """
      RETURN (1 IN [1] AND FALSE) OR 1 in [2] AS result
      """
    Then the result should be, in any order:
      | result |
      | false  |
    And no side effects

  Scenario: Combining IN predicates intersected with AND
    Given any graph
    When executing query:
      """
      RETURN (1 IN [2] OR TRUE) AND 1 in [1] AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects

  Scenario: Disjunction of NULL and EXISTS
    Given an empty graph
    And having executed:
      """
      CREATE (:A)
      """
    When executing query:
      """
      RETURN
        NULL OR false AS n1,
        NULL OR EXISTS { (:XYZ) } AS n2,
        NULL OR NOT EXISTS { (:A) } AS n3,

        NULL OR true AS t1,
        NULL OR EXISTS { (:A) } AS t2,
        NULL OR NOT EXISTS { (:XYZ) } AS t3
      """
    Then the result should be, in any order:
      | n1    | n2     | n3    | t1   | t2   | t3   |
      | null  | null   | null  | true | true | true |

  Scenario: XOR of NULL and EXISTS
    Given an empty graph
    And having executed:
      """
      CREATE (:A)
      """
    When executing query:
      """
      RETURN
        NULL XOR false AS n1,
        NULL XOR true AS n2,
        NULL XOR EXISTS { (:A) } AS n3,
        NULL XOR EXISTS { (:XYZ) } AS n4,
        NULL XOR NOT EXISTS { (:A) } AS n5,
        NULL XOR NOT EXISTS { (:XYZ) } AS n6
      """
    Then the result should be, in any order:
      | n1    | n2     | n3    | n4   | n5   | n6   |
      | null  | null   | null  | null | null | null |
