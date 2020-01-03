#
# Copyright (c) 2002-2020 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
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

#encoding: utf-8

Feature: SubqueryAcceptance

  Background:
    Given an empty graph

  Scenario: CALL around single query - using returned var in outer query
    When executing query:
      """
      CALL { RETURN 1 as x } RETURN x
      """
    Then the result should be, in any order:
      | x |
      | 1 |
    And no side effects

  Scenario: Post-processing of a subquery result
    When executing query:
      """
      CALL { UNWIND [1, 2, 3, 4] AS x RETURN x } WITH x WHERE x > 2 RETURN sum(x) AS sum
      """
    Then the result should be, in any order:
      | sum |
      | 7   |
    And no side effects

  Scenario: Executes subquery for all incoming rows
    When executing query:
      """
      UNWIND [1, 2, 3] AS i
      CALL { RETURN 'x' as x } RETURN i, x
      """
    Then the result should be, in any order:
      | i | x   |
      | 1 | 'x' |
      | 2 | 'x' |
      | 3 | 'x' |
    And no side effects

  Scenario: CALLs in sequence
    When executing query:
      """
      CALL { UNWIND [1, 2, 3] AS x RETURN x }
      CALL { UNWIND ['a', 'b'] AS y RETURN y }
      RETURN x, y
      """
    Then the result should be, in any order:
      | x | y   |
      | 1 | 'a' |
      | 1 | 'b' |
      | 2 | 'a' |
      | 2 | 'b' |
      | 3 | 'a' |
      | 3 | 'b' |
    And no side effects

  Scenario: Simple nested subqueries
    When executing query:
      """
      CALL { CALL { CALL { RETURN 1 as x } RETURN x } RETURN x } RETURN x
      """
    Then the result should be, in any order:
      | x |
      | 1 |
    And no side effects

  Scenario: Nested subqueries
    And having executed:
    """
    CREATE (:A), (:B), (:C)
    """
    When executing query:
      """
      CALL {
        CALL {
          CALL {
            MATCH (a:A) RETURN a
          }
          MATCH (b:B) RETURN a, b
        }
        MATCH (c:C) RETURN a, b, c
      }
      RETURN a, b, c
      """
    Then the result should be, in any order:
      | a    | b    | c    |
      | (:A) | (:B) | (:C) |
    And no side effects

  Scenario: CALL around union query - using returned var in outer query
    When executing query:
      """
      CALL { RETURN 1 as x UNION RETURN 2 as x } RETURN x
      """
    Then the result should be, in any order:
      | x |
      | 1 |
      | 2 |
    And no side effects

  Scenario: CALL around union query with different return column orders - using returned vars in outer query
    When executing query:
      """
      CALL { RETURN 1 as x, 2 AS y UNION RETURN 3 AS y, 2 as x } RETURN x, y
      """
    Then the result should be, in any order:
      | x | y |
      | 1 | 2 |
      | 2 | 3 |
    And no side effects

  Scenario: Aggregating top and bottom results
    And having executed:
    """
    UNWIND range(1, 10) as p
    CREATE ({prop: p})
    """
    When executing query:
      """
      CALL {
        MATCH (x) WHERE x.prop > 0 RETURN x ORDER BY x.prop LIMIT 3
        UNION
        MATCH (x) WHERE x.prop > 0 RETURN x ORDER BY x.prop DESC LIMIT 3
      }
      RETURN sum(x.prop) AS sum
      """
    Then the result should be, in any order:
      | sum  |
      | 33   |
    And no side effects

  Scenario: Should treat variables with the same name but different scopes correctly
    And having executed:
    """
    CREATE (), ()
    """
    When executing query:
      """
      MATCH (x)
      CALL {
        MATCH (x) RETURN x AS y
      }
      RETURN count(*) AS count
      """
    Then the result should be, in any order:
      | count |
      | 4     |
    And no side effects

  Scenario: Should work with preceding MATCH and aggregation
    And having executed:
    """
    CREATE (:Person {age: 20, name: 'Alice'}),
           (:Person {age: 27, name: 'Bob'}),
           (:Person {age: 65, name: 'Charlie'}),
           (:Person {age: 30, name: 'Dora'})
    """
    When executing query:
      """
      MATCH (p:Person)
      CALL {
        UNWIND range(1, 5) AS i
        RETURN count(i) AS numberOfClones
      }
      RETURN p.name, numberOfClones
      """
    Then the result should be, in any order:
      | p.name    | numberOfClones |
      | 'Alice'   | 5              |
      | 'Bob'     | 5              |
      | 'Charlie' | 5              |
      | 'Dora'    | 5              |
    And no side effects
