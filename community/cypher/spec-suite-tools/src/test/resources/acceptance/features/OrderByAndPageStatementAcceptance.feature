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

Feature: OrderByAndPageStatement

  Background:
    Given having executed:
      """
      CREATE (:Person {name: "Albert"}), (:Person {name: "Bert"}), (:Person {name: "Carl"}), (:Person {name: "David"}), (:Person {name: "Eric"})
      """

  Scenario: Order By Skip Limit
    When executing query:
      """
      MATCH (n)
      ORDER BY n.name DESC SKIP 1 LIMIT 2
      RETURN n.name AS name
      """
    Then the result should be, in order:
      | name    |
      | 'David' |
      | 'Carl'  |
    And no side effects

  Scenario: Order By Skip
    When executing query:
      """
      MATCH (n)
      ORDER BY n.name DESC SKIP 1
      RETURN n.name AS name
      """
    Then the result should be, in order:
      | name    |
      | 'David' |
      | 'Carl'  |
      | 'Bert'  |
      | 'Albert'|
    And no side effects

  Scenario: Order By Limit
    When executing query:
      """
      MATCH (n)
      ORDER BY n.name DESC LIMIT 1
      RETURN n.name AS name
      """
    Then the result should be, in order:
      | name    |
      | 'Eric'  |
    And no side effects

  Scenario: Order By
    When executing query:
      """
      MATCH (n)
      ORDER BY n.name DESC
      RETURN n.name AS name
      """
    Then the result should be, in order:
      | name    |
      | 'Eric'  |
      | 'David' |
      | 'Carl'  |
      | 'Bert'  |
      | 'Albert'|
    And no side effects

  Scenario: Skip Limit
    When executing query:
      """
      MATCH (n)
      SKIP 1 LIMIT 2
      RETURN count(n) AS c
      """
    Then the result should be, in order:
      | c |
      | 2 |
    And no side effects

  Scenario: Skip
    When executing query:
      """
      MATCH (n)
      SKIP 1
      RETURN count(n) AS c
      """
    Then the result should be, in order:
      | c |
      | 4  |
    And no side effects

  Scenario: Limit
    When executing query:
      """
      MATCH (n)
      LIMIT 1
      RETURN count(n) AS c
      """
    Then the result should be, in order:
      | c |
      | 1 |
    And no side effects

  Scenario: Order By Offset Limit
    When executing query:
      """
      MATCH (n)
      ORDER BY n.name DESC OFFSET 1 LIMIT 2
      RETURN n.name AS name
      """
    Then the result should be, in order:
      | name    |
      | 'David' |
      | 'Carl'  |
    And no side effects

  Scenario: Order By Offset
    When executing query:
      """
      MATCH (n)
      ORDER BY n.name DESC OFFSET 1
      RETURN n.name AS name
      """
    Then the result should be, in order:
      | name    |
      | 'David' |
      | 'Carl'  |
      | 'Bert'  |
      | 'Albert'|
    And no side effects

  Scenario: Offset Limit
    When executing query:
      """
      MATCH (n)
      OFFSET 1 LIMIT 2
      RETURN count(n) AS c
      """
    Then the result should be, in order:
      | c |
      | 2 |
    And no side effects

  Scenario: Offset
    When executing query:
      """
      MATCH (n)
      OFFSET 1
      RETURN count(n) AS c
      """
    Then the result should be, in order:
      | c |
      | 4  |
    And no side effects


  Scenario: Order By and Page Statement with Unwind and subquery call
    Given an empty graph
    When executing query:
      """
      UNWIND [0, 1, 2] AS x
      UNWIND ["a", "b", "c"] AS y
      ORDER BY x DESC
      CALL (x) {
        RETURN x * 10 AS z
      }
      ORDER BY y ASC OFFSET 2 LIMIT 5
      RETURN x, y, z
      """
    Then the result should be, in order:
      | x |  y  | z |
      | 1 | 'a' | 10 |
      | 0 | 'b' | 0 |
      | 1 | 'b' | 10 |
      | 2 | 'b' | 20 |
      | 1 | 'c' | 10 |
    And no side effects

  Scenario: Order By and Page Statement preserved order after call
    Given an empty graph
    When executing query:
      """
      UNWIND [0, 1, 2] AS x
      UNWIND ["a", "b", "c"] AS y
      ORDER BY x DESC
      CALL (x) {
        RETURN x * 10 AS z
      }
      RETURN x, y, z
      """
    Then the result should be, in order:
      | x |  y  | z |
      | 2 | 'a' | 20 |
      | 2 | 'b' | 20 |
      | 2 | 'c' | 20 |
      | 1 | 'a' | 10 |
      | 1 | 'b' | 10 |
      | 1 | 'c' | 10 |
      | 0 | 'a' | 0 |
      | 0 | 'b' | 0 |
      | 0 | 'c' | 0 |
    And no side effects

  Scenario: Ordering before collect
    When executing query:
      """
      MATCH (n)
      ORDER BY n.name DESC LIMIT 3
      RETURN collect(n.name)
      """
    Then the result should be, in order:
      | collect(n.name) |
      | ['Eric', 'David', 'Carl'] |
    And no side effects

  Scenario: Ordering before collect
    Given having executed:
      """
      MATCH (n) DETACH DELETE n
      """
    Given having executed:
      """
      CREATE
        (a {name: 'A'}),
        (b {name: 'B'}),
        (c {name: 'C'}),
        (d {name: 'D'}),
        (e {name: 'E'}),
        (a)-[:KNOWS]->(b),
        (a)-[:KNOWS]->(c),
        (a)-[:KNOWS]->(d),
        (a)-[:KNOWS]->(e)
      """
    When executing query:
      """
      MATCH (n)
      ORDER BY n.name DESC SKIP 1 LIMIT 3
      RETURN collect(n.name) as names
      """
    Then the result should be, in order:
      | names |
      | ['D', 'C', 'B'] |
    And no side effects

