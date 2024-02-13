#
# Copyright (c) "Neo4j"
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

Feature: AggregationAcceptance

  Background:
    Given an empty graph

  Scenario: Using a optional match after aggregation and before an aggregation
    And having executed:
      """
      CREATE (:Z{key:1})-[:IS_A]->(:A)
      """
    When executing query:
      """
      MATCH (a:A)
      WITH count(*) AS aCount
      OPTIONAL MATCH (z:Z)-[IS_A]->()
      RETURN aCount, count(distinct z.key) as zCount
      """
    Then the result should be, in any order:
      | aCount | zCount |
      | 1      | 1      |
    And no side effects

  Scenario: Multiple aggregations should work
    And having executed:
      """
      CREATE (zadie: AUTHOR {name: "Zadie Smith"})
      CREATE (zadie)-[:WROTE]->(:BOOK {book: "White teeth"})
      CREATE (zadie)-[:WROTE]->(:BOOK {book: "The Autograph Man"})
      CREATE (zadie)-[:WROTE]->(:BOOK {book: "On Beauty"})
      CREATE (zadie)-[:WROTE]->(:BOOK {book: "NW"})
      CREATE (zadie)-[:WROTE]->(:BOOK {book: "Swing Time"})
      """
    When executing query:
     """
     MATCH (a)-[r]->(b)
     RETURN b.book as book, count(r), count(distinct a)
     ORDER BY book
     """
    Then the result should be, in order:
      | book                | count(r) | count(distinct a) |
      | 'NW'                | 1        | 1                 |
      | 'On Beauty'         | 1        | 1                 |
      | 'Swing Time'        | 1        | 1                 |
      | 'The Autograph Man' | 1        | 1                 |
      | 'White teeth'       | 1        | 1                 |
    And no side effects

  Scenario: Distinct should work with multiple equal grouping keys and only one different
    And having executed:
      """
      UNWIND range(1,9) as i
      CREATE ({prop1:'prop1',prop2:'prop2',prop3:'prop3',prop4:'prop4',prop5:'prop5',prop6:toString(i),prop7:'prop7',prop8:'prop8',prop9:'prop9'})
      """
    When executing query:
      """
      MATCH (node)
      RETURN DISTINCT
        node.prop1 as p1,
        node.prop2 as p2,
        node.prop3 as p3,
        node.prop4 as p4,
        node.prop5 as p5,
        node.prop6 as p6,
        node.prop7 as p7,
        node.prop8 as p8,
        node.prop9 as p9
      """
    Then the result should be, in any order:
      | p1      | p2      | p3      | p4      | p5      | p6  | p7      | p8      | p9      |
      | 'prop1' | 'prop2' | 'prop3' | 'prop4' | 'prop5' | '1' | 'prop7' | 'prop8' | 'prop9' |
      | 'prop1' | 'prop2' | 'prop3' | 'prop4' | 'prop5' | '2' | 'prop7' | 'prop8' | 'prop9' |
      | 'prop1' | 'prop2' | 'prop3' | 'prop4' | 'prop5' | '3' | 'prop7' | 'prop8' | 'prop9' |
      | 'prop1' | 'prop2' | 'prop3' | 'prop4' | 'prop5' | '4' | 'prop7' | 'prop8' | 'prop9' |
      | 'prop1' | 'prop2' | 'prop3' | 'prop4' | 'prop5' | '5' | 'prop7' | 'prop8' | 'prop9' |
      | 'prop1' | 'prop2' | 'prop3' | 'prop4' | 'prop5' | '6' | 'prop7' | 'prop8' | 'prop9' |
      | 'prop1' | 'prop2' | 'prop3' | 'prop4' | 'prop5' | '7' | 'prop7' | 'prop8' | 'prop9' |
      | 'prop1' | 'prop2' | 'prop3' | 'prop4' | 'prop5' | '8' | 'prop7' | 'prop8' | 'prop9' |
      | 'prop1' | 'prop2' | 'prop3' | 'prop4' | 'prop5' | '9' | 'prop7' | 'prop8' | 'prop9' |
    And no side effects

  Scenario: percentileDisc on empty data should return null
    When executing query:
      """
       MATCH (n:FAKE) RETURN percentileDisc(n.x, 0.9) AS result
      """
    Then the result should be, in any order:
      | result |
      | null   |
    And no side effects

  Scenario: optional match followed by aggregation
    Given an empty graph
    When executing query:
    """
   OPTIONAL MATCH (n)
   UNWIND [n] AS m
   RETURN collect(m) AS c
    """
    Then the result should be, in any order:
      | c  |
      | [] |
    And no side effects

  Scenario: aggregation on function with null argument
    Given an empty graph
    When executing query:
    """
    RETURN collect(sin(null)) AS c
    """
    Then the result should be, in any order:
      | c  |
      | [] |
    And no side effects

  Scenario: Count nodes and average properties at the same time
    Given an empty graph
    And having executed:
      """
      UNWIND range(1, 10) AS i
      CREATE (:N)
      CREATE (:N {prop: i})
      """
    And having executed:
      """
      CREATE INDEX FOR (n:N) ON (n.prop)
      """
    And having executed:
      """
      CALL db.awaitIndexes()
      """
    When executing query:
    """
    MATCH (n:N) RETURN count(n) AS count, avg(n.prop) AS avg
    """
    Then the result should be, in any order:
      | count | avg |
      | 20    | 5.5 |
    And no side effects
