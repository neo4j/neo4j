#
# Copyright (c) 2002-2018 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Neo4j is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

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
    Then the result should be:
      | aCount | zCount |
      | 1      | 1      |
    And no side effects

  Scenario: max() over strings
    When executing query:
      """
      UNWIND ['a', 'b', 'B', null, 'abc', 'abc1'] AS i
      RETURN max(i)
      """
    Then the result should be:
      | max(i) |
      | 'b'    |
    And no side effects

  Scenario: min() over strings
    When executing query:
      """
      UNWIND ['a', 'b', 'B', null, 'abc', 'abc1'] AS i
      RETURN min(i)
      """
    Then the result should be:
      | min(i) |
      | 'B'    |
    And no side effects

  Scenario: max() over integers
    When executing query:
      """
      UNWIND [1, 2, 0, null, -1] AS x
      RETURN max(x)
      """
    Then the result should be:
      | max(x) |
      | 2      |
    And no side effects

  Scenario: min() over integers
    When executing query:
      """
      UNWIND [1, 2, 0, null, -1] AS x
      RETURN min(x)
      """
    Then the result should be:
      | min(x) |
      | -1     |
    And no side effects

  Scenario: max() over floats
    When executing query:
      """
      UNWIND [1.0, 2.0, 0.5, null] AS x
      RETURN max(x)
      """
    Then the result should be:
      | max(x) |
      | 2.0    |
    And no side effects

  Scenario: min() over floats
    When executing query:
      """
      UNWIND [1.0, 2.0, 0.5, null] AS x
      RETURN min(x)
      """
    Then the result should be:
      | min(x) |
      | 0.5    |
    And no side effects

  Scenario: max() over mixed numeric values
    When executing query:
      """
      UNWIND [1, 2.0, 5, null, 3.2, 0.1] AS x
      RETURN max(x)
      """
    Then the result should be:
      | max(x) |
      | 5      |
    And no side effects

  Scenario: min() over mixed numeric values
    When executing query:
      """
      UNWIND [1, 2.0, 5, null, 3.2, 0.1] AS x
      RETURN min(x)
      """
    Then the result should be:
      | min(x) |
      | 0.1    |
    And no side effects

  Scenario: max() over mixed values
    When executing query:
      """
      UNWIND [1, 'a', null, [1, 2], 0.2, 'b'] AS x
      RETURN max(x)
      """
    Then the result should be:
      | max(x) |
      | 1      |
    And no side effects

  Scenario: min() over mixed values
    When executing query:
      """
      UNWIND [1, 'a', null, [1, 2], 0.2, 'b'] AS x
      RETURN min(x)
      """
    Then the result should be:
      | min(x) |
      | [1, 2] |
    And no side effects

  Scenario: max() over list values
    When executing query:
      """
      UNWIND [[1], [2], [2, 1]] AS x
      RETURN max(x)
      """
    Then the result should be:
      | max(x) |
      | [2, 1] |
    And no side effects

  Scenario: min() over list values
    When executing query:
      """
      UNWIND [[1], [2], [2, 1]] AS x
      RETURN min(x)
      """
    Then the result should be:
      | min(x) |
      | [1]    |
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
    Then the result should be:
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
    Then the result should be:
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
