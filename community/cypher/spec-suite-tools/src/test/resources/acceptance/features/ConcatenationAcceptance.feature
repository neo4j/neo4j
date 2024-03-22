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

Feature: ConcatenationAcceptance

  Background:
    Given an empty graph
    And having executed:
    """
    CREATE (:Person {firstName: "Ada", lastName: "A.", age: 100, cats: ["George", "Fred"], dogs: ["Shadow"]}),
       (:Person {firstName: "Bob", lastName: "B.", age: 50, cats: ["Ron", "Ginny"], dogs: ["Arne"]}),
       (:Person {firstName: "Charles", lastName: "C.", age: 13, cats: ["Snuffles", "Fluffy"], dogs: ["Percy", "Charlie"]})
    """

  Scenario: Simple LIST concatenation
    Given an empty graph
    When executing query:
      """
      RETURN [2] || [1] || ["a"] AS list
      """
    Then the result should be, in any order:
      | list   |
      | [2, 1, 'a'] |
    And no side effects

  Scenario: Simple STRING concatenation
    Given an empty graph
    When executing query:
      """
      RETURN "a" || "b" || "c" AS string
      """
    Then the result should be, in any order:
      | string |
      | 'abc'  |
    And no side effects

  Scenario: Simple LIST concatenation with type casting
    Given an empty graph
    When executing query:
      """
      RETURN [1] || (2 + []) AS list
      """
    Then the result should be, in any order:
      | list   |
      | [1, 2] |
    And no side effects

  Scenario: Simple STRING concatenation with type casting
    Given an empty graph
    When executing query:
      """
      RETURN toString(1) || "a" || toString(1.0) AS string
      """
    Then the result should be, in any order:
      | string |
      | '1a1.0'   |
    And no side effects

  Scenario: NULL concatenation
    Given an empty graph
    When executing query:
      """
      RETURN null || null AS nullConcatenation
      """
    Then the result should be, in any order:
      | nullConcatenation |
      | null              |
    And no side effects

  Scenario: Simple LIST concatenation with NULL
    Given an empty graph
    When executing query:
      """
      RETURN [1] || null AS list1, null || ["a"] AS list2
      """
    Then the result should be, in any order:
      | list1 | list2 |
      | null  | null  |
    And no side effects

  Scenario: Simple STRING concatenation with NULL
    Given an empty graph
    When executing query:
      """
      RETURN  "a" || null AS string1, null || "a" AS string2
      """
    Then the result should be, in any order:
      | string1 | string2 |
      | null    | null    |
    And no side effects

  Scenario: More complex concatenation mixing (1)
    Given an empty graph
    When executing query:
      """
      RETURN  1 + [] || [2] || [4] + "a" + "b" || [1] AS result
      """
    Then the result should be, in any order:
      | result                 |
      | [1, 2, 4, 'a', 'b', 1] |
    And no side effects

  Scenario: More complex concatenation mixing (2)
    Given an empty graph
    When executing query:
      """
      RETURN  "a" + "b" || "c" + "d" + 1 + "e" + 1 || "f" AS result
      """
    Then the result should be, in any order:
      | result     |
      | 'abcd1e1f' |
    And no side effects

  Scenario: More complex concatenation mixing (3)
    Given an empty graph
    When executing query:
      """
      RETURN  [12] || [11] || [10] || [9, 8] || ["7"] AS result
      """
    Then the result should be, in any order:
      | result                  |
      | [12, 11, 10, 9, 8, '7'] |
    And no side effects

  Scenario: String concatenation from Node properties
    Given an empty graph
    When executing query:
      """
      MATCH (n)
      RETURN  n.firstName || " " || n.lastName AS fullName
      """
    Then the result should be, in any order:
      | fullName     |
      | 'Ada A.'     |
      | 'Bob B.'     |
      | 'Charles C.' |
    And no side effects

  Scenario: List concatenation from Node properties
    Given an empty graph
    When executing query:
      """
      MATCH (n)
      RETURN  n.cats || n.dogs AS pets
      """
    Then the result should be, in any order:
      | pets                                       |
      | ['George', 'Fred', 'Shadow']               |
      | ['Ron', 'Ginny', 'Arne']                   |
      | ['Snuffles', 'Fluffy', 'Percy', 'Charlie'] |
    And no side effects

  Scenario: Concatenation from Node properties should fail if wrong types
    Given an empty graph
    When executing query:
      """
      MATCH (n)
      RETURN  n.firstName || n.age AS fail
      """
    Then a TypeError should be raised at runtime: *

  Scenario: Concatenation doesn't work with temporal types
    Given an empty graph
    When executing query:
      """
      RETURN time('13:42:19') || duration({days: 1, hours: 12}) AS theTime
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Concatenation doesn't work with number types
    Given an empty graph
    When executing query:
      """
      RETURN 1 || 3
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Concatenation doesn't implicitly coerce integers (1)
    Given an empty graph
    When executing query:
      """
      RETURN 1 || [3]
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Concatenation doesn't implicitly coerce integers (2)
    Given an empty graph
    When executing query:
      """
      RETURN [3] || 2
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Concatenation doesn't implicitly coerce strings (1)
    Given an empty graph
    When executing query:
      """
      RETURN "a" || 3
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Concatenation doesn't implicitly coerce strings (2)
    Given an empty graph
    When executing query:
      """
      RETURN 1 || "a"
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Concatenation doesn't work for strings and lists at the same time (1)
    Given an empty graph
    When executing query:
      """
      RETURN [1] || "a"
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Concatenation doesn't work for strings and lists at the same time (2)
    Given an empty graph
    When executing query:
      """
      RETURN "a" || [2]
      """
    Then a SyntaxError should be raised at compile time: *