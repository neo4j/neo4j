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

Feature: ReturnAcceptance

  Scenario: LIMIT 0 should not stop side effects
    Given an empty graph
    When executing query:
      """
      CREATE (n)
      RETURN n
      LIMIT 0
      """
    Then the result should be, in any order:
      | n |
    And the side effects should be:
      | +nodes      | 1 |

  Scenario: LIMIT in projection should not stop side effects
    Given an empty graph
    When executing query:
      """
      UNWIND range(1, 10) AS i
      CREATE (n)
      RETURN i
      LIMIT 3
      """
    Then the result should be, in any order:
      | i |
      | 1 |
      | 2 |
      | 3 |
    And the side effects should be:
      | +nodes      | 10 |

  Scenario: Accessing a list with null should return null
    Given any graph
    When executing query:
      """
      RETURN [1, 2, 3][null] AS result
      """
    Then the result should be, in any order:
      | result |
      | null   |
    And no side effects

  Scenario: Accessing a list with null as lower bound should return null
    Given any graph
    When executing query:
      """
      RETURN [1, 2, 3][null..5] AS result
      """
    Then the result should be, in any order:
      | result |
      | null   |
    And no side effects

  Scenario: Accessing a list with null as upper bound should return null
    Given any graph
    When executing query:
      """
      RETURN [1, 2, 3][1..null] AS result
      """
    Then the result should be, in any order:
      | result |
      | null   |
    And no side effects

  Scenario: Accessing a map with null should return null
    Given any graph
    When executing query:
      """
      RETURN {key: 1337}[null] AS result
      """
    Then the result should be, in any order:
      | result |
      | null   |
    And no side effects

  Scenario: Return a nested list with null
    Given any graph
    When executing query:
      """
      RETURN [[1], [null], null] AS result
      """
    Then the result should be, in any order:
      | result              |
      | [[1], [null], null] |
    And no side effects

  Scenario: Return a map with null
    Given any graph
    When executing query:
      """
      RETURN {foo: null} AS result
      """
    Then the result should be, in any order:
      | result      |
      | {foo: null} |
    And no side effects

  Scenario: Return null in list with nested lists and maps
    Given any graph
    When executing query:
      """
      RETURN [null, [null, {a: null}], {b: [null, {c: [null]}]}] AS result
      """
    Then the result should be, in any order:
      | result                                              |
      | [null, [null, {a: null}], {b: [null, {c: [null]}]}] |
    And no side effects

  Scenario: Return null in map with nested lists and maps
    Given any graph
    When executing query:
      """
      RETURN {a: null, b: {c: null, d: {e: null}, f: [null, {g: null, h: [null], i: {j: null}}]}} as result
      """
    Then the result should be, in any order:
      | result                                                                               |
      | {a: null, b: {c: null, d: {e: null}, f: [null, {g: null, h: [null], i: {j: null}}]}} |

  Scenario: Accessing a non-existing property with string should work
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      WITH 'prop' AS prop
      MATCH (n) RETURN n[prop] AS result
      """
    Then the result should be, in any order:
      | result |
      | null   |
    And no side effects

  Scenario: Accessing a non-existing property with literal should work
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      MATCH (n) RETURN n['prop'] AS result
      """
    Then the result should be, in any order:
      | result |
      | null   |
    And no side effects

  Scenario: RETURN true AND list
    Given an empty graph
    And parameters are:
      | list | [] |
    When executing query:
      """
      RETURN true AND $list AS result
      """
    Then the result should be, in any order:
      | result |
      | false  |
    And no side effects

  Scenario: RETURN false OR list
    Given an empty graph
    And parameters are:
      | list | [] |
    When executing query:
      """
      RETURN true AND $list AS result
      """
    Then the result should be, in any order:
      | result |
      | false  |
    And no side effects

  Scenario: Exponentiation should work
    Given an empty graph
    When executing query:
      """
       WITH 2 AS number, 3 AS exponent RETURN number ^ exponent AS result
      """
    Then the result should be, in any order:
      | result |
      | 8.0    |
    And no side effects

  Scenario: Multiplying a float and an integer should be no problem
    Given an empty graph
    When executing query:
      """
      WITH 1.0 AS a, 1000 AS b RETURN a * (b / 10) AS result
      """
    Then the result should be, in any order:
      | result |
      | 100.0  |
    And no side effects

  Scenario: Positive range with negative step should be empty
    Given any graph
    When executing query:
      """
      RETURN range(2, 8, -1) AS result
      """
    Then the result should be, in any order:
      | result |
      | []     |
    And no side effects

  Scenario: Negative range with positive step should be empty
    Given any graph
    When executing query:
      """
      RETURN range(8, 2, 1) AS result
      """
    Then the result should be, in any order:
      | result |
      | []     |
    And no side effects

  Scenario: Return size of a huge list
      Given any graph
      When executing query:
        """
        RETURN size(range(1, 100000000000000)) AS result
        """
      Then the result should be, in any order:
        | result          |
        | 100000000000000 |
      And no side effects

  Scenario: Index into a huge list
      Given any graph
      When executing query:
        """
        RETURN range(1, 100000000000000)[99999999999999] AS result
        """
      Then the result should be, in any order:
        | result          |
        | 100000000000000 |
      And no side effects
  Scenario: Unaliased return items in a top level union should be accepted
    Given any graph
    When executing query:
      """
      RETURN 5 UNION ALL RETURN 5
      """
    Then the result should be, in any order:
      | 5 |
      | 5 |
      | 5 |
    And no side effects


  Scenario: Graph projections with aggregation
    Given an empty graph
    And having executed:
      """
      CREATE (a:Actor {name: "Actor 1"})
      CREATE (a)-[:REL]->(:Movie {title: "Movie 1"}),
             (a)-[:REL]->(:Movie {title: "Movie 2"})
      """
    When executing query:
      """
      MATCH (actor:Actor)-->(movie:Movie)
      WITH actor.name AS name, collect(movie{.title}) AS movies
      RETURN { name: name, movies: movies } AS actor
      """
    Then the result should be (ignoring element order for lists):
      | actor                                                               |
      | {name: 'Actor 1', movies: [{title: 'Movie 1'}, {title: 'Movie 2'}]} |

  Scenario: UNION with different RETURN order
    Given an empty graph
    When executing query:
      """
      WITH 1 AS y, 2 AS x
      RETURN y, x
      UNION
      WITH 2 AS x, 1 AS y
      RETURN *
      """
    Then the result should be, in any order:
      | y | x |
      | 1 | 2 |

