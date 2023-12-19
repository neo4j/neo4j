#
# Copyright (c) 2002-2020 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j Enterprise Edition. The included source
# code can be redistributed and/or modified under the terms of the
# Neo4j Sweden Software License, as found in the associated LICENSE.txt
# file.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# Neo4j Sweden Software License for more details.
#

#encoding: utf-8

Feature: ReturnAcceptance

  Scenario: Filter should work
    Given an empty graph
    And having executed:
      """
      CREATE (a {foo: 1})-[:T]->({foo: 1}),
        (a)-[:T]->({foo: 2}),
        (a)-[:T]->({foo: 3})
      """
    When executing query:
      """
      MATCH (a {foo: 1})
      MATCH p=(a)-->()
      RETURN filter(x IN nodes(p) WHERE x.foo > 2) AS n
      """
    Then the result should be:
      | n            |
      | [({foo: 3})] |
      | []           |
      | []           |
    And no side effects

  Scenario: LIMIT 0 should stop side effects
    Given an empty graph
    When executing query:
      """
      CREATE (n)
      RETURN n
      LIMIT 0
      """
    Then the result should be:
      | n            |
    And no side effects

  Scenario: Accessing a list with null should return null
    Given any graph
    When executing query:
      """
      RETURN [1, 2, 3][null] AS result
      """
    Then the result should be:
      | result |
      | null   |
    And no side effects

  Scenario: Accessing a list with null as lower bound should return null
    Given any graph
    When executing query:
      """
      RETURN [1, 2, 3][null..5] AS result
      """
    Then the result should be:
      | result |
      | null   |
    And no side effects

  Scenario: Accessing a list with null as upper bound should return null
    Given any graph
    When executing query:
      """
      RETURN [1, 2, 3][1..null] AS result
      """
    Then the result should be:
      | result |
      | null   |
    And no side effects

  Scenario: Accessing a map with null should return null
    Given any graph
    When executing query:
      """
      RETURN {key: 1337}[null] AS result
      """
    Then the result should be:
      | result |
      | null   |
    And no side effects

  Scenario: Return a nested list with null
    Given any graph
    When executing query:
      """
      RETURN [[1], [null], null] AS result
      """
    Then the result should be:
      | result      |
      | [[1], [null], null] |
    And no side effects

  Scenario: Return a map with null
    Given any graph
    When executing query:
      """
      RETURN {foo: null} AS result
      """
    Then the result should be:
      | result      |
      | {foo: null} |
    And no side effects

  Scenario: Return null in list with nested lists and maps
    Given any graph
    When executing query:
      """
      RETURN [null, [null, {a: null}], {b: [null, {c: [null]}]}] AS result
      """
    Then the result should be:
      | result      |
      | [null, [null, {a: null}], {b: [null, {c: [null]}]}] |
    And no side effects

  Scenario: Return null in map with nested lists and maps
    Given any graph
    When executing query:
      """
      RETURN {a: null, b: {c: null, d: {e: null}, f: [null, {g: null, h: [null], i: {j: null}}]}} as result
      """
    Then the result should be:
      | result      |
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
    Then the result should be:
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
    Then the result should be:
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
    Then the result should be:
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
    Then the result should be:
      | result |
      | false   |
    And no side effects
