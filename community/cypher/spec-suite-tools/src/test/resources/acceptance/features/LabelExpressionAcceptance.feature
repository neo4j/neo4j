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

Feature: LabelExpressionAcceptance

  Scenario: Simple label conjunction
    Given an empty graph
    And having executed:
      """
      CREATE (:A   {id: 'a' })
      CREATE (:A:B {id: 'ab'})
      CREATE (:A:C {id: 'ac'})
      """

    When executing query:
      """
      MATCH (n:A&B)
      RETURN n.id AS id
      """
    Then the result should be, in any order:
      | id   |
      | 'ab' |
    And no side effects

  Scenario: Repeating label in conjunction
    Given an empty graph
    And having executed:
      """
      CREATE (:A   {id: 'a' })
      CREATE (:A:A {id: 'aa'})
      CREATE (:A:B {id: 'ab'})
      """

    When executing query:
      """
      MATCH (n:A&A)
      RETURN n.id AS id
      """
    Then the result should be, in any order:
      | id   |
      | 'a'  |
      | 'aa' |
      | 'ab' |
    And no side effects

  Scenario: Simple label disjunction
    Given an empty graph
    And having executed:
      """
      CREATE (:A   {id: 'a' })
      CREATE (:A:B {id: 'ab'})
      CREATE (:C   {id: 'c'})
      """

    When executing query:
      """
      MATCH (n:B|C)
      RETURN n.id AS id
      """
    Then the result should be, in any order:
      | id   |
      | 'ab' |
      | 'c'  |
    And no side effects

  Scenario: Repeating label in disjunction
    Given an empty graph
    And having executed:
      """
      CREATE (:A   {id: 'a' })
      CREATE (:A:B {id: 'ab'})
      CREATE (:C   {id: 'c'})
      """

    When executing query:
      """
      MATCH (n:A|A)
      RETURN n.id AS id
      """
    Then the result should be, in any order:
      | id   |
      | 'a'  |
      | 'ab' |
    And no side effects

  Scenario: Simple label negation
    Given an empty graph
    And having executed:
      """
      CREATE (:A   {id: 'a' })
      CREATE (:B   {id: 'b' })
      CREATE (:A:B {id: 'ab'})
      """

    When executing query:
      """
      MATCH (n:!B)
      RETURN n.id AS id
      """
    Then the result should be, in any order:
      | id  |
      | 'a' |
    And no side effects

  Scenario: Double negated expression
    Given an empty graph
    And having executed:
      """
      CREATE (:A   {id: 'a' })
      CREATE (:B   {id: 'b' })
      CREATE (:A:B {id: 'ab'})
      """

    When executing query:
      """
      MATCH (n:!!B)
      RETURN n.id AS id
      """
    Then the result should be, in any order:
      | id   |
      | 'b'  |
      | 'ab' |
    And no side effects

  Scenario: Simple label wildcard
    Given an empty graph
    And having executed:
      """
      CREATE (   {id:  ''})
      CREATE (:A {id: 'a'})
      """

    When executing query:
      """
      MATCH (n:%)
      RETURN n.id AS id
      """
    Then the result should be, in any order:
      | id  |
      | 'a' |
    And no side effects

  Scenario: Negated label wildcard
    Given an empty graph
    And having executed:
      """
      CREATE (   {id: '' })
      CREATE (:A {id: 'a'})
      """

    When executing query:
      """
      MATCH (n:!%)
      RETURN n.id AS id
      """
    Then the result should be, in any order:
      | id  |
      | ''  |
    And no side effects

  Scenario: Conjunction has precedence over disjunction
    Given an empty graph
    And having executed:
      """
      CREATE (:A   {id: 'a' })
      CREATE (:B:C {id: 'bc'})
      CREATE (:A:C {id: 'ac'})
      """

    When executing query:
      """
      MATCH (n:A|B&C)
      RETURN n.id AS id
      """
    Then the result should be, in any order:
      | id   |
      | 'a'  |
      | 'ac' |
      | 'bc' |
    And no side effects

  Scenario: Parenthesized expression has precedence over conjunction
    Given an empty graph
    And having executed:
      """
      CREATE (:A   {id: 'a' })
      CREATE (:B   {id: 'b' })
      CREATE (:B:C {id: 'bc'})
      """

    When executing query:
      """
      MATCH (n:(A|B)&C)
      RETURN n.id AS id
      """
    Then the result should be, in any order:
      | id   |
      | 'bc' |
    And no side effects

  Scenario: Parenthesized expression has precedence over negation
    Given an empty graph
    And having executed:
      """
      CREATE (:A   {id: 'a' })
      CREATE (:B   {id: 'b' })
      CREATE (:C   {id: 'c' })
      CREATE (:B:C {id: 'bc'})
      """

    When executing query:
      """
      MATCH (n:!(A|B)&C)
      RETURN n.id AS id
      """
    Then the result should be, in any order:
      | id  |
      | 'c' |
    And no side effects

  Scenario: Label expression on node with label predicate in WHERE clause
    Given an empty graph
    And having executed:
      """
      CREATE (:A:B {id: 'ab'})
      CREATE (:A:C {id: 'ac'})
      CREATE (:A:D {id: 'ad'})
      """

    When executing query:
      """
      MATCH (n:A&(B|C))
      WHERE NOT n:C
      RETURN n.id AS id
      """
    Then the result should be, in any order:
      | id   |
      | 'ab' |
    And no side effects

  Scenario: Label expression on node inside relationship pattern
    Given an empty graph
    And having executed:
      """
      CREATE (:A:B {id: 'ab'})-[:REL]->()
      CREATE (:A:C {id: 'ac'})-[:REL]->()
      CREATE (:A:D {id: 'ad'})-[:REL]->()
      """

    When executing query:
      """
      MATCH (n:A&B)-->()
      RETURN n.id AS id
      """
    Then the result should be, in any order:
      | id   |
      | 'ab' |
    And no side effects

  Scenario: Label wildcard in conjunction
    Given an empty graph
    And having executed:
      """
      CREATE (:A   {id: 'a' })
      CREATE (:B   {id: 'b' })
      CREATE (:C   {id: 'c' })
      CREATE (:A:B {id: 'ab'})
      """

    When executing query:
      """
      MATCH (n:% & !A)
      RETURN n.id AS id
      """
    Then the result should be, in any order:
      | id   |
      | 'b'  |
      | 'c'  |
    And no side effects
