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

  Scenario Outline: Semantics of label expression on node in MATCH
    Given an empty graph
    And having executed:
      """
      CREATE (),
             (:A),
             (:B),
             (:C),
             (:A:B),
             (:A:C),
             (:B:C),
             (:A:B:C)
      """

    When executing query:
      """
      MATCH <labelExpressionNode>
      WITH n ORDER BY size(labels(n)), labels(n)
      RETURN collect(n) AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    And no side effects
    Examples:
      | labelExpressionNode | result                                                   |
      | (n)                 | [(), (:A), (:B), (:C), (:A:B), (:A:C), (:B:C), (:A:B:C)] |
      | (n:A)               | [(:A), (:A:B), (:A:C), (:A:B:C)]                         |
      | (n:A&B)             | [(:A:B), (:A:B:C)]                                       |
      | (n:A\|B)            | [(:A), (:B), (:A:B), (:A:C), (:B:C), (:A:B:C)]           |
      | (n:!A)              | [(), (:B), (:C), (:B:C)]                                 |
      | (n:!!A)             | [(:A), (:A:B), (:A:C), (:A:B:C)]                         |
      | (n:A&!A)            | []                                                       |
      | (n:A\|!A)           | [(), (:A), (:B), (:C), (:A:B), (:A:C), (:B:C), (:A:B:C)] |
      | (n:%)               | [(:A), (:B), (:C), (:A:B), (:A:C), (:B:C), (:A:B:C)]     |
      | (n:!%)              | [()]                                                     |
      | (n:%\|!%)           | [(), (:A), (:B), (:C), (:A:B), (:A:C), (:B:C), (:A:B:C)] |
      | (n:%&!%)            | []                                                       |
      | (n:A&%)             | [(:A), (:A:B), (:A:C), (:A:B:C)]                         |
      | (n:A\|%)            | [(:A), (:B), (:C), (:A:B), (:A:C), (:B:C), (:A:B:C)]     |
      | (n:(A&B)&!(B&C))    | [(:A:B)]                                                 |
      | (n:!(A&%)&%)        | [(:B), (:C), (:B:C)]                                     |

  Scenario Outline: Semantics of relationship type expression on relationship in MATCH
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:A]->(),
             ()-[:B]->(),
             ()-[:C]->()
      """

    When executing query:
      """
      MATCH ()-<relationshipTypeExpressionRelationship>->()
      WITH type(r) AS rType ORDER BY rType
      RETURN collect(rType) AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    And no side effects
    Examples:
      | relationshipTypeExpressionRelationship | result           |
      | [r]                                     | ['A', 'B', 'C'] |
      | [r:A]                                   | ['A']           |
      | [r:A\|B]                                | ['A', 'B']      |
      | [r:!A]                                  | ['B', 'C']      |
      | [r:!!A]                                 | ['A']           |
      | [r:A&!A]                                | []              |
      | [r:A\|!A]                               | ['A', 'B', 'C'] |
      | [r:%]                                   | ['A', 'B', 'C'] |
      | [r:!%]                                  | []              |
      | [r:%\|!%]                               | ['A', 'B', 'C'] |
      | [r:%&!%]                                | []              |
      | [r:A&%]                                 | ['A']           |
      | [r:A\|%]                                | ['A', 'B', 'C'] |
      | [r:!(A&%)&%)]                           | ['B', 'C']      |

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

  Scenario: Label expression on node and label predicate in WHERE clause
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

  Scenario: Label expression in WHERE clause
    Given an empty graph
    And having executed:
      """
      CREATE (:A   {id: 'a' })
      CREATE (:C   {id: 'c' })
      CREATE (:A:B {id: 'ab'})
      """

    When executing query:
      """
      MATCH (n) WHERE n:A&B
      RETURN n.id AS id
      """
    Then the result should be, in any order:
      | id   |
      | 'ab' |
    And no side effects

  Scenario: Relationship type expression in WHERE clause
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:A {id: 'a'}]->()
      CREATE ()-[:B {id: 'b'}]->()
      CREATE ()-[:C {id: 'c'}]->()
      """

    When executing query:
      """
      MATCH (r) WHERE r:A|B
      RETURN r.id AS id
      """
    Then the result should be, in any order:
      | id   |
      | 'a'  |
      | 'b'  |
    And no side effects

  Scenario: Label expression in CASE expression
    Given an empty graph
    And having executed:
      """
      CREATE (:A {id: 'a'})
      CREATE (:B {id: 'b'})
      CREATE (:C {id: 'c'})
      """

    When executing query:
      """
      MATCH (n)
      RETURN CASE
               WHEN n:A&B THEN 1
               WHEN n:B&C THEN 2
               ELSE 0
             END AS result
      """
    Then the result should be, in any order:
      | result |
      | 0      |
      | 0      |
      | 0      |
      | 1      |
      | 2      |
    And no side effects

  Scenario: Relationship type expression in CASE expression
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:A {id: 'a'}]->()
      CREATE ()-[:B {id: 'b'}]->()
      CREATE ()-[:C {id: 'c'}]->()
      CREATE ()-[:D {id: 'd'}]->()
      """

    When executing query:
      """
      MATCH ()-[r]->()
      RETURN CASE
               WHEN r:A|B THEN 1
               WHEN r:C   THEN 2
               ELSE 0
             END AS result
      """
    Then the result should be, in any order:
      | result |
      | 1      |
      | 1      |
      | 2      |
      | 0      |
    And no side effects

  Scenario: Label expression in RETURN clause
    Given an empty graph
    And having executed:
      """
      CREATE (:A   {id: 'a' })
      CREATE (:B   {id: 'b' })
      CREATE (:A:B {id: 'ab'})
      """

    When executing query:
      """
      MATCH (n)
      RETURN n:A&B AS result
      """
    Then the result should be, in any order:
      | result |
      | false  |
      | false  |
      | true   |
    And no side effects

  Scenario: Relationship type expression in RETURN clause
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:A {id: 'a'}]->()
      CREATE ()-[:B {id: 'b'}]->()
      CREATE ()-[:C {id: 'c'}]->()
      """

    When executing query:
      """
      MATCH ()-[r]->()
      RETURN r:A|B AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
      | true   |
      | false  |
    And no side effects
