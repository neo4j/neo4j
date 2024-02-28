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

Feature: CaseExpression

  Scenario: Case should handle mixed number types
    Given any graph
    When executing query:
      """
      WITH 0.5 AS x
      WITH (CASE WHEN x < 1 THEN 1 ELSE 2.0 END) AS x
      RETURN x + 1
      """
    Then the result should be, in any order:
      | x + 1 |
      | 2     |
    And no side effects

  Scenario: Case should handle mixed types
    Given any graph
    When executing query:
      """
      WITH 0.5 AS x
      WITH (CASE WHEN x < 1 THEN 'wow' ELSE true END) AS x
      RETURN x + '!'
      """
    Then the result should be, in any order:
      | x + '!' |
      | 'wow!'  |
    And no side effects

  Scenario: Returning a CASE expression into pattern expression
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)
      CREATE (a)-[:T]->(:B),
             (a)-[:T]->(:C)
      """
    When executing query:
      """
      MATCH (n)
      WITH n, CASE
        WHEN id(n) >= 0 THEN [p=(n)-->() | p]
        ELSE 42
        END AS p
      WITH n, p UNWIND CASE
          WHEN p = [] THEN [null]
          ELSE p
        END AS path
      RETURN n, path
      """
    Then the result should be, in any order:
      | n    | path              |
      | (:A) | <(:A)-[:T]->(:C)> |
      | (:A) | <(:A)-[:T]->(:B)> |
      | (:B) | null              |
      | (:C) | null              |
    And no side effects

  Scenario: Returning a CASE expression into integer
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)
      CREATE (a)-[:T]->(:B),
             (a)-[:T]->(:C)
      """
    When executing query:
      """
      MATCH (n)
      RETURN CASE
               WHEN id(n) < 0 THEN [p=(n)-->() | p]
               ELSE 42
             END AS p
      """
    Then the result should be, in any order:
      | p  |
      | 42 |
      | 42 |
      | 42 |
    And no side effects

  Scenario: Returning a CASE expression with label predicates
    Given an empty graph
    And having executed:
      """
      CREATE (a1:A1), (b1:B1), (a2:A2), (b2:B2)
      CREATE (a1)-[:T1]->(b1),
             (a1)-[:T2]->(b1),
             (a2)-[:T1]->(b2),
             (a2)-[:T2]->(b2)
      """
    When executing query:
      """
      MATCH (n)
      WITH n, CASE
          WHEN n:A1 THEN [p=(n)-->(:B1) | p]
          WHEN n:A2 THEN [p=(n)-->(:B2) | p]
          ELSE 42
          END AS p
      WITH n, p UNWIND p as path
      RETURN n, path
      """
    Then the result should be, in any order:
      | n   | path                 |
      |(:A1)| <(:A1)-[:T2]->(:B1)> |
      |(:A1)| <(:A1)-[:T1]->(:B1)> |
      |(:A2)| <(:A2)-[:T2]->(:B2)> |
      |(:A2)| <(:A2)-[:T1]->(:B2)> |
      |(:B1)| 42                   |
      |(:B2)| 42                   |
    And no side effects

  Scenario: Using a CASE expression in a WITH, positive case
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)
      CREATE (a)-[:T]->(:B),
             (a)-[:T]->(:C)
      """
    When executing query:
      """
      MATCH (n)
      WITH n, CASE
              WHEN id(n) >= 0 THEN [p=(n)-->() | p]
              ELSE 42
          END AS p
      WITH n, p UNWIND CASE
          WHEN p = [] THEN [null]
          ELSE p
          END AS path
      RETURN n, path
      """
    Then the result should be, in any order:
      | n    | path              |
      | (:A) | <(:A)-[:T]->(:C)> |
      | (:A) | <(:A)-[:T]->(:B)> |
      | (:B) | null              |
      | (:C) | null              |
    And no side effects

  Scenario: Using a CASE expression in a WITH, negative case
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)
      CREATE (a)-[:T]->(:B),
             (a)-[:T]->(:C)
      """
    When executing query:
      """
      MATCH (n)
      WITH CASE
             WHEN id(n) < 0 THEN [p=(n)-->() | p]
             ELSE 42
           END AS p, count(n) AS c
      RETURN p, c
      """
    Then the result should be, in any order:
      | p  | c |
      | 42 | 3 |
    And no side effects

  Scenario: Using a CASE expression with label predicates in a WITH
    Given an empty graph
    And having executed:
      """
      CREATE (a1:A1), (b1:B1), (a2:A2), (b2:B2)
      CREATE (a1)-[:T1]->(b1),
             (a1)-[:T2]->(b1),
             (a2)-[:T1]->(b2),
             (a2)-[:T2]->(b2)
      """
    When executing query:
      """
      MATCH (n)
      WITH n, CASE
              WHEN n:A1 THEN [p=(n)-->(:B1) | p]
              WHEN n:A2 THEN [p=(n)-->(:B2) | p]
              ELSE 42
          END AS p
      WITH n, p UNWIND CASE
          WHEN p = [] THEN [null]
          ELSE p
          END AS path
      RETURN n, path
      """
    Then the result should be, in any order:
      | n     | path                 |
      | (:A1) | <(:A1)-[:T2]->(:B1)> |
      | (:A1) | <(:A1)-[:T1]->(:B1)> |
      | (:A2) | <(:A2)-[:T2]->(:B2)> |
      | (:A2) | <(:A2)-[:T1]->(:B2)> |
      | (:B1) | 42                   |
      | (:B2) | 42                   |
    And no side effects

  Scenario: Using a CASE expression in a WHERE, positive case
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)
      CREATE (a)-[:T]->(:B),
             (a)-[:T]->(:C)
      """
    When executing query:
      """
      MATCH (n)
      WHERE (CASE
               WHEN id(n) >= 0 THEN size([p=(n)-->() | p])
               ELSE 42
             END) > 0
      RETURN n
      """
    Then the result should be, in any order:
      | n    |
      | (:A) |
    And no side effects

  Scenario: Using a CASE expression in a WHERE, negative case
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)
      CREATE (a)-[:T]->(:B),
             (a)-[:T]->(:C)
      """
    When executing query:
      """
      MATCH (n)
      WHERE (CASE
               WHEN id(n) < 0 THEN size([p=(n)-->() | p])
               ELSE 42
             END) > 0
      RETURN n
      """
    Then the result should be, in any order:
      | n    |
      | (:A) |
      | (:B) |
      | (:C) |
    And no side effects

  Scenario: Using a CASE expression in a WHERE, with relationship predicate
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)
      CREATE (a)-[:T]->(:B),
             (a)-[:T]->(:C)
      """
    When executing query:
      """
      MATCH (n)
      WHERE (CASE
               WHEN id(n) < 0 THEN size([p=(n)-[:X]->() | p])
               ELSE 42
             END) > 0
      RETURN n
      """
    Then the result should be, in any order:
      | n    |
      | (:A) |
      | (:B) |
      | (:C) |
    And no side effects

  Scenario: Using a CASE expression in a WHERE, with label predicate
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)
      CREATE (a)-[:T]->(:B),
             (a)-[:T]->(:C)
      """
    When executing query:
      """
      MATCH (n)
      WHERE (CASE
               WHEN id(n) < 0 THEN size([p=(n)-->(:X) | p])
               ELSE 42
             END) > 0
      RETURN n
      """
    Then the result should be, in any order:
      | n    |
      | (:A) |
      | (:B) |
      | (:C) |
    And no side effects

  Scenario: Returning a CASE expression with a pattern expression alternative
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {prop: 42})
      CREATE (a)-[:T]->(),
             (a)-[:T]->(),
             (a)-[:T]->()
      """
    When executing query:
      """
      MATCH (a:A)
      RETURN CASE
               WHEN a.prop = 42 THEN []
               ELSE [p=(a)-->() | p]
             END AS x
      """
    Then the result should be, in any order:
      | x  |
      | [] |
    And no side effects

  Scenario: Shorthand case with filtering pattern comprehension should work as expected
    Given an empty graph
    And having executed:
      """
      CREATE (:test)<-[:rel]-(:test_rel)
      """
    When executing query:
      """
      MATCH (t:test)
      WITH COLLECT(t) AS ts
      WITH
      CASE 1
          WHEN 0 THEN []
          ELSE [t IN ts WHERE (t)<--()]
      END AS res
      RETURN COUNT(res) AS count
      """
    Then the result should be, in any order:
      | count  |
      | 1      |
    And no side effects

  Scenario: When given null, any WHEN containing null should not match as null == null = null
    When executing query:
      """
      RETURN
      CASE null
          WHEN null THEN true
          ELSE false
      END AS res
      """
    Then the result should be, in any order:
      | res   |
      | false |
    And no side effects


  Scenario: When given null = null as a comparison it should evaluate to false
    When executing query:
      """
      RETURN
      CASE
          WHEN null = null THEN true
          ELSE false
      END AS res
      """
    Then the result should be, in any order:
      | res   |
      | false |
    And no side effects


  Scenario: When given null IS null as a comparison it should evaluate to true
    When executing query:
      """
      RETURN
      CASE
          WHEN null IS NULL THEN true
          ELSE false
      END AS res
      """
    Then the result should be, in any order:
      | res   |
      | true  |
    And no side effects


  Scenario: When given null and no default, null should be returned
    When executing query:
      """
      RETURN
      CASE null
          WHEN null THEN true
      END AS res
      """
    Then the result should be, in any order:
      | res   |
      | null  |
    And no side effects


  Scenario: Auto aliasing of null outside of CASE should work as expected
    When executing query:
      """
      RETURN null
      ORDER BY properties(CASE WHEN null THEN null END)
      """
    Then the result should be, in any order:
      | null  |
      | null  |
    And no side effects

  Scenario: Simple case with comma separated where lists
    Given an empty graph
    And having executed:
      """
      CREATE ({salary : 1000}),
            ({salary : 1500}),
            ({salary : 2000}),
            ({salary : 2500}),
            ({salary : 3000}),
            ({salary : 3500})
      """
    When executing query:
      """
      MATCH (n)
      RETURN
      CASE n.salary
          WHEN 1000, 1500 THEN 'low'
          WHEN 2000, 2500 THEN 'med'
          WHEN 3000, 3500 THEN 'high'
      END AS res
      """
    Then the result should be, in any order:
      | res    |
      | 'low'  |
      | 'low'  |
      | 'med'  |
      | 'med'  |
      | 'high' |
      | 'high' |
    And no side effects

  Scenario: Simple case with comma separated where lists and default
    Given an empty graph
    And having executed:
      """
      CREATE ({salary : 1000}),
            ({salary : 1500}),
            ({salary : 2000}),
            ({salary : 2500}),
            ({salary : 3000}),
            ({salary : 3500})
      """
    When executing query:
      """
      MATCH (n)
      RETURN
      CASE n.salary
          WHEN 1000, 1500 THEN 'low'
          WHEN 2000, 2500 THEN 'med'
          ELSE 'high'
      END AS res
      """
    Then the result should be, in any order:
      | res    |
      | 'low'  |
      | 'low'  |
      | 'med'  |
      | 'med'  |
      | 'high' |
      | 'high' |
    And no side effects

  Scenario: Simple case with comma separated where lists and more complicated expressions
    Given an empty graph
    And having executed:
      """
      CREATE ({salary : 1000}),
            ({salary : 1500}),
            ({salary : 2000}),
            ({salary : 2500}),
            ({salary : 3000}),
            ({salary : 3500})
      """
    When executing query:
      """
      MATCH (n)
      RETURN
      CASE n.salary
          WHEN 1000 + 500, id(n) * 0 + 1000 THEN 'low'
          WHEN 1000 * 2, 5000 - 2500 THEN 'med'
          ELSE 'high'
      END AS res
      """
    Then the result should be, in any order:
      | res    |
      | 'low'  |
      | 'low'  |
      | 'med'  |
      | 'med'  |
      | 'high' |
      | 'high' |
    And no side effects

  Scenario: Simple case with comparison operators
    Given an empty graph
    And having executed:
      """
      CREATE (),
            ({salary : 1000}),
            ({salary : 1500}),
            ({salary : 2000}),
            ({salary : 2500}),
            ({salary : 3000}),
            ({salary : 3500})
      """
    When executing query:
      """
      MATCH (n)
      RETURN
      CASE n.salary
          WHEN < 1500, IS NULL THEN 'low'
          WHEN <= 2500 THEN 'med'
          ELSE 'high'
      END AS res
      """
    Then the result should be, in any order:
      | res    |
      | 'low'  |
      | 'low'  |
      | 'med'  |
      | 'med'  |
      | 'med'  |
      | 'high' |
      | 'high' |
    And no side effects

  Scenario: Simple case with all comparison operators
    Given an empty graph
    And having executed:
      """
      CREATE ({name : "Alice"}),
            ({name : "Bob"}),
            ({name : "Cat"}),
            ({name : "Dave"}),
            ({name : "Erik"}),
            ({name : "Fred"})
      """
    When executing query:
      """
      MATCH (n)
      RETURN
      CASE n.name
          WHEN IS NULL THEN 1
          WHEN IS NOT NORMALIZED THEN 2
          WHEN IS NOT NFKD NORMALIZED THEN 3
          WHEN IS TYPED BOOLEAN THEN 4
          WHEN IS NOT TYPED STRING THEN 5
          WHEN :: POINT THEN 6
          WHEN STARTS WITH "A" THEN 7
          WHEN ENDS WITH "k" THEN 8
          WHEN =~ 'C.*t' THEN 9
          WHEN IS NOT NULL THEN 10
          WHEN IS NORMALIZED THEN 11
          ELSE 13
      END AS res
      """
    Then the result should be, in any order:
      | res |
      | 7   |
      | 8   |
      | 9   |
      | 10  |
      | 10  |
      | 10  |
    And no side effects