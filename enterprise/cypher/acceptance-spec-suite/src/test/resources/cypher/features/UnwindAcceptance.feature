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

Feature: UnwindAcceptance

  Scenario: Flat type support in list literal
    Given an empty graph
    And having executed:
      """
      CREATE (:A {prop: 'a'})-[:R {prop: 'r'}]->()
      """
    When executing query:
      """
      MATCH (n:A {prop: 'a'})-[r:R {prop: 'r'}]->()
      WITH *
      UNWIND [42, 0.7, true, 's', n, r, null] as i
      RETURN i
      ORDER BY "no order"
      """
    Then the result should be:
      | i                 |
      | 42                |
      | 0.7               |
      | true              |
      | 's'               |
      | (:A {prop: 'a'})  |
      | [:R {prop: 'r'}]  |
      | null              |
    And no side effects

  Scenario: Nested type support in list literal
    Given an empty graph
    And having executed:
      """
      CREATE (:A {prop: 'a'})-[:R {prop: 'r'}]->()
      """
    When executing query:
      """
      MATCH (n:A {prop: 'a'})-[r:R {prop: 'r'}]->()
      WITH *
      UNWIND [[42],[0.7],[true],[n],[r],[n,42],[r,42],[null]] as i
      RETURN i
      ORDER BY "no order"
      """
    Then the result should be:
      | i                      |
      | [42]                   |
      | [0.7]                  |
      | [true]                 |
      | [(:A {prop: 'a'})]     |
      | [[:R {prop: 'r'}]]     |
      | [(:A {prop: 'a'}), 42] |
      | [[:R {prop: 'r'}], 42] |
      | [null]                 |
    And no side effects

  Scenario: Nested type support in map literal
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:R]->()
      """
    When executing query:
      """
      MATCH (n:A)-[r:R]->()
      WITH *
      UNWIND [ {k: n},
               {k: r},
               {k: n, l: 42},
               {k: r, l: 's'}
             ] as i
      RETURN i
      ORDER BY "no order"
      """
    Then the result should be:
      | i                 |
      | {k: (:A)}         |
      | {k: [:R]}         |
      | {k: (:A), l: 42}  |
      | {k: [:R], l: 's'} |
    And no side effects

  Scenario: Nested type support with mixed list and map literals
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:R]->()
      """
    When executing query:
      """
      MATCH (n:A)-[r:R]->()
      WITH *
      UNWIND [ {k: [n]},
               {k: [r]},
               {k: [n, r], l: 42},
               {k: [r, null], l: 's'},
               [ {k: [n, r]} ]
             ] as i
      RETURN i
      ORDER BY "no order"
      """
    Then the result should be:
      | i                        |
      | {k: [(:A)]}              |
      | {k: [[:R]]}              |
      | {k: [(:A), [:R]], l: 42} |
      | {k: [[:R], null], l: 's'}|
      | [{k: [(:A), [:R]]}]      |
    And no side effects

  Scenario: Nested type support with mixed list and map literals with projection
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:R]->()
      """
    When executing query:
      """
      MATCH (n:A)-[r:R]->()
      WITH *
      UNWIND [ {k: [n]},
               {k: [r]},
               {k: [n, r], l: 42},
               {k: [r, null], l: 's'},
               [ {k: [n, r]} ]
             ] as i
      WITH i as j
      RETURN j
      ORDER BY "no order"
      """
    Then the result should be:
      | j                        |
      | {k: [(:A)]}              |
      | {k: [[:R]]}              |
      | {k: [(:A), [:R]], l: 42} |
      | {k: [[:R], null], l: 's'}|
      | [{k: [(:A), [:R]]}]      |
    And no side effects

  Scenario: Primitive node type support in list literal
    Given an empty graph
    And having executed:
      """
      CREATE (:A)
      CREATE (:B)
      CREATE (:C)
      """
    When executing query:
      """
      MATCH (a:A), (b:B), (c:C)
      WITH *
      UNWIND [a, b, c] as i
      RETURN i
      ORDER BY "no order"
      """
    Then the result should be:
      | i    |
      | (:A) |
      | (:B) |
      | (:C) |
    And no side effects

  Scenario: Primitive relationship type support in list literal
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:R]->()
      CREATE ()-[:S]->()
      """
    When executing query:
      """
      MATCH ()-[r:R]->(), ()-[s:S]->()
      WITH *
      UNWIND [r, s] as i
      RETURN i
      ORDER BY "no order"
      """
    Then the result should be:
      | i    |
      | [:R] |
      | [:S] |
    And no side effects

  Scenario: Nested primitive lists and UNWIND
    Given an empty graph
    When executing query:
      """
      UNWIND [[1],[2],[3]] AS i RETURN i
      """
    Then the result should be:
      | i   |
      | [1] |
      | [2] |
      | [3] |
    And no side effects

  Scenario: Unwind on array property
    Given an empty graph
    And having executed:
      """
      CREATE (:L {array:['a', 'b', 'c']})
      """
    When executing query:
      """
      MATCH (n:L)
      UNWIND n.array AS array
      RETURN array
      """
    Then the result should be:
      | array |
      | 'a'   |
      | 'b'   |
      | 'c'   |
    And no side effects

  Scenario: Nested unwind with longs
    Given an empty graph
    When executing query:
      """
      WITH [[1, 2], [3, 4], 5] AS nested
          UNWIND nested AS x
          UNWIND x AS y
          RETURN y
      """
    Then the result should be:
        | y |
        | 1 |
        | 2 |
        | 3 |
        | 4 |
        | 5 |
    And no side effects

  Scenario: Nested unwind with doubles
    Given an empty graph
    When executing query:
      """
      WITH [[1.5, 2.5], [3.5, 4.5], 5.5] AS nested
          UNWIND nested AS x
          UNWIND x AS y
          RETURN y
      """
    Then the result should be:
      | y   |
      | 1.5 |
      | 2.5 |
      | 3.5 |
      | 4.5 |
      | 5.5 |
    And no side effects

  Scenario: Nested unwind with strings
    Given an empty graph
    When executing query:
      """
      WITH [['a', 'b'], ['c', 'd'], 'e'] AS nested
          UNWIND nested AS x
          UNWIND x AS y
          RETURN y
      """
    Then the result should be:
      | y   |
      | 'a' |
      | 'b' |
      | 'c' |
      | 'd' |
      | 'e' |
    And no side effects

  Scenario: Nested unwind with mixed types
    Given an empty graph
    When executing query:
      """
      WITH [['a', 'b'], ['1.5', null, 'c'], '2', 'd' ] AS nested
          UNWIND nested AS x
          UNWIND x AS y
          RETURN y
      """
    Then the result should be:
      | y     |
      | 'a'   |
      | 'b'   |
      | '1.5' |
      | null  |
      | 'c'   |
      | '2'   |
      | 'd'   |
    And no side effects

  Scenario: Pattern comprehension in unwind with empty db
    Given an empty graph
    When executing query:
      """
        UNWIND [(a)-->(b) | b ] as c
        RETURN c
      """
    Then the result should be:
      | c |
    And no side effects

  Scenario: Pattern comprehension in unwind with hits
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)
      CREATE (b:B)
      CREATE (c:C)
      CREATE (a)-[:T]->(b),
             (b)-[:T]->(c)
      """
    When executing query:
      """
        UNWIND [(a)-->(b) | b ] as c
        RETURN c
      """
    Then the result should be:
      | c     |
      | (:B)  |
      | (:C)  |
    And no side effects
