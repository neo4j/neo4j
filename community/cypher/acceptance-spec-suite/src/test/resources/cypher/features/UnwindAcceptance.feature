#
# Copyright (c) 2002-2017 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
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
      UNWIND [42, 0.7, true, 's', n, r] as i
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
      UNWIND [[42],[0.7],[true],[n],[r],[n,42],[r,42]] as i
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
               {k: [r], l: 's'},
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
      | {k: [[:R]], l: 's'}      |
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
               {k: [r], l: 's'},
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
      | {k: [[:R]], l: 's'}     |
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

