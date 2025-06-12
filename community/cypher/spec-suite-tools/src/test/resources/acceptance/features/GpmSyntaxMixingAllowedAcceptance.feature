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

Feature: GpmSyntaxMixingAllowedAcceptance

  Background:
    Given an empty graph

  Scenario: Allowed to mix label expression syntax in CALL with AllowClauseWithMixedLabelSyntax
    Given an empty graph
    And having executed:
      """
      CREATE (:A {id:0})
      CREATE (:B {id:1})
      CREATE (:C {id:2})
      CREATE (:A:B {id:3})
      CREATE (:A:C {id:4})
      """
    When executing query:
      """
      CALL {
        MATCH (e:A|B) RETURN e
        UNION
        MATCH (e:A:C) RETURN e
      }
      RETURN *
      ORDER BY e.id
      """
    Then the result should be, in order:
      | e              |
      | (:A {id: 0})   |
      | (:B {id: 1})   |
      | (:A:B {id: 3}) |
      | (:A:C {id: 4}) |
    And no side effects

  Scenario: Mixing & and : in label predicates in same statement
    When executing query:
      """
      MATCH (n)
      RETURN n:A&B, n:A:B
      """
    Then the result should be, in order:
      | n:A&B | n:A:B |


  Scenario Outline: Conflicting syntax in separate statements in same COUNT sub-query
    And having executed:
      """
      CREATE (n:A:B)-[:Q]->(:A:C)
      CREATE (n)-[:R]->()
      """
    When executing query:
      """
      RETURN COUNT {
        <statement1>
        <statement2>
      } > 0 AS result
      """
    Then the result should be, in order:
      | result |
      | true   |
    Examples:
      | statement1    | statement2            |
      | MATCH (n:A:B) | MATCH (n)--(:A&!B)    |
      | MATCH (n:A:B) | MATCH (n)-[:!R&!S]-() |
      | MATCH (n:A:B) | MATCH (n)--(IS A)     |
      | MATCH (n:A:B) | MATCH (n)-[IS R]-()   |

  Scenario Outline: Conflicting syntax in separate statements in same EXISTS sub-query
    And having executed:
      """
      CREATE (n:A:B)-[:R]->(:A:C)
      CREATE (n)-[:Q]->()
      """
    When executing query:
      """
      RETURN EXISTS {
        <statement1>
        <statement2>
      } AS result
      """
    Then the result should be, in order:
      | result |
      | true   |
    Examples:
      | statement1    | statement2            |
      | MATCH (n:A:B) | MATCH (n)--(:A&!B)    |
      | MATCH (n:A:B) | MATCH (n)-[:!R&!S]-() |
      | MATCH (n:A:B) | MATCH (n)--(IS A)     |
      | MATCH (n:A:B) | MATCH (n)-[IS R]-()   |


  Scenario Outline: Conflicting syntax in separate statements within a CALL subquery
    And having executed:
      """
      <setup>
      """
    When executing query:
      """
      CALL {
        <statement1>
        <statement2>
        RETURN *
      }
      RETURN collect({n: n, m: m}) AS result
      """
    Then the result should be (ignoring element order for lists):
      | result     |
      | <expected> |

    Examples:
      | statement1    | statement2             | setup                    | expected                      |
      | MATCH (n:A:B) | MATCH (n)--(m:A&!B)    | CREATE (:A:B)-[:Q]->(:A) | [{m: (:A {}), n: (:A:B {})}]  |
      | MATCH (n:A:B) | MATCH (n)-[m:!R&!S]-() | CREATE (:A:B)-[:R]->(:A) | [] |
      | MATCH (n:A:B) | MATCH (n)--(m IS A)    | CREATE (:A:B)-[:Q]->(:A) | [{m: (:A {}), n: (:A:B {})}]  |
      | MATCH (n:A:B) | MATCH (n)-[IS R]-(m)   | CREATE (:A:B)-[:R]->(:A) | [{m: (:A {}), n: (:A:B {})}]  |
    @fails:parallel-runtime
    Examples:
      | statement1            | statement2      | setup                    | expected                                                    |
      | CREATE (n:A:B)        | CREATE (m:C&D)  | CREATE (:A:B)-[:Q]->(:A) | [{m: (:C:D {}), n: (:A:B {})}]                              |
      | MERGE (n:A:B)         | CREATE (m IS C) | CREATE (:A:B)-[:Q]->(:A) | [{m: (:C {}), n: (:A:B {})}]                                |
      | MERGE (n IS A&B)      | CREATE (m:C:D)  | CREATE (:A:B)-[:Q]->(:A) | [{m: (:C:D {}), n: (:A:B {})}]                              |
      | MATCH (n:A&B)         | MERGE (m:B:C)   | CREATE (:A:B)-[:Q]->(:A) | [{m: (:B:C {}), n: (:A:B {})}]                              |
      | MATCH (n IS A)        | MERGE (m:B:C)   | CREATE (:A:B)-[:Q]->(:A) | [{m: (:B:C {}), n: (:A:B {})}, {m: (:B:C {}), n: (:A {})}] |
      | MATCH (:A)-[:!R]->(n) | MERGE (m:B:C)   | CREATE (:A:B)-[:Q]->(:A) | [{m: (:B:C {}), n: (:A {})}]                                |

  Scenario: Mixing QPP and var-length relationship quantifiers in pattern expressions in same statement - syntax error
    When executing query:
      """
      RETURN [(n)-->+(m) | m], [(n)-[*3]-(m) | m]
      """
    Then a SyntaxError should be raised at compile time: *


  Scenario Outline: Mixing : conjunction with GPM-only label/type expression operators - syntax error
    When executing query:
      """
      MATCH <graphPattern>
      RETURN *
      """
    Then a SyntaxError should be raised at compile time: *

    Examples:
      | graphPattern | rewrite     |
      | (n:A\|B:C)   | `:A\|(B&C)` |

  Scenario Outline: Mixing : conjunction with GPM-only label/type expression operators
    And having executed:
      """
      <setup>
      """
    When executing query:
      """
      MATCH <graphPattern>
      RETURN collect({n: n, m: m}) AS result
      """
    Then the result should be, in order (ignoring element order for lists):
      | result     |
      | <expected> |
    Examples:
      | graphPattern                                     | setup                                     | expected                                                                                           |
      | (n:A:B:C)-->(m:(A&B)\|C)                         | CREATE (:A:B:C)-[:R]->(:A:B:C)-[:Q]->(:C) | [{m: (:A:B:C {}), n: (:A:B:C {})}, {m: (:C {}), n: (:A:B:C {})}]                                   |
      | (n:A:B)--(:C), (n)-->(m:(A&B)\|C)                | CREATE (:A:B:C)-[:R]->(:A:B:C)-[:Q]->(:C) | [{m: (:C {}), n: (:A:B:C {})}]                                                                     |
      | (n:A:B)-[]-(m) WHERE m:(A&B)\|C                  | CREATE (:A:B:C)-[:R]->(:A:B:C)-[:Q]->(:C) | [{m: (:A:B:C {}), n: (:A:B:C {})}, {m: (:A:B:C {}), n: (:A:B:C {})}, {m: (:C {}), n: (:A:B:C {})}] |
      | (n:A:B)-[]-(m) WHERE NOT EXISTS { (m:(A&B)\|C) } | CREATE (:A:B:C)-[:R]->(:A:B:C)-[:Q]->(:X) | [{m: (:X {}), n: (:A:B:C {})}]                                                                     |

  Scenario Outline: Mixing : conjunction of label expression with IS introducer of label or type expression
    And having executed:
      """
      <setup>
      """
    When executing query:
      """
      <statement>
      RETURN collect({n: n, m: m}) AS result
      """
    Then the result should be, in order (ignoring element order for lists):
      | result     |
      | <expected> |
    Examples:
      | statement                                                               | setup                                                   | expected                                          |
      | MATCH (m)-->+(n IS S:R)                                                 | CREATE ()-[:R]->()-[:R]->(:S:R)-[:R]->(:X)              | [{m: ({}), n: (:R:S{})}, {m: ({}), n: (:R:S {})}] |
      | MATCH (m)-[IS Q]->+(n:S:R)                                              | CREATE ()-[:R]->()-[:Q]->(:S:R)-[:R]->(:X)              | [{m:({}), n:(:R:S{})}]                            |
      | MATCH (m)-[:Q]->+(n:S:R), (m)-[IS T]-+(n)                               | CREATE (m)-[:Q]->(n:S:R) CREATE (m)-[:T]->()-[:T]->(n)  | [{m: ({}), n: (:R:S {})}]                         |
      | MATCH (m)-[:Q]->+(n:S:R), (m)--+(IS T)                                  | CREATE (m)-[:Q]->(n:S:R) CREATE (m)-[:T]->()-[:T]->(:T) | [{m: ({}), n: (:R:S {})}]                         |
      | MATCH (:P) ((n)-[:Q]->(m:R) WHERE EXISTS { (n)-->+(IS S) })+ (:S:R)     | CREATE (:P)-[:Q]->(:S:R)-[:R]->(:S)                     | [{m: [(:R:S {})], n: [(:P {})]}]                  |
      | MATCH (:P) ((n)-[:Q]->(m:S:R) WHERE EXISTS { (n)-->+(IS S) })+ (:S)     | CREATE (:P)-[:Q]->(:S:R)-[:R]->(:S)                     | [{m: [(:R:S {})], n: [(:P {})]}]                  |
      | MATCH (:P) ((n)-[:Q]->(m IS S) WHERE EXISTS { (n)-->+(:S:T) })+ (:S)    | CREATE (:P)-[:Q]->(:S:R)-[:R]->(:S:T)                   | [{m: [(:R:S {})], n: [(:P {})]}]                  |
      | MATCH (:P) ((n)-[:Q]->(m:S) WHERE EXISTS { (n)-->+(:S:T) })+ (IS S)     | CREATE (:P)-[:Q]->(:S:R)-[:R]->(:S:T)                   | [{m: [(:R:S {})], n: [(:P {})]}]                  |
      | MATCH (:P) ((n)-[IS Q]->(m:S) WHERE EXISTS { (n)-->+(:S:T) })+ (:S)     | CREATE (:P)-[:Q]->(:S:R)-[:R]->(:S:T)                   | [{m: [(:R:S {})], n: [(:P {})]}]                  |
      | MATCH (:P) ((n)-[:Q]->(m:R) WHERE EXISTS { (n)-[IS S]->+(:T) })+ (:S:R) | CREATE (n:P)-[:Q]->(:S:R) CREATE (n)-[:S]->(:T)         | [{m: [(:R:S {})], n: [(:P{})]}]                   |
      | MATCH (:P) ((n)-[:Q]->(m:S:R) WHERE EXISTS { (n)-[IS S]->+(:T) })+ (:S) | CREATE (n:P)-[:Q]->(:S:R) CREATE (n)-[:S]->(:T)         | [{m: [(:R:S{})], n: [(:P{})]}]                    |


  Scenario Outline: Applying both var-length and QPP quantifiers in the same statement - syntax error
    When executing query:
      """
      <statement>
      RETURN r
      """
    Then a SyntaxError should be raised at compile time: *

    Examples:
      | statement                                       |
      | MATCH ()-[r:A*]->*()                            |
      | MATCH ()-[r:A*1..2]->{1,2}()                    |
      | MATCH ()-[r:A*1..2]->+()                        |
      | MATCH ()-[r:A*1..2]->()-->+()                   |
      | MATCH ()-[r:A*]->()-[]->{2}()                   |
      | MATCH ()-[r:A*]->() (()-[:B]->(:C)){2,} (:D)    |
      | MATCH ()-[r:A*]->(n), (()-[:B]->(:C)){2,} (n:D) |
      | MATCH ()-[r:A*]->(n), ()-[:B]->+(:C)            |

  Scenario Outline: Applying GPM path selector and shortestPath to same pattern - syntax error
    When executing query:
      """
      <statement>
      RETURN *
      """
    Then a SyntaxError should be raised at compile time: *

    Examples:
      | statement                                                                              |
      | MATCH p = ANY SHORTEST shortestPath((:A)-[*..5]-(:B))                                  |
      | MATCH p = SHORTEST 2 shortestPath((:A)-[*..5]-(:B))                                    |
      | MATCH p = ALL SHORTEST shortestPath((:A)-[*..5]-(:B))                                  |
      | MATCH p = SHORTEST GROUP shortestPath((:A)-[*..5]-(:B))                                |
      | MATCH p = SHORTEST 2 GROUPS shortestPath((:A)-[*..5]-(:B))                             |
      | MATCH p = ANY SHORTEST allShortestPaths((:A)-[*..5]-(:B))                              |
      | MATCH p = SHORTEST 2 allShortestPaths((:A)-[*..5]-(:B))                                |
      | MATCH p = ALL SHORTEST allShortestPaths((:A)-[*..5]-(:B))                              |
      | MATCH p = SHORTEST GROUP allShortestPaths((:A)-[*..5]-(:B))                            |
      | MATCH p = SHORTEST 2 GROUPS allShortestPaths((:A)-[*..5]-(:B))                         |
      | MATCH p = SHORTEST 2 GROUPS allShortestPaths((:A)-[*..5]-(:B))                         |
      | WITH EXISTS { ALL SHORTEST allShortestPaths((:A)-[*..5]-(:B)) } AS x                   |
      | WITH COUNT { ALL SHORTEST allShortestPaths((:A)-[r*]->(:B)) } AS x                     |
      | WITH COLLECT { MATCH p = ALL SHORTEST allShortestPaths((:A)-[*]->(:B)) RETURN p } AS x |

  Scenario Outline: Applying shortestPath to QPP - syntax error
    When executing query:
      """
      MATCH <pattern>
      RETURN *
      """
    Then a SyntaxError should be raised at compile time: *

    Examples:
      | pattern                                  |
      | p = shortestPath((n)-[]->+({s: 1}))      |
      | p = allShortestPaths((n)-[]->+({s: 1}))  |
      | p = shortestPath( ((:A)-[:R]->())+ )     |
      | p = allShortestPaths( ((:A)-[:R]->())+ ) |

  Scenario Outline: Explicit match mode <matchMode> with <pathFunction> - syntax error
    When executing query:
      """
      MATCH <matchMode> p = <pathFunction>(()-[*]->())
      RETURN p
      """
    Then a SyntaxError should be raised at compile time: *

    Examples:
      | matchMode               | pathFunction     |
      | REPEATABLE ELEMENTS     | shortestPath     |
      | REPEATABLE ELEMENTS     | allShortestPaths |
      | DIFFERENT RELATIONSHIPS | shortestPath     |
      | DIFFERENT RELATIONSHIPS | allShortestPaths |
      | DIFFERENT NODES         | shortestPath     |
      | DIFFERENT NODES         | allShortestPaths |

  Scenario Outline: Mixing : and & in same DML statement - syntax error
    When executing query:
      """
      <statement>
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | statement        | rewrite  |
      | CREATE (n:A:B&C) | `:A&B&C` |
      | MERGE (n:A:B&C)  | `:A&B&C` |

  @fails:parallel-runtime
  Scenario Outline: Mixing : and & in same DML statement - OK
    When executing query:
      """
      <statement>
      """
    Then the result should be empty
    When executing control query:
      """
      MATCH p = ()-[*0..1]->()
      RETURN collect(p) AS paths
      """
    Then the result should be, in order (ignoring element order for lists):
      | paths      |
      | <expected> |
    Examples:
      | statement                  | expected                                                   |
      | CREATE (:A:B)-[:R]->(:T&S) | [<(:A:B {})>, <(:A:B {})-[:R {}]->(:S:T {})>, <(:S:T {})>] |
      | MERGE (:A:B)-[:R]->(:T&S)  | [<(:A:B {})>, <(:A:B {})-[:R {}]->(:S:T {})>, <(:S:T {})>] |

  @fails:parallel-runtime
  Scenario Outline: Mixing : conjunction and IS in same label specification of same CREATE or MERGE
    When executing query:
      """
      <statement>
      """
    Then the result should be empty
    When executing control query:
      """
      MATCH p = ()-[*0..1]->()
      RETURN collect(p) AS paths
      """
    Then the result should be, in order (ignoring element order for lists):
      | paths      |
      | <expected> |

    Examples:
      | statement                    | expected                                                   |
      | CREATE (IS A:B:C)            | [<(:A:B:C {})>]                                            |
      | CREATE (:A:B:C), (IS A&B)    | [<(:A:B:C {})>, <(:A:B {})>]                               |
      | CREATE (:A:B)-[:R]->(IS T:S) | [<(:A:B {})>, <(:A:B {})-[:R {}]->(:S:T {})>, <(:S:T {})>] |
      | CREATE (:A:B)-[IS R]->(:T:S) | [<(:A:B {})>, <(:A:B {})-[:R {}]->(:S:T {})>, <(:S:T {})>] |
      | MERGE (IS A:B:C)             | [<(:A:B:C {})>]                                            |
      | MERGE (:A:B)-[:R]->(IS T:S)  | [<(:A:B {})>, <(:A:B {})-[:R {}]->(:S:T {})>, <(:S:T {})>] |
      | MERGE (:A:B)-[IS R]->(:T:S)  | [<(:A:B {})>, <(:A:B {})-[:R {}]->(:S:T {})>, <(:S:T {})>] |

  Scenario Outline: Mixing : conjunction and IS in same label specification of same SET or REMOVE - syntax error
    When executing query:
      """
      MATCH (n)
      <statement>
      """
    Then a SyntaxError should be raised at compile time: *

    Examples:
      | statement              | rewrite                          | operator |
      | SET n IS A:B:C         | 'n IS A, n IS B, n IS C'         | SET      |
      | SET n:A:B:C, n IS A    | 'n IS A, n IS B, n IS C, n IS A' | SET      |
      | REMOVE n IS A:B:C      | 'n IS A, n IS B, n IS C'         | REMOVE   |
      | REMOVE n:A:B:C, n IS A | 'n IS A, n IS B, n IS C, n IS A' | REMOVE   |

