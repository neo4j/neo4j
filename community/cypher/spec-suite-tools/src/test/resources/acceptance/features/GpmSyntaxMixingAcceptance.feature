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

Feature: GpmSyntaxMixingAcceptance

  Background:
    Given an empty graph

  Scenario: MATCH path pattern with colon conjunction and VLR quantifier - OK
    When executing query:
      """
      MATCH (n:A:B:C)-[*]->()
      RETURN n
      """
    Then the result should be, in any order:
      | n |

    Scenario: MATCH path pattern with colon conjunction and rel type disjunction - OK
    When executing query:
      """
      MATCH (n:A:B)-[r:S|T|U]-()
      RETURN n
      """
    Then the result should be, in any order:
      | n |
#
    Scenario: shortestPath with var length relationship pattern - OK
    When executing query:
      """
      MATCH p = shortestPath(()-[*1..5]-())
      RETURN p
      """
      Then the result should be, in any order:
        | p |

    Scenario Outline: MATCH statement with all GPM syntax - OK
    When executing query:
      """
      MATCH <graphPattern>
      RETURN n
      """
    Then the result should be, in any order:
      | n |
    Examples:
      | graphPattern                           |
      | (n)-[r:A&B]->*()                       |
      | (n:(A&B)\|C)-[]->+()                   |
      | (n IS (A&B)\|C)-[IS R]->+()            |
      | (n IS (A&B)\|C)-[:R&S]->+()            |
      | (n:(A&B)\|C)-[]->+(), (n)-[r:A&B]->*() |

  Scenario: CREATE with only colon conjunction - OK
    When executing query:
      """
      CREATE (:A:B)-[:R]->(:T:S)
      """
    Then the result should be empty

  Scenario: MERGE with only : conjunction - OK
    When executing query:
      """
      MERGE (:A:B)-[:R]->(:T:S)
      """
    Then the result should be empty

  Scenario: REMOVE with only : conjunction - OK
    When executing query:
      """
      MATCH (n), (m)
      REMOVE n:A:B, m:A:B
      """
    Then the result should be empty

  Scenario: SET with only : conjunction - OK
    When executing query:
      """
      MATCH (n), (m)
      SET n:A:B, m:A:B
      """
    Then the result should be empty

  Scenario: CREATE with only & conjunction - OK
    When executing query:
      """
      CREATE (IS A&B)-[:R]->(:T&S)
      """
    Then the result should be empty

  Scenario: MERGE with only & conjunction - OK
    When executing query:
      """
      MERGE (IS A&B)-[:R]->(:T&S)
      """
    Then the result should be empty

  Scenario: INSERT with only & conjunction - OK
    When executing query:
      """
      INSERT (IS A&B)-[:R]->(:T&S)
      """
    Then the result should be empty

  Scenario: REMOVE with IS and : introducers - OK
    When executing query:
      """
      MATCH (n), (m)
      REMOVE n IS A, n IS B, m:A, m:B
      """
    Then the result should be empty

  Scenario: SET with IS and : introducers - OK
    When executing query:
      """
      MATCH (n), (m)
      SET n IS A, n IS B, m:A, m:B
      """
    Then the result should be empty

  Scenario: SHORTEST k with QPP - OK
    When executing query:
      """
      MATCH p = SHORTEST 2 PATHS ()-[]-{1,5}()
      RETURN p;
      """
    Then the result should be, in any order:
      | p |

  Scenario: REPEATABLE ELEMENTS with QPP - OK
    When executing query:
      """
      MATCH REPEATABLE ELEMENTS p = SHORTEST 2 PATHS ()-[]-{1,5}()
      RETURN p
      """
    Then the result should be, in any order:
      | p |

  Scenario: Colon conjunction with other label expression operators in different statements - OK
    When executing query:
      """
      MATCH (m:A:B:C)-[]->()
      MATCH (n:(A&B)|C)-[]->(m)
      RETURN n
      """
    Then the result should be, in any order:
      | n |

  Scenario: Var length relationship with QPP in different statements - OK
    When executing query:
      """
      MATCH (n)-[r*]-(m)
      MATCH (n)-[]->+()
      RETURN n
      """
    Then the result should be, in any order:
      | n |

  Scenario: SHORTEST k with shortestPath in different statements - OK
    When executing query:
      """
      MATCH p = shortestPath(()-[*1..5]-())
      MATCH q = SHORTEST 2 PATHS ()-[]-{1,5}()
      RETURN q
      """
    Then the result should be, in any order:
      | q |

  Scenario: Label expression with : conjunction with label predicate with | in different statements - OK
    When executing query:
      """
      MATCH (m:A:B:C)-[]->()
      RETURN
        CASE
          WHEN m:D|E THEN m.p
          ELSE null
        END AS q
      """
    Then the result should be, in any order:
      | q |

  Scenario: Mixing IS in SET with colon conjunction in MATCH and REMOVE - OK
    When executing query:
      """
      MATCH (m:A:B:C)
      SET m IS D
      REMOVE m:A:B:C
      """
    Then the result should be empty

  Scenario: REPEATABLE ELEMENTS with shortestPath in different statements - OK
    When executing query:
      """
      MATCH REPEATABLE ELEMENTS (:Q)-[*]->(q)
      MATCH p = shortestPath((q)-[*]->())
      RETURN p
      """
    Then the result should be, in any order:
      | p |

  Scenario: QPP with label expression using : conjunction in different element patterns - OK
    When executing query:
      """
      MATCH (m)-[]->+(n:S:R)
      RETURN m
      """
    Then the result should be, in any order:
      | m |

  Scenario: QPP with label expression using : conjunction in contained element pattern - OK
    When executing query:
      """
      MATCH p=((a:A:B)-[]->(b) WHERE a.p < b.p)+
      RETURN p
      """
    Then the result should be, in any order:
      | p |

  Scenario: Var-length relationship with inline predicate - OK
    When executing query:
      """
      MATCH ()-[r*1..5 WHERE r.p > 30]->()
      RETURN r
      """
    Then the result should be, in any order:
      | r |

  Scenario: Var-length relationship with GPM relationship type expression - OK
    When executing query:
      """
      MATCH ()-[r:!A&!B*1..5]->()
      RETURN r
      """
    Then the result should be, in any order:
      | r |

  Scenario: SHORTEST k with var-length relationship - OK
    When executing query:
      """
      MATCH p = SHORTEST 2 PATHS (m)-[*0..5]-(n)
      RETURN p
      """
    Then the result should be, in any order:
      | p |

  Scenario: DIFFERENT NODES with var-length relationship - OK
    When executing query:
      """
      MATCH DIFFERENT NODES (m)-[*0..5]-()
      RETURN m
      """
    Then the result should be, in any order:
      | m |

  Scenario: REPEATABLE ELEMENTS with var-length relationship - OK
    When executing query:
      """
      MATCH REPEATABLE ELEMENTS p = (m:A:B)-[*0..5]-(n)
      RETURN p
      """
    Then the result should be, in any order:
      | p |

  Scenario Outline: Mixing GPM and non-GPM of unrelated features within a subquery - OK
    When executing query:
      """
      CALL {
        <statement1>
        <statement2>
        RETURN *
      }
      RETURN n
      """
    Then the result should be, in any order:
      | n |
    Examples:
      | statement1                                  | statement2                                  |
      | MATCH (n:A:B)                               | CREATE (n)-[IS R]->(m IS B)                 |
      | MATCH (n:A:B)                               | CREATE (n)-[:R]->(m:A&B)                    |
      | MATCH (n:A:B)                               | MATCH (n)--+(:A)                            |
      | MATCH (n:A:B)                               | MATCH SHORTEST 1 (n)-->+(:B)                |
      | MATCH (n:A:B)                               | MATCH REPEATABLE ELEMENTS (n)-->(:B)-->()   |
      | MATCH (n:A&B)                               | SET n:B:C                                   |
      | MATCH (n IS A)                              | SET n:B:C                                   |
      | MATCH (:A)-[:!R]->(n)                       | SET n:B:C                                   |
      | MATCH (n)-->+(:B)                           | SET n:B:C                                   |
      | MATCH p = SHORTEST 1 (:A)-->+(n:B)          | SET n:B:C                                   |
      | MATCH REPEATABLE ELEMENTS (:A)-->(:B)-->(n) | SET n:B:C                                   |
      | MATCH (:A)-[*1..5]->(m:B)                   | WITH m:!A AS n                              |
      | MATCH (:A)-[*1..5]->(n:B)                   | MATCH (n)-[IS !R]->(:A)                     |
      | MATCH (:A)-[*1..5]->(n:B)                   | SET n IS D                                  |
      | MATCH (:A)-[*1..5]->(n:B)                   | MERGE (m IS D)                              |
      | MATCH (:A)-[*1..5]->(n:B)                   | CREATE (n)-[:R]->(:A&B)                     |
      | MATCH (:A)-[*1..5]->(n:B)                   | MATCH ANY (n)-->(:B)-->(:C)                 |
      | MATCH (:A)-[*1..5]->(n:B)                   | MATCH REPEATABLE ELEMENTS (n)-->(:B)-->()   |
      | MATCH shortestPath((:A)-->(m:B))            | WITH m:!A AS n                              |
      | MATCH shortestPath((:A)-->(n:B))            | MATCH (n)-[IS !R]->(:A)                     |
      | MATCH shortestPath((:A)-->(n:B))            | SET n IS D                                  |
      | MATCH shortestPath((:A)-->(n:B))            | MERGE (m IS D)                              |
      | MATCH shortestPath((:A)-->(n:B))            | CREATE (n)-[:R]->(:A&B)                     |
      | MATCH shortestPath((:A)-->(n:B))            | MATCH (n)-->+(:B)                           |
      | MATCH (n)-->+(:B)                           | MERGE (m:B:C)                               |
      | MATCH p = SHORTEST 1 (:A)-->+(n:B)          | MERGE (m:B:C)                               |
      | MATCH REPEATABLE ELEMENTS (:A)-->(:B)-->(n) | MERGE (m:B:C)                               |

  Scenario: Mixing & and : in label predicates in same statement - syntax error
    When executing query:
      """
      MATCH (n)
      RETURN n:A&B, n:A:B
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario Outline: Conflicting syntax in separate statements in same COUNT sub-query - syntax error
    When executing query:
      """
      RETURN COUNT {
        <statement1>
        <statement2>
      }
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | statement1                                  | statement2                                  |
      | MATCH (n:A:B)                               | MATCH (n)--(:A&!B)                          |
      | MATCH (n:A:B)                               | MATCH (n)-[:!R&!S]-()                       |
      | MATCH (n:A:B)                               | MATCH (n)--(IS A)                           |
      | MATCH (n:A:B)                               | MATCH (n)-[IS R]-()                         |
      | MATCH (n:A)--{,5}(:B)                       | MATCH (n)-[*0..5]-(:C)                      |
      | MATCH p = shortestPath((n:A)-[:R*]-(m:!A))  | MATCH q = SHORTEST 1 (n)-[:!S]-+(:C)        |
      | MATCH p = shortestPath((n:A)-[:R*]-(m:!A))  | MATCH REPEATABLE ELEMENTS (n)-[:!S]-+(m:C)  |

  Scenario Outline: Conflicting syntax in separate statements in same EXISTS sub-query - syntax error
    When executing query:
      """
      RETURN EXISTS {
        <statement1>
        <statement2>
      }
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | statement1                                  | statement2                                  |
      | MATCH (n:A:B)                               | MATCH (n)--(:A&!B)                          |
      | MATCH (n:A:B)                               | MATCH (n)-[:!R&!S]-()                       |
      | MATCH (n:A:B)                               | MATCH (n)--(IS A)                           |
      | MATCH (n:A:B)                               | MATCH (n)-[IS R]-()                         |
      | MATCH (n:A)--{,5}(:B)                       | MATCH (n)-[*0..5]-(:C)                      |
      | MATCH p = shortestPath((n:A)-[:R*]-(m:!A))  | MATCH q = SHORTEST 1 (n)-[:!S]-+(:C)        |
      | MATCH p = shortestPath((n:A)-[:R*]-(m:!A))  | MATCH REPEATABLE ELEMENTS (n)-[:!S]-+(m:C)  |

  Scenario Outline: Conflicting syntax in separate statements within a CALL subquery - syntax error
    When executing query:
      """
      CALL {
        <statement1>
        <statement2>
        RETURN *
      }
      RETURN *
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | statement1                                  | statement2                                  |
      | MATCH (n:A:B)                               | MATCH (n)--(:A&!B)                          |
      | MATCH (n:A:B)                               | MATCH (n)-[:!R&!S]-()                       |
      | MATCH (n:A:B)                               | MATCH (n)--(IS A)                           |
      | MATCH (n:A:B)                               | MATCH (n)-[IS R]-()                         |
      | CREATE (n:A:B)                              | CREATE (m:C&D)                              |
      | MERGE (n:A:B)                               | CREATE (m IS C)                             |
      | MATCH (n:A)--{,5}(:B)                       | MATCH (n)-[*0..5]-(:C)                      |
      | MATCH p = shortestPath((n:A)-[:R*]-(m:!A))  | MATCH q = SHORTEST 1 (n)-[:!S]-+(:C)        |
      | MATCH p = shortestPath((n:A)-[:R*]-(m:!A))  | MATCH REPEATABLE ELEMENTS (n)-[:!S]-+(m:C)  |
      | CREATE (n:A&B)                              | SET n:C:D                                   |
      | MERGE (n IS A&B)                            | CREATE (m:C:D)                              |
      | MATCH (n:A&B)                               | MERGE (n:B:C)                               |
      | MATCH (n IS A)                              | MERGE (n:B:C)                               |
      | MATCH (:A)-[:!R]->(n)                       | MERGE (n:B:C)                               |

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
      | graphPattern                                         |
      | (n:A\|B:C)                                           |
      | (n:A:B:C)-->(m:(A&B)\|C)                             |
      | (n:A:B)--(:C), (n)-->(m:(A&B)\|C)                    |
      | (n:A:B)-[]-(m) WHERE m:(A&B)\|C                      |
      | (n:A:B)-[]-(m) WHERE NOT EXISTS { (m:(A&B)\|C) }     |

  Scenario Outline: Mixing : conjunction of label expression with IS introducer of label or type expression - syntax error
    When executing query:
      """
      <statement>
      RETURN *
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | statement                                                               |
      | MATCH (m)-->+(n IS S:R)                                                 |
      | MATCH (m)-[IS Q]->+(n:S:R)                                              |
      | MATCH (m)-[:Q]->+(n:S:R), (m)-[IS T]-+(n)                               |
      | MATCH (m)-[:Q]->+(n:S:R), (m)--+(IS T)                                  |
      | MATCH (:P) ((n)-[:Q]->(:R) WHERE EXISTS { (n)-->+(IS S) })+ (:S:R)      |
      | MATCH (:P) ((n)-[:Q]->(:S:R) WHERE EXISTS { (n)-->+(IS S) })+ (:S)      |
      | MATCH (:P) ((n)-[:Q]->(IS S) WHERE EXISTS { (n)-->+(:S:T) })+ (:S)      |
      | MATCH (:P) ((n)-[:Q]->(:S) WHERE EXISTS { (n)-->+(:S:T) })+ (IS S)      |
      | MATCH (:P) ((n)-[IS Q]->(:S) WHERE EXISTS { (n)-->+(:S:T) })+ (:S)      |
      | MATCH (:P) ((n)-[:Q]->(:R) WHERE EXISTS { (n)-[IS S]->+(:T) })+ (:S:R)  |
      | MATCH (:P) ((n)-[:Q]->(:S:R) WHERE EXISTS { (n)-[IS S]->+(:T) })+ (:S)  |

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
      | statement                                                                                           |
      | MATCH p = ANY SHORTEST shortestPath((:A)-[*..5]-(:B))                                               |
      | MATCH p = SHORTEST 2 shortestPath((:A)-[*..5]-(:B))                                                 |
      | MATCH p = ALL SHORTEST shortestPath((:A)-[*..5]-(:B))                                               |
      | MATCH p = SHORTEST GROUP shortestPath((:A)-[*..5]-(:B))                                             |
      | MATCH p = SHORTEST 2 GROUPS shortestPath((:A)-[*..5]-(:B))                                          |
      | MATCH p = ANY SHORTEST allShortestPaths((:A)-[*..5]-(:B))                                           |
      | MATCH p = SHORTEST 2 allShortestPaths((:A)-[*..5]-(:B))                                             |
      | MATCH p = ALL SHORTEST allShortestPaths((:A)-[*..5]-(:B))                                           |
      | MATCH p = SHORTEST GROUP allShortestPaths((:A)-[*..5]-(:B))                                         |
      | MATCH p = SHORTEST 2 GROUPS allShortestPaths((:A)-[*..5]-(:B))                                      |
      | MATCH p = SHORTEST 2 GROUPS allShortestPaths((:A)-[*..5]-(:B))                                      |
      | WITH EXISTS { ALL SHORTEST allShortestPaths((:A)-[*..5]-(:B)) } AS x                                |
      | WITH COUNT { ALL SHORTEST allShortestPaths((:A)-[r*]->(:B)) } AS x                                  |
      | WITH COLLECT { MATCH p = ALL SHORTEST allShortestPaths((:A)-[*]->(:B)) RETURN p } AS x              |

  Scenario Outline: Applying shortestPath to QPP - syntax error
    When executing query:
      """
      MATCH <pattern>
      RETURN *
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | pattern                                        |
      | p = shortestPath((n)-[]->+({s: 1}))            |
      | p = allShortestPaths((n)-[]->+({s: 1}))        |
      | p = shortestPath( ((:A)-[:R]->())+ )           |
      | p = allShortestPaths( ((:A)-[:R]->())+ )       |

  Scenario Outline: Explicit match mode with shortestPath - syntax error
    When executing query:
      """
      MATCH <matchMode> p = <pathFunction>(()-[*]->())
      RETURN p
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | matchMode               | pathFunction      |
      | REPEATABLE ELEMENTS     | shortestPath      |
      | REPEATABLE ELEMENTS     | allShortestPaths  |
      | DIFFERENT RELATIONSHIPS | shortestPath      |
      | DIFFERENT RELATIONSHIPS | allShortestPaths  |
      | DIFFERENT NODES         | shortestPath      |
      | DIFFERENT NODES         | allShortestPaths  |

  Scenario Outline: Mixing : and & in same DML statement - syntax error
    When executing query:
      """
      <statement>
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | statement                  |
      | CREATE (n:A:B&C)           |
      | CREATE (:A:B)-[:R]->(:T&S) |
      | MERGE (n:A:B&C)            |
      | MERGE (:A:B)-[:R]->(:T&S)  |

  Scenario Outline: Mixing : conjunction and IS in same label specification of same CREATE or MERGE - syntax error
    When executing query:
      """
      <statement>
      """
    Then a SyntaxError should be raised at compile time: *
      Examples:
        | statement                     |
        | CREATE (IS A:B:C)             |
        | CREATE (:A:B:C), (IS A&B)     |
        | CREATE (:A:B)-[:R]->(IS T:S)  |
        | CREATE (:A:B)-[IS R]->(:T:S)  |
        | MERGE (IS A:B:C)              |
        | MERGE (:A:B)-[:R]->(IS T:S)   |
        | MERGE (:A:B)-[IS R]->(:T:S)   |

  Scenario Outline: Mixing : conjunction and IS in same label specification of same SET or REMOVE - syntax error
    When executing query:
      """
      MATCH (n)
      <statement>
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | statement                     |
      | SET n IS A:B:C                |
      | SET n:A:B:C, n IS A           |
      | REMOVE n IS A:B:C             |
      | REMOVE n:A:B:C, n IS A        |

