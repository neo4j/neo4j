#
# Copyright (c) 2002-2018 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j Enterprise Edition. The included source
# code can be redistributed and/or modified under the terms of the
# GNU AFFERO GENERAL PUBLIC LICENSE Version 3
# (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
# Commons Clause, as found in the associated LICENSE.txt file.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# Neo4j object code can be licensed independently from the source
# under separate terms from the AGPL. Inquiries can be directed to:
# licensing@neo4j.com
#
# More information is also available at:
# https://neo4j.com/licensing/
#

Feature: PatternExpressionAcceptance

  Scenario: Returning an `extract()` expression
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
      RETURN extract(x IN (n)-->() | head(nodes(x))) AS p
      """
    Then the result should be:
      | p            |
      | [(:A), (:A)] |
      | []           |
      | []           |
    And no side effects

  Scenario: Using an `extract()` expression in a WITH
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)
      CREATE (a)-[:T]->(:B),
             (a)-[:T]->(:C)
      """
    When executing query:
      """
      MATCH (n:A)
      WITH extract(x IN (n)-->() | head(nodes(x))) AS p, count(n) AS c
      RETURN p, c
      """
    Then the result should be:
      | p            | c |
      | [(:A), (:A)] | 1 |
    And no side effects

  Scenario: Using an `extract()` expression in a WHERE
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
      WHERE n IN extract(x IN (n)-->() | head(nodes(x)))
      RETURN n
      """
    Then the result should be:
      | n    |
      | (:A) |
    And no side effects

  Scenario: Using a pattern expression and a CASE expression in a WHERE
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)
      CREATE (a)-[:T]->(:C),
             (a)-[:T]->(:C),
             (:B)-[:T]->(:D),
             ()-[:T]->()
      """
    When executing query:
      """
      MATCH (n)
      WHERE (n)-->() AND (CASE
                            WHEN n:A THEN length((n)-->(:C))
                            WHEN n:B THEN length((n)-->(:D))
                            ELSE 42
                          END) > 1
      RETURN n
      """
    Then the result should be:
      | n    |
      | (:A) |
      | ()   |
    And no side effects

  Scenario: Pattern expressions and ORDER BY
    Given an empty graph
    And having executed:
      """
      CREATE (a {time: 10}), (b {time: 20})
      CREATE (a)-[:T]->(b)
      """
    When executing query:
      """
      MATCH (liker)
      RETURN (liker)--() AS isNew
        ORDER BY liker.time
      """
    Then the result should be:
      | isNew                               |
      | [<({time: 10})-[:T]->({time: 20})>] |
      | [<({time: 20})<-[:T]-({time: 10})>] |
    And no side effects

  Scenario: Returning a pattern expression
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
      RETURN (n)-->() AS p
      """
    Then the result should be:
      | p                                      |
      | [<(:A)-[:T]->(:C)>, <(:A)-[:T]->(:B)>] |
      | []                                     |
      | []                                     |
    And no side effects

  Scenario: Returning a pattern expression with label predicate
    Given an empty graph
    And having executed:
      """
      CREATE (a:A), (b:B), (c:C), (d:D)
      CREATE (a)-[:T]->(b),
             (a)-[:T]->(c),
             (a)-[:T]->(d)
      """
    When executing query:
      """
      MATCH (n:A)
      RETURN (n)-->(:B)
      """
    Then the result should be:
      | (n)-->(:B)          |
      | [<(:A)-[:T]->(:B)>] |
    And no side effects

  Scenario: Returning a pattern expression with bound nodes
    Given an empty graph
    And having executed:
      """
      CREATE (a:A), (b:B)
      CREATE (a)-[:T]->(b)
      """
    When executing query:
      """
      MATCH (a:A), (b:B)
      RETURN (a)-[*]->(b) AS path
      """
    Then the result should be:
      | path                |
      | [<(:A)-[:T]->(:B)>] |
    And no side effects

  Scenario: Using a pattern expression in a WITH
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)
      CREATE (a)-[:T]->(:B),
             (a)-[:T]->(:C)
      """
    When executing query:
      """
      MATCH (n)-->(b)
      WITH (n)-->() AS p, count(b) AS c
      RETURN p, c
      """
    Then the result should be:
      | p                                      | c |
      | [<(:A)-[:T]->(:C)>, <(:A)-[:T]->(:B)>] | 2 |
    And no side effects

  Scenario: Using a variable-length pattern expression in a WITH
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:T]->(:B)
      """
    When executing query:
      """
      MATCH (a:A), (b:B)
      WITH (a)-[*]->(b) AS path, count(a) AS c
      RETURN path, c
      """
    Then the result should be:
      | path                | c |
      | [<(:A)-[:T]->(:B)>] | 1 |
    And no side effects

  Scenario: Using pattern expression in RETURN
    Given an empty graph
    And having executed:
      """
      CREATE (a:A), (:A), (:A)
      CREATE (a)-[:HAS]->()
      """
    When executing query:
      """
      MATCH (n:A)
      RETURN (n)-[:HAS]->() AS p
      """
    Then the result should be:
      | p                   |
      | [<(:A)-[:HAS]->()>] |
      | []                  |
      | []                  |
    And no side effects

  Scenario: Aggregating on pattern expression
    Given an empty graph
    And having executed:
      """
      CREATE (a:A), (:A), (:A)
      CREATE (a)-[:HAS]->()
      """
    When executing query:
      """
      MATCH (n:A)
      RETURN count((n)-[:HAS]->()) AS c
      """
    Then the result should be:
      | c |
      | 3 |
    And no side effects

  Scenario: Using `length()` on outgoing pattern expression
    Given an empty graph
    And having executed:
      """
      CREATE (n1:X {n: 1}), (n2:X {n: 2})
      CREATE (n1)-[:T]->(),
             (n1)-[:T]->(),
             (n1)-[:T]->(),
             ()-[:T]->(n2),
             ()-[:T]->(n2),
             ()-[:T]->(n2)
      """
    When executing query:
      """
      MATCH (n:X)
      WHERE length((n)-->()) > 2
      RETURN n
      """
    Then the result should be:
      | n           |
      | (:X {n: 1}) |
    And no side effects

  Scenario: Using `length()` on incoming pattern expression
    Given an empty graph
    And having executed:
      """
      CREATE (n1:X {n: 1}), (n2:X {n: 2})
      CREATE (n1)-[:T]->(),
             (n1)-[:T]->(),
             (n1)-[:T]->(),
             ()-[:T]->(n2),
             ()-[:T]->(n2),
             ()-[:T]->(n2)
      """
    When executing query:
      """
      MATCH (n:X)
      WHERE length((n)<--()) > 2
      RETURN n
      """
    Then the result should be:
      | n           |
      | (:X {n: 2}) |
    And no side effects

  Scenario: Using `length()` on undirected pattern expression
    Given an empty graph
    And having executed:
      """
      CREATE (n1:X {n: 1}), (n2:X {n: 2})
      CREATE (n1)-[:T]->(),
             (n1)-[:T]->(),
             (n1)-[:T]->(),
             ()-[:T]->(n2),
             ()-[:T]->(n2),
             ()-[:T]->(n2)
      """
    When executing query:
      """
      MATCH (n:X)
      WHERE length((n)--()) > 2
      RETURN n
      """
    Then the result should be:
      | n           |
      | (:X {n: 1}) |
      | (:X {n: 2}) |
    And no side effects

  Scenario: Using `length()` on pattern expression with complex relationship predicate
    Given an empty graph
    And having executed:
      """
      CREATE (n1:X {n: 1}), (n2:X {n: 2})
      CREATE (n1)-[:T]->(),
             (n1)-[:T]->(),
             (n1)-[:T]->(),
             ()-[:T]->(n2),
             ()-[:T]->(n2),
             ()-[:T]->(n2)
      """
    When executing query:
      """
      MATCH (n)
      WHERE length((n)-[:X|Y]-()) > 2
      RETURN n
      """
    Then the result should be:
      | n |
    And no side effects

  Scenario: Returning pattern expression in `exists()`
    Given an empty graph
    And having executed:
      """
      CREATE (a:X {prop: 42}), (:X {prop: 43})
      CREATE (a)-[:T]->()
      """
    When executing query:
      """
      MATCH (n:X)
      RETURN n, exists((n)--()) AS b
      """
    Then the result should be:
      | n               | b     |
      | (:X {prop: 42}) | true  |
      | (:X {prop: 43}) | false |
    And no side effects

  Scenario: Pattern expression inside list comprehension
    Given an empty graph
    And having executed:
      """
      CREATE (n1:X {n: 1}), (m1:Y), (i1:Y), (i2:Y)
      CREATE (n1)-[:T]->(m1),
             (m1)-[:T]->(i1),
             (m1)-[:T]->(i2)
      CREATE (n2:X {n: 2}), (m2), (i3:L), (i4:Y)
      CREATE (n2)-[:T]->(m2),
             (m2)-[:T]->(i3),
             (m2)-[:T]->(i4)
      """
    When executing query:
      """
      MATCH p = (n:X)-->(b)
      RETURN n, [x IN nodes(p) | length((x)-->(:Y))] AS list
      """
    Then the result should be:
      | n           | list   |
      | (:X {n: 1}) | [1, 2] |
      | (:X {n: 2}) | [0, 1] |
    And no side effects

  Scenario: Failing when introducing new node variable in pattern expression
    Given any graph
    When executing query:
      """
      MATCH (n)
      RETURN (n)-[:T]->(b)
      """
    Then a SyntaxError should be raised at compile time: UndefinedVariable

  Scenario: Failing when introducing new relationship variable in pattern expression
    Given any graph
    When executing query:
      """
      MATCH (n)
      RETURN (n)-[r:T]->()
      """
    Then a SyntaxError should be raised at compile time: UndefinedVariable

  Scenario: Get node degree via length of pattern expression
    Given an empty graph
    And having executed:
      """
      CREATE (x:X),
        (x)-[:T]->(),
        (x)-[:T]->(),
        (x)-[:T]->()
      """
    When executing query:
      """
      MATCH (a:X)
      RETURN length((a)-->()) AS length
      """
    Then the result should be:
      | length |
      | 3      |
    And no side effects

  Scenario: Get node degree via length of pattern expression that specifies a relationship type
    Given an empty graph
    And having executed:
      """
      CREATE (x:X),
        (x)-[:T]->(),
        (x)-[:T]->(),
        (x)-[:T]->(),
        (x)-[:OTHER]->()
      """
    When executing query:
      """
      MATCH (a:X)
      RETURN length((a)-[:T]->()) AS length
      """
    Then the result should be:
      | length |
      | 3      |
    And no side effects

  Scenario: Get node degree via length of pattern expression that specifies multiple relationship types
    Given an empty graph
    And having executed:
      """
      CREATE (x:X),
        (x)-[:T]->(),
        (x)-[:T]->(),
        (x)-[:T]->(),
        (x)-[:OTHER]->()
      """
    When executing query:
      """
      MATCH (a:X)
      RETURN length((a)-[:T|OTHER]->()) AS length
      """
    Then the result should be:
      | length |
      | 4      |
    And no side effects

