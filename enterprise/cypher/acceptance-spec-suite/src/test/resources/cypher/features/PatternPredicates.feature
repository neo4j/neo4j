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

Feature: PatternPredicates

  Background:
    Given an empty graph

  Scenario: Filter relationships with properties using pattern predicate
    And having executed:
      """
      CREATE (n1 {node: 1}), (n2 {node: 2}), (n3 {node: 3}), (n4 {node: 4})
      CREATE (n1)-[:X {rel: 1}]->(n2),
             (n3)-[:X {rel: 2}]->(n4)
      """
    When executing query:
      """
      MATCH (n)
      WHERE (n)-[{rel: 1}]->()
      RETURN n.node AS id
      """
    Then the result should be:
      | id |
      | 1  |
    And no side effects


  Scenario: Filter using negated pattern predicate
    And having executed:
      """
      CREATE (n1 {node: 1}), (n2 {node: 2}), (n3 {node: 3}), (n4 {node: 4})
      CREATE (n1)-[:X {rel: 1}]->(n2),
             (n3)-[:X {rel: 2}]->(n4)
      """
    When executing query:
      """
      MATCH (n)
      WHERE NOT (n)-[{rel: 1}]->()
      RETURN n.node AS id
      """
    Then the result should be:
      | id |
      | 2  |
      | 3  |
      | 4  |
    And no side effects

  Scenario: Filter using a variable length relationship pattern predicate with properties
    And having executed:
      """
      UNWIND [{node: 12, rel: 42}, {node: 324234, rel: 666}] AS props
      CREATE (start:Start {p: props.node}), (middle), (end)
      CREATE (start)-[:X {prop: props.rel}]->(middle),
             (middle)-[:X {prop: props.rel}]->(end)
      """
    When executing query:
      """
      MATCH (n:Start)
      WHERE (n)-[*2 {prop: 42}]->()
      RETURN n.p AS p
      """
    Then the result should be:
      | p  |
      | 12 |
    And no side effects

  Scenario: Filter using a pattern predicate that is a logical OR between an expression and a subquery
    And having executed:
      """
      UNWIND [{node: 33, rel: 42}, {node: 12, rel: 666}, {node: 55555, rel: 7777}] AS props
      CREATE (start:Start {p: props.node}), (middle), (end)
      CREATE (start)-[:X {prop: props.rel}]->(middle),
             (middle)-[:X {prop: props.rel}]->(end)
      """
    When executing query:
      """
      MATCH (n:Start)
      WHERE n.p = 12 OR (n)-[*2 {prop: 42}]->()
      RETURN n.p AS p
      """
    Then the result should be:
      | p  |
      | 33 |
      | 12 |
    And no side effects

  Scenario: Filter using a pattern predicate that is a logical OR between two expressions and a subquery
    And having executed:
      """
      UNWIND [{node: 33, rel: 42}, {node: 12, rel: 666}, {node: 25, rel: 444}, {node: 55555, rel: 7777}] AS props
      CREATE (start:Start {p: props.node}), (middle), (end)
      CREATE (start)-[:X {prop: props.rel}]->(middle),
             (middle)-[:X {prop: props.rel}]->(end)
      """
    When executing query:
      """
      MATCH (n:Start)
      WHERE n.p = 12 OR (n)-[*2 {prop: 42}]->() OR n.p = 25
      RETURN n.p AS p
      """
    Then the result should be:
      | p  |
      | 33 |
      | 12 |
      | 25 |
    And no side effects

  Scenario: Filter using a pattern predicate that is a logical OR between one expression and a negated subquery
    And having executed:
      """
      UNWIND [{node: 25, rel: 444}, {node: 12, rel: 42}, {node: 25, rel: 42}] AS props
      CREATE (start:Start {p: props.node}), (middle), (end)
      CREATE (start)-[:X {prop: props.rel}]->(middle),
             (middle)-[:X {prop: props.rel}]->(end)
      """
    When executing query:
      """
      MATCH (n:Start)
      WHERE n.p = 12 OR NOT (n)-[*2 {prop: 42}]->()
      RETURN n.p AS p
      """
    Then the result should be:
      | p  |
      | 25 |
      | 12 |
    And no side effects

  Scenario: Filter using a pattern predicate that is a logical OR between one subquery and a negated subquery
    And having executed:
      """
      CREATE (s1:Start {id: 1}), (e1 {prop: 42}),
             (s2:Start {id: 2}), (e2 {prop: 411}), (:Start {id: 3})
      CREATE (s1)-[:X]->(e1),
             (s2)-[:X]->(e2)
      """
    When executing query:
      """
      MATCH (n:Start)
      WHERE (n)-->({prop: 42}) OR NOT (n)-->()
      RETURN n.id AS id
      """
    Then the result should be:
      | id |
      | 1  |
      | 3  |
    And no side effects

  Scenario: Filter using a pattern predicate that is a logical OR between one negated subquery and a subquery
    And having executed:
      """
      CREATE (s1:Start {id: 1}), (e1 {prop: 42}),
             (s2:Start {id: 2}), (e2 {prop: 411}), (:Start {id: 3})
      CREATE (s1)-[:X]->(e1),
             (s2)-[:X]->(e2)
      """
    When executing query:
      """
      MATCH (n:Start)
      WHERE NOT (n)-->() OR (n)-->({prop: 42})
      RETURN n.id AS id
      """
    Then the result should be:
      | id |
      | 1  |
      | 3  |
    And no side effects

  Scenario: Filter using a pattern predicate that is a logical OR between two subqueries
    And having executed:
      """
      CREATE (s1:Start {id: 1}), (e1 {prop: 42}),
             (s2:Start {id: 2}), (e2 {prop: 411}), (:Start {id: 3})
      CREATE (s1)-[:X]->(e1),
             (s2)-[:X]->(e2)
      """
    When executing query:
      """
      MATCH (n:Start)
      WHERE (n)-->({prop: 42}) OR (n)-->({prop: 411})
      RETURN n.id AS id
      """
    Then the result should be:
      | id |
      | 1  |
      | 2  |
    And no side effects

  Scenario: Filter using a pattern predicate that is a logical OR between one negated subquery, a subquery, and an equality expression
    And having executed:
      """
      CREATE (s1:Start {id: 1}), (e1 {prop: 42}),
             (s2:Start {id: 2}), (e2 {prop: 411}), (:Start {id: 3, prop: 21})
      CREATE (s1)-[:X]->(e1),
             (s2)-[:X]->(e2)
      """
    When executing query:
      """
      MATCH (n:Start)
      WHERE n.prop = 21 OR NOT (n)-->() OR (n)-->({prop: 42})
      RETURN n.id AS id
      """
    Then the result should be:
      | id |
      | 1  |
      | 3  |
    And no side effects

  Scenario: Filter using a pattern predicate that is a logical OR between one negated subquery, two subqueries, and an equality expression
    And having executed:
      """
      CREATE (s1:Start {id: 1}), (e1 {prop: 42}),
             (s2:Start {id: 2}), (e2 {prop: 411}),
             (:Start {id: 3, prop: 21}),
             (s4:Start {id: 4}), (e4 {prop: 1})
      CREATE (s1)-[:X]->(e1),
             (s2)-[:X]->(e2),
             (s4)-[:X]->(e4)
      """
    When executing query:
      """
      MATCH (n:Start)
      WHERE n.prop = 21 OR NOT (n)-->() OR (n)-->({prop: 42}) OR (n)-->({prop: 1})
      RETURN n.id AS id
      """
    Then the result should be:
      | id |
      | 1  |
      | 3  |
      | 4  |
    And no side effects

  Scenario: Filter using a pattern predicate that is a logical OR between one negated subquery, two subqueries, and an equality expression 2
    And having executed:
      """
      CREATE (s1:Start {id: 1}), (e1 {prop: 42}),
             (s2:Start {id: 2}), (e2 {prop: 411}),
             (:Start {id: 3, prop: 21}),
             (s4:Start {id: 4}), (e4 {prop: 1})
      CREATE (s1)-[:X]->(e1),
             (s2)-[:X]->(e2),
             (s4)-[:X]->(e4)
      """
    When executing query:
      """
      MATCH (n:Start)
      WHERE n.prop = 21 OR (n)-->({prop: 42}) OR NOT (n)-->() OR (n)-->({prop: 1})
      RETURN n.id AS id
      """
    Then the result should be:
      | id |
      | 1  |
      | 3  |
      | 4  |
    And no side effects

  Scenario: Using a pattern predicate after aggregation 1
    And having executed:
      """
      CREATE (:A)-[:T]->(:B)
      """
    When executing query:
      """
      MATCH (owner)
      WITH owner, count(*) > 0 AS collected
      WHERE (owner)-->()
      RETURN owner
      """
    Then the result should be:
      | owner |
      | (:A)  |
    And no side effects

  Scenario: Using a pattern predicate after aggregation 2
    And having executed:
      """
      CREATE (:A)-[:T]->(:B)
      """
    When executing query:
      """
      MATCH (owner)
      WITH owner, count(*) AS collected
      WHERE (owner)-->()
      RETURN owner
      """
    Then the result should be:
      | owner |
      | (:A)  |
    And no side effects

  Scenario: Returning a relationship from a pattern predicate
    And having executed:
      """
      CREATE (:A)-[:T]->(:B)
      """
    When executing query:
      """
      MATCH ()-[r]->()
      WHERE ()-[r]-(:B)
      RETURN r
      """
    Then the result should be:
      | r    |
      | [:T] |
    And no side effects

  Scenario: Pattern predicate should uphold the relationship uniqueness constraint
    And having executed:
      """
      CREATE (a:Foo), (b:Bar {name: 'b'}), (c:Foo), (d:Foo), (e:Bar {name: 'e'}), (:Bar), (:Bar)
      CREATE (a)-[:HAS]->(b),
             (c)-[:HAS]->(b),
             (d)-[:HAS]->(e)
      """
    When executing query:
      """
      MATCH (a:Foo)
      OPTIONAL MATCH (a)--(b:Bar)
      WHERE (a)--(b:Bar)--()
      RETURN b
      """
    Then the result should be:
      | b                  |
      | (:Bar {name: 'b'}) |
      | (:Bar {name: 'b'}) |
      | null               |
    When executing query:
      """
      MATCH (a:Foo)
      OPTIONAL MATCH (a)--(b:Bar)
      WHERE NOT ((a)--(b:Bar)--())
      RETURN b
      """
    Then the result should be:
      | b                  |
      | (:Bar {name: 'e'}) |
      | null               |
      | null               |
    And no side effects

  Scenario: Using pattern predicate
    And having executed:
      """
      CREATE (a:A), (:A), (:A)
      CREATE (a)-[:HAS]->()
      """
    When executing query:
      """
      MATCH (n:A)
      WHERE (n)-[:HAS]->()
      RETURN n
      """
    Then the result should be:
      | n    |
      | (:A) |
    And no side effects

  Scenario: Matching using pattern predicate with multiple relationship types
    And having executed:
      """
      CREATE (a:A), (b:B), (x)
      CREATE (a)-[:T1]->(x),
             (b)-[:T2]->(x)
      """
    When executing query:
      """
      MATCH (a)
      WHERE (a)-[:T1|:T2]->()
      RETURN a
      """
    Then the result should be:
      | a    |
      | (:A) |
      | (:B) |
    And no side effects

  Scenario: Matching using pattern predicate
    And having executed:
      """
      CREATE (:A)-[:T]->(x)
      """
    When executing query:
      """
      MATCH (a)
      WHERE (a)-[:T]->()
      RETURN a
      """
    Then the result should be:
      | a    |
      | (:A) |
    And no side effects

  Scenario: Pattern predicates on missing optionally matched nodes should simply evaluate to false
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      MATCH (n)
      OPTIONAL MATCH (n)-->(x)
      WHERE (x)-->()
      RETURN x
      """
    Then the result should be:
      | x    |
      | null |
    And no side effects

  Scenario: Pattern predicates and parametrised predicate
    And having executed:
      """
      CREATE ({id: 0})-[:T]->({name: 'Neo'})
      """
    And parameters are:
      | id | 0 |
    When executing query:
      """
      MATCH (a)-->(b)
      WHERE (b)-->()
        AND a.id = {id}
      RETURN b
      """
    Then the result should be:
      | b |
    And no side effects

  Scenario: Matching with complex composite pattern predicate
    And having executed:
      """
      CREATE (a:Label {id: 0}), (b:Label {id: 1}), (c:Label {id: 2})
      CREATE (a)-[:T]->(b),
             (b)-[:T]->(c)
      """
    When executing query:
      """
      MATCH (a), (b)
      WHERE (a.id = 0 OR (a)-[:T]->(b:MissingLabel))
        AND ((a)-[:T]->(b:Label) OR (a)-[:T]->(b:MissingLabel))
      RETURN b
      """
    Then the result should be:
      | b                |
      | (:Label {id: 1}) |
    And no side effects

  Scenario: Handling pattern predicates without matches
    And having executed:
      """
      CREATE (s:Single), (a:A {prop: 42}),
             (b:B {prop: 46}), (c:C)
      CREATE (s)-[:REL]->(a),
             (s)-[:REL]->(b),
             (a)-[:REL]->(c),
             (b)-[:LOOP]->(b)
      """
    When executing query:
      """
      MATCH (a:A), (c:C)
      OPTIONAL MATCH (a)-->(b)
      WHERE (b)-->(c)
      RETURN b
      """
    Then the result should be:
      | b    |
      | null |
    And no side effects

  Scenario: Handling pattern predicates
    And having executed:
      """
      CREATE (s:Single), (a:A {prop: 42}),
             (b:B {prop: 46}), (c:C)
      CREATE (s)-[:REL]->(a),
             (s)-[:REL]->(b),
             (a)-[:REL]->(c),
             (b)-[:LOOP]->(b)
      """
    When executing query:
      """
      MATCH (a:Single), (c:C)
      OPTIONAL MATCH (a)-->(b)
      WHERE (b)-->(c)
      RETURN b
      """
    Then the result should be:
      | b               |
      | (:A {prop: 42}) |
    And no side effects

  Scenario: Matching named path with variable length pattern and pattern predicates
    And having executed:
      """
      CREATE (a:A), (b:B), (c:C), (d:D)
      CREATE (a)-[:X]->(b),
             (a)-[:X]->(c),
             (c)-[:X]->(d)
      """
    When executing query:
      """
      MATCH p = (:A)-[*]->(leaf)
      WHERE NOT((leaf)-->())
      RETURN p, leaf
      """
    Then the result should be:
      | p                            | leaf |
      | <(:A)-[:X]->(:B)>            | (:B) |
      | <(:A)-[:X]->(:C)-[:X]->(:D)> | (:D) |
    And no side effects

  Scenario: Undirected NOOP path predicate 1
    And having executed:
      """
      CREATE (a1:A)
      CREATE (a2:A)
      CREATE (a3:A)

      CREATE (b1:B)
      CREATE (b2:B)

      CREATE (a1)-[:R]->(b1)
      CREATE (a2)-[:R]->(a1)
      """
    When executing query:
      """
      MATCH (a:A)-[r]-(b:B) WHERE (b)-[r]-(a) RETURN a
      """
    Then the result should be:
      | a               |
      | (:A)            |
    And no side effects

  Scenario: Undirected NOOP path predicate 2
    And having executed:
      """
      CREATE (a1:A)
      CREATE (a2:A)
      CREATE (a3:A)

      CREATE (b1:B)
      CREATE (b2:B)

      CREATE (a1)-[:R]->(b1)
      CREATE (a2)-[:R]->(a1)
      """
    When executing query:
      """
      MATCH (a:A)-[r]-(b:B) WHERE (a)-[r]-(b) RETURN a
      """
    Then the result should be:
      | a               |
      | (:A)            |
    And no side effects

