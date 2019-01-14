#
# Copyright (c) 2002-2019 "Neo4j,"
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

#encoding: utf-8

Feature: MatchAcceptance

  Scenario: Filter on path nodes
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {foo: 'bar'})-[:REL]->(b:B {foo: 'bar'})-[:REL]->(c:C {foo: 'bar'})-[:REL]->(d:D {foo: 'bar'})
      """
    When executing query:
      """
      MATCH p = (pA)-[:REL*3..3]->(pB)
      WHERE all(i IN nodes(p) WHERE i.foo = 'bar')
      RETURN pB
      """
    Then the result should be:
      | pB                |
      | (:D {foo: 'bar'}) |
    And no side effects

  Scenario: Filter with AND/OR
    Given an empty graph
    And having executed:
      """
      CREATE (:X   {foo: 1}),
             (:Y   {foo: 2}),
             (:Y   {id: 42, foo: 3}),
             (:Y:X {id: 42, foo: 4})
      """
    When executing query:
      """
      MATCH (n)
      WHERE n:X OR (n:Y AND n.id = 42)
      RETURN n.foo ORDER BY n.foo
      """
    Then the result should be:
      | n.foo |
      | 1     |
      | 3     |
      | 4     |
    And no side effects

  Scenario: Should allow AND and OR with equality predicates
    Given an empty graph
    And having executed:
      """
      UNWIND range(1, 100) AS x CREATE (:User {prop1: x, prop2: x})
      """
    When executing query:
      """
      MATCH (c:User)
      WHERE ((c.prop1 = 1 AND c.prop2 = 1)
      OR (c.prop1 = 11 AND c.prop2 = 11))
      RETURN c
      """
    Then the result should be:
      | c                               |
      | (:User {prop1: 1, prop2: 1})    |
      | (:User {prop1: 11, prop2: 11})  |
    And no side effects

  Scenario: Should allow AND and OR with inequality predicates
    Given an empty graph
    And having executed:
      """
      UNWIND range(1, 100) AS x CREATE (:User {prop1: x, prop2: x})
      """
    When executing query:
      """
      MATCH (c:User)
      WHERE ((c.prop1 >= 1 AND c.prop2 < 2)
      OR (c.prop1 > 10 AND c.prop2 <= 11))
      RETURN c
      """
    Then the result should be:
      | c                               |
      | (:User {prop1: 1, prop2: 1})    |
      | (:User {prop1: 11, prop2: 11})  |
    And no side effects

  Scenario: Should allow AND and OR with STARTS WITH predicates
    Given an empty graph
    And having executed:
      """
      UNWIND range(1, 100) AS x CREATE (:User {prop1: x+'_val', prop2: x+'_val'})
      """
    When executing query:
      """
      MATCH (c:User)
      WHERE ((c.prop1 STARTS WITH '1_' AND c.prop2 STARTS WITH '1_')
      OR (c.prop1 STARTS WITH '11_' AND c.prop2 STARTS WITH '11_'))
      RETURN c
      """
    Then the result should be:
      | c                                           |
      | (:User {prop1: '1_val', prop2: '1_val'})    |
      | (:User {prop1: '11_val', prop2: '11_val'})  |
    And no side effects

  Scenario: Should allow AND and OR with regex predicates
    Given an empty graph
    And having executed:
      """
      UNWIND range(1, 100) AS x CREATE (:User {prop1: x+'_val', prop2: x+'_val'})
      """
    When executing query:
      """
      MATCH (c:User)
      WHERE ((c.prop1 =~ '1_.*' AND c.prop2 =~ '1_.*')
      OR (c.prop1 =~ '11_.*' AND c.prop2 =~ '11_.*'))
      RETURN c
      """
    Then the result should be:
      | c                                           |
      | (:User {prop1: '1_val', prop2: '1_val'})    |
      | (:User {prop1: '11_val', prop2: '11_val'})  |
    And no side effects

  Scenario: Should allow OR with regex predicates
    Given an empty graph
    And having executed:
      """
      UNWIND range(1, 100) AS x CREATE (u:User {prop: x+'_val'})
      """
    When executing query:
      """
      MATCH (c:User)
      WHERE c.prop =~ '1_.*' OR c.prop =~ '11_.*'
      RETURN c
      """
    Then the result should be:
      | c                         |
      | (:User {prop: '1_val'})   |
      | (:User {prop: '11_val'})  |
    And no side effects

  Scenario: difficult to plan query number 1
    Given an empty graph
    And having executed:
      """
      CREATE (:A {foo: 42})-[:T]->(),
             (:C {bar: 42}),
             (:C {bar: 665})
      """
    When executing query:
      """
      MATCH (a:A)
      WITH a WHERE true
      MATCH (c:C), (a)-->()
      WHERE a.foo = c.bar
      RETURN a.foo
      """
    Then the result should be:
      | a.foo |
      | 42    |
    And no side effects

  Scenario: difficult to plan query number 2
    Given an empty graph
    When executing query:
      """
      MATCH (ts)
      MATCH (k)-[:M]->(sta)
      OPTIONAL MATCH (sta)<-[:N]-(p)
      WITH k, ts, coalesce(p, sta) AS ab
      MATCH (d:A) WHERE d.Id = ab.OtherId
      MATCH (ts)-[:R]->(f)
      RETURN k, ts, f, d
      """
    Then the result should be:
      | k | ts | f | d |
    And no side effects

  Scenario: difficult to plan query number 3
    Given an empty graph
    And having executed:
    """
    CREATE (:A {foo: 42})-[:T]->(),
           (:C {bar: 42, baz: 'apa'}),
           (:C {bar: 665})
    """
    When executing query:
    """
    MATCH (a1)-[r]->(b1)
    WITH r WHERE true
    MATCH (a2)-[r]->(b2), (c)
    WHERE a2.foo = c.bar
    RETURN c.baz
    """
    Then the result should be:
      | c.baz |
      | 'apa' |
    And no side effects

  Scenario: Match on multiple labels
    Given an empty graph
    And having executed:
      """
      CREATE (:A:B), (:A:C), (:B:C)
      """
    When executing query:
      """
      MATCH (a)
      WHERE a:A:B
      RETURN a
      """
    Then the result should be:
      | a      |
      | (:A:B) |
    And no side effects

  Scenario: Match on multiple labels with OR
    Given an empty graph
    And having executed:
      """
      CREATE (:A:B), (:A:C), (:B:C)
      """
    When executing query:
      """
      MATCH (a)
      WHERE (a:A:B OR a:A:C)
      RETURN a
      """
    Then the result should be:
      | a      |
      | (:A:B) |
      | (:A:C) |
    And no side effects

  Scenario: Handle filtering with empty properties map
    Given an empty graph
    And having executed:
      """
      CREATE ({foo: 1})-[:R {bar: 1}]->({foo: 2}),
             ({foo: 3})-[:R {bar: 2}]->({foo: 4}),
             ({foo: 5})-[:R {bar: 3}]->({foo: 6})
      """
    When executing query:
      """
      MATCH (a { })-[r:R { }]->(b { }) WHERE a.foo = 3 AND b.foo = 4
      RETURN r.bar
      """
    Then the result should be:
      | r.bar |
      | 2     |
    And no side effects

  Scenario: Variable length path with both sides already bound
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {id: 1})-[:T]->(b:B {id: 2})
      CREATE (a)-[:S]->(b)
      CREATE (a)-[:T]->(:B)
      CREATE (a)-[:T]->()-[:T]->(:B)
      """
    When executing query:
      """
      MATCH (a:A {id: 1})-[:S]->(b:B {id: 2}), (a)-[:T*1..2]->(b)
      RETURN DISTINCT a.id, b.id
      """
    Then the result should be:
      | a.id | b.id |
      | 1    | 2    |
    And no side effects

  Scenario: Should handle EXISTS on node property when node is null
    Given an empty graph
    And having executed:
      """
      CREATE (:TestNode)
      """
    When executing query:
      """
      MATCH (t:TestNode)
      OPTIONAL MATCH (t)-[:LINKED]-(a:AnotherNode)
      RETURN t, a.Status as s, exists(a.Status) as e
      """
    Then the result should be:
      | t           | s    | e    |
      | (:TestNode) | null | null |
    And no side effects

  Scenario: Should handle NOT EXISTS on node property when node is null
    Given an empty graph
    And having executed:
        """
        CREATE (:TestNode)
        """
    When executing query:
        """
        MATCH (t:TestNode)
        OPTIONAL MATCH (t)-[:LINKED]-(a:AnotherNode)
        RETURN t, SUM(CASE WHEN NOT EXISTS(a.Status) OR COALESCE(a.Status, 'Complete') <> 'Complete' THEN 1 ELSE 0 END) AS PendingCount
        """
    Then the result should be:
      | t           | PendingCount |
      | (:TestNode) | 0            |
    And no side effects

  Scenario: Should handle simple IS NOT NULL on node property when node is null
    Given an empty graph
    And having executed:
      """
      CREATE (:LBL1)
      """
    When executing query:
      """
      OPTIONAL MATCH (o:LBL1)-[]->(p)
      WITH p
      OPTIONAL MATCH (p)
      WHERE p.prop2 IS NOT NULL
      RETURN p
      """
    Then the result should be:
      | p    |
      | null |
    And no side effects

  Scenario: Should handle complex IS NOT NULL on node property when node is null
    Given an empty graph
    And having executed:
      """
      CREATE (:LBL1 {prop0:'foo'})-[:rel]->(:LBL1 {prop1:'bar'})
      """
    When executing query:
      """
      MATCH (n:LBL1 {prop0:'foo'})-[]->(o:LBL1) WHERE EXISTS(o.prop1)
      WITH o
      OPTIONAL MATCH (o)-[:rel1]->(p:LBL2)
      WITH p
      OPTIONAL MATCH (p)-[:rel2]->(e:LBL3)
      WHERE p.prop2 IS NOT NULL
      RETURN p
      """
    Then the result should be:
      | p    |
      | null |
    And no side effects

  Scenario: equality with boolean lists
    Given an empty graph
    And having executed:
      """
      CREATE ({prop: [false]})
      """
    When executing query:
      """
      MATCH (n {prop: false}) RETURN n
      """
    Then the result should be:
      | n |
    And no side effects

  Scenario: loops with relationship type
    Given an empty graph
    And having executed:
      """
      CREATE (x:V)-[:R]->(x)
      """
    When executing query:
      """
      MATCH (x:V)-[:NON_EXISTENT]->(x) RETURN x
      """
    Then the result should be:
      | x |
    And no side effects
