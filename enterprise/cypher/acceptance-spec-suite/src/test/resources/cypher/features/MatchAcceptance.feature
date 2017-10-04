#
# Copyright (c) 2002-2017 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Neo4j is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

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
    Then the result should be empty
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

