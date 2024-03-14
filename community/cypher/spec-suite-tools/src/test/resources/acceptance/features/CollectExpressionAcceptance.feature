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

Feature: CollectExpressionAcceptance

  Background:
    Given an empty graph
    And having executed:
    """
    CREATE (a:Person {name: "Ada", nicknames: [], age: 27}),
           (b:Person {name: "Bob", nicknames: ["Robert"], age: 33}),
           (c:Person {name: "Carl", nicknames: ["Carlos", "Chaos Carl"], age: 22}),
           (d:Person {name: "Danielle", nicknames: ["Dani", "Elle"], age: 38}),
           (e:Person:Immortal {name: "Eve", nicknames: [], age: 1000}),
     (a)-[:FRIEND]->(b),
     (a)-[:FRIEND]->(c),
     (b)-[:FRIEND]->(a),
     (b)-[:FRIEND]->(d),
     (c)-[:FRIEND]->(d),
     (d)-[:FRIEND]->(c)
    """

  Scenario: COLLECT with simple MATCH
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN
        p.name AS name, COLLECT {
          MATCH (p)-[:FRIEND]->(f) RETURN f.name ORDER BY f.name
        } AS friends
      """
    Then the result should be, in any order:
      | name       | friends             |
      | 'Ada'      | ['Bob', 'Carl']     |
      | 'Bob'      | ['Ada', 'Danielle'] |
      | 'Carl'     | ['Danielle']        |
      | 'Danielle' | ['Carl']            |
      | 'Eve'      | []                  |
    And no side effects

  Scenario: COLLECT with unwind inside
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN
        p.name AS name, COLLECT {
          MATCH (p)-[:FRIEND]->(f)
          UNWIND f.nicknames AS nickname
          RETURN nickname ORDER BY nickname
        } AS friendNicknames
      """
    Then the result should be, in any order:
      | name       | friendNicknames                    |
      | 'Ada'      | ['Carlos', 'Chaos Carl', 'Robert'] |
      | 'Bob'      | ['Dani', 'Elle']                   |
      | 'Carl'     | ['Dani', 'Elle']                   |
      | 'Danielle' | ['Carlos', 'Chaos Carl']           |
      | 'Eve'      | []                                 |
    And no side effects

  Scenario: COLLECT in a WHERE
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      WHERE COLLECT { MATCH (p)-[:FRIEND]->(f) RETURN f.name ORDER BY f.name } = ['Bob', 'Carl']
      RETURN p.name AS name
      """
    Then the result should be, in any order:
      | name  |
      | 'Ada' |
    And no side effects

  Scenario: COLLECT with single bound node
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN COLLECT { MATCH (p) RETURN p.name ORDER BY p.name } AS names
      """
    Then the result should be, in any order:
      | names        |
      | ['Ada']      |
      | ['Bob']      |
      | ['Carl']     |
      | ['Danielle'] |
      | ['Eve']      |
    And no side effects

  Scenario: COLLECT with null RETURNs
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN
        p.name AS name, COLLECT {
          MATCH (p)-[:FRIEND]->(f) RETURN f.nonExistingProp
        } AS friends
      """
    Then the result should be, in any order:
      | name       | friends      |
      | 'Ada'      | [null, null] |
      | 'Bob'      | [null, null] |
      | 'Carl'     | [null]       |
      | 'Danielle' | [null]       |
      | 'Eve'      | []           |
    And no side effects

  Scenario: COLLECT with single unbound node
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN COLLECT { MATCH (q) RETURN q.name ORDER BY q.name } AS names
      """
    Then the result should be, in any order:
      | names                                  |
      | ['Ada', 'Bob','Carl','Danielle','Eve'] |
      | ['Ada', 'Bob','Carl','Danielle','Eve'] |
      | ['Ada', 'Bob','Carl','Danielle','Eve'] |
      | ['Ada', 'Bob','Carl','Danielle','Eve'] |
      | ['Ada', 'Bob','Carl','Danielle','Eve'] |
    And no side effects

  Scenario: COLLECT with reference to outer variable
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN p.name as name, COLLECT { MATCH (q) WHERE q.age > p.age RETURN q.name ORDER BY q.name } AS olderPeople
      """
    Then the result should be, in any order:
      | name       | olderPeople                       |
      | 'Ada'      | ['Bob', 'Danielle', 'Eve']        |
      | 'Bob'      | ['Danielle', 'Eve']               |
      | 'Carl'     | ['Ada', 'Bob', 'Danielle', 'Eve'] |
      | 'Danielle' | ['Eve']                           |
      | 'Eve'      | []                                |
    And no side effects

  Scenario: COLLECT with single node with inlined property map predicate
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN COLLECT { MATCH (p {age: 1000}) RETURN p.name } AS names
      """
    Then the result should be, in any order:
      | names   |
      | []      |
      | []      |
      | []      |
      | []      |
      | ['Eve'] |
    And no side effects

  Scenario: COLLECT with single unbound node with inlined property map predicate
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN COLLECT { MATCH (q {age: 1000}) RETURN q.name } AS names
      """
    Then the result should be, in any order:
      | names   |
      | ['Eve'] |
      | ['Eve'] |
      | ['Eve'] |
      | ['Eve'] |
      | ['Eve'] |
    And no side effects

  Scenario: COLLECT with single node with inlined predicate
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN COLLECT { MATCH (p WHERE p.age > 30) RETURN p.name } AS collected
      """
    Then the result should be, in any order:
      | collected    |
      | []           |
      | ['Bob']      |
      | []           |
      | ['Danielle'] |
      | ['Eve']      |
    And no side effects

  Scenario: COLLECT with single node with label predicate
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN COLLECT { MATCH (p:Immortal) RETURN p.name } AS names
      """
    Then the result should be, in any order:
      | names   |
      | []      |
      | []      |
      | []      |
      | []      |
      | ['Eve'] |
    And no side effects

  Scenario: COLLECT with single node with predicate
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN COLLECT { MATCH (p) WHERE p.age > 30 RETURN p.name } AS names
      """
    Then the result should be, in any order:
      | names        |
      | []           |
      | ['Bob']      |
      | []           |
      | ['Danielle'] |
      | ['Eve']      |
    And no side effects

  Scenario: Inner query with create should fail with syntax error
    Given an empty graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE COLLECT {
       CREATE (person)-[:HAS_DOG]->(:Dog)
      } = []
      RETURN person.name
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Inner query with set should fail with syntax error
    Given an empty graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE COLLECT {
       SET person.name = "Karen"
      } = []
      RETURN person.name
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Inner query with merge should fail with syntax error
    Given an empty graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE COLLECT {
        MATCH (person)
        MERGE (person)-[:HAS_DOG]->(Dog { name: "Pluto" })
      } = []
      RETURN person.name
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Inner query with delete should fail with syntax error
    Given an empty graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE COLLECT {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
        DETACH DELETE dog
      } = []
      RETURN person.name
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: COLLECT with where clause in a node
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      WHERE COLLECT { MATCH (p)-[r]->(f WHERE f.age > 30) RETURN f.name } = ['Danielle']
      RETURN p.name AS name
      """
    Then the result should be, in any order:
      | name   |
      | 'Bob'  |
      | 'Carl' |
    And no side effects

  Scenario: COLLECT with where clause outside the node
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      WHERE COLLECT { MATCH (p)-[r]->(f) WHERE f.age > 30 RETURN f.name } = ['Danielle']
      RETURN p.name AS name
      """
    Then the result should be, in any order:
      | name   |
      | 'Bob'  |
      | 'Carl' |
    And no side effects

  Scenario: COLLECT can be nested inside another COLLECT
    Given an empty graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE COLLECT {
         MATCH (person)-[:FRIEND]->(friend:Person)
         WHERE 'Carl' IN COLLECT {
           MATCH (friend)-[:FRIEND]->(friendOfAFriend:Person) RETURN friendOfAFriend.name
         }
         RETURN person.name
      } <> []
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name   |
      | 'Bob'  |
      | 'Carl' |
    And no side effects

  Scenario: COLLECT with a function should work
    Given an empty graph
    When executing query:
      """
      MATCH (person:Person)
      WITH COLLECT {
       MATCH (person)-[:FRIEND]->(friend:Person)
       WHERE reverse(friend.name) = "adA"
       RETURN person.name
      } as foo
      RETURN foo
      """
    Then the result should be, in any order:
      | foo     |
      | []      |
      | ['Bob'] |
      | []      |
      | []      |
      | []      |
    And no side effects

  Scenario: COLLECT with a union made of RETURNs
    Given an empty graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE COLLECT {
          RETURN 1 as a
          UNION ALL
          RETURN 1 as a
      } = [1, 1]
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name       |
      | 'Ada'      |
      | 'Bob'      |
      | 'Carl'     |
      | 'Danielle' |
      | 'Eve'      |
    And no side effects

  Scenario: COLLECT with a RETURNING case
    Given an empty graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE COLLECT {
          RETURN CASE
             WHEN true THEN 1
             ELSE 2
          END
      } = [1]
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name       |
      | 'Ada'      |
      | 'Bob'      |
      | 'Carl'     |
      | 'Danielle' |
      | 'Eve'      |
    And no side effects

  Scenario: COLLECT should work in a return statement
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN COLLECT { MATCH (p)-[:FRIEND]->(q) RETURN q.name ORDER BY q.name } AS friends,
             p.name                       AS name
      """
    Then the result should be, in any order:
      | friends             | name       |
      | ['Bob', 'Carl']     | 'Ada'      |
      | ['Ada', 'Danielle'] | 'Bob'      |
      | ['Danielle']        | 'Carl'     |
      | ['Carl']            | 'Danielle' |
      | []                  | 'Eve'      |
    And no side effects

  Scenario: COLLECT works in conjunction with list operators (+)
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN COLLECT { MATCH (p)-[:FRIEND]->(q) RETURN q.name ORDER BY q.name } + ['Fred'] AS friends,
             p.name AS name
      """
    Then the result should be, in any order:
      | friends                     | name       |
      | ['Bob', 'Carl', 'Fred']     | 'Ada'      |
      | ['Ada', 'Danielle', 'Fred'] | 'Bob'      |
      | ['Danielle', 'Fred']        | 'Carl'     |
      | ['Carl', 'Fred']            | 'Danielle' |
      | ['Fred']                    | 'Eve'      |
    And no side effects

  Scenario: COLLECT works in conjunction with list operators (IN)
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN 'Danielle' IN COLLECT { MATCH (p)-[:FRIEND]->(q) RETURN q.name ORDER BY q.name } AS friendsWithDanielle,
             p.name AS name
      """
    Then the result should be, in any order:
      | friendsWithDanielle | name       |
      | false               | 'Ada'      |
      | true                | 'Bob'      |
      | true                | 'Carl'     |
      | false               | 'Danielle' |
      | false               | 'Eve'      |
    And no side effects

  Scenario: COLLECT works in conjunction with list operators (Subscript operator [])
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      WHERE p.age < 50
      RETURN COLLECT { MATCH (p)-[:FRIEND]->(q) RETURN q.name ORDER BY q.name }[0] AS friend,
             p.name AS name
      """
    Then the result should be, in any order:
      | friend     | name       |
      | 'Bob'      | 'Ada'      |
      | 'Ada'      | 'Bob'      |
      | 'Danielle' | 'Carl'     |
      | 'Carl'     | 'Danielle' |
    And no side effects

  Scenario: COLLECT should work as grouping key
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN COLLECT { MATCH (p)-[:FRIEND]->(f) WHERE f.age > p.age RETURN f.name } AS olderFriends,
             avg(p.age) AS averageAge
      """
    Then the result should be, in any order:
      | olderFriends | averageAge |
      | ['Bob']      | 27.0       |
      | ['Danielle'] | 27.5       |
      | []           | 519.0      |
    And no side effects

  Scenario: COLLECT should work together with count aggregation
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN COLLECT { MATCH (p)-[:FRIEND]->(f) WHERE f.age > p.age RETURN f.name } AS olderFriends,
             count(p) AS numPersons
      """
    Then the result should be, in any order:
      | olderFriends | numPersons |
      | ['Bob']      | 1          |
      | ['Danielle'] | 2          |
      | []           | 2          |
    And no side effects

  Scenario: COLLECT should work in a WHEN statement
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN CASE WHEN COLLECT { MATCH (p:Person)-[:FRIEND]->(f) RETURN f.name } = ['Carl'] THEN p.name END AS result
      """
    Then the result should be, in any order:
      | result     |
      | null       |
      | null       |
      | null       |
      | 'Danielle' |
      | null       |
    And no side effects

  Scenario: COLLECT should work with a DISTINCT
    Given an empty graph
    When executing query:
      """
      RETURN COLLECT { MATCH (p:Person)-[:FRIEND]->(f) RETURN DISTINCT f.name ORDER BY f.name } AS friends
      """
    Then the result should be, in any order:
      | friends                            |
      | ['Ada', 'Bob', 'Carl', 'Danielle'] |
    And no side effects

  Scenario: COLLECT should work in a WITH statement
    Given an empty graph
    When executing query:
      """
      WITH COLLECT { MATCH (p:Person)-[:FRIEND]->(f) RETURN DISTINCT f.name ORDER BY f.name } AS friends
      RETURN friends
      """
    Then the result should be, in any order:
      | friends                            |
      | ['Ada', 'Bob', 'Carl', 'Danielle'] |
    And no side effects

  Scenario: COLLECT should work in a SET statement
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      SET p.friends = COLLECT { MATCH (p)-[:FRIEND]->(f) RETURN f.name ORDER BY f.name }
      RETURN p.name AS name, p.friends AS friends
      """
    Then the result should be, in any order:
      | name       | friends             |
      | 'Ada'      | ['Bob', 'Carl']     |
      | 'Bob'      | ['Ada', 'Danielle'] |
      | 'Carl'     | ['Danielle']        |
      | 'Danielle' | ['Carl']            |
      | 'Eve'      | []                  |
    And the side effects should be:
      | +properties | 5 |

  Scenario: COLLECT should return [] for NULL nodes
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH (n:DoesNotExist)
      RETURN [
        COLLECT { MATCH (n) RETURN n },
        COLLECT { MATCH (n)-->() RETURN n },
        COLLECT { MATCH (n)-->({prop: 1}) RETURN n }
      ] AS collects
      """
    Then the result should be, in any order:
      | collects     |
      | [[], [], []] |
    And no side effects

  Scenario: COLLECT should return [] for NULL relationships
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH ()-[r:DOES_NOT_EXIST]->()
      RETURN [
        COLLECT { MATCH ()-[r]-() RETURN r },
        COLLECT { MATCH ()-[r]-({prop: 1}) RETURN r }
      ] AS collects
      """
    Then the result should be, in any order:
      | collects |
      | [[], []] |
    And no side effects

  Scenario: COLLECT with multiple patterns in inner MATCH should work
    Given an empty graph
    When executing query:
      """
      MATCH (person:Person)
      RETURN person.name AS name, COLLECT {
       MATCH (person), (immortal:Immortal) WHERE immortal.nicknames = person.nicknames RETURN 1
      } AS sameNumNicknames
      """
    Then the result should be, in any order:
      | name       | sameNumNicknames  |
      | 'Ada'      | [1]               |
      | 'Bob'      | []                |
      | 'Carl'     | []                |
      | 'Danielle' | []                |
      | 'Eve'      | [1]               |
    And no side effects

  Scenario: COLLECT with multiple patterns with relationships in inner MATCH should work
    Given an empty graph
    When executing query:
      """
      MATCH (person:Person)
      RETURN person.name AS name, COLLECT {
       MATCH (person)-[:FRIEND]->(other:Person), (other)-[:FRIEND]->(person) RETURN true
      } AS mutualFriends
      """
    Then the result should be, in any order:
      | name       | mutualFriends |
      | 'Ada'      | [true]        |
      | 'Bob'      | [true]        |
      | 'Carl'     | [true]        |
      | 'Danielle' | [true]        |
      | 'Eve'      | []            |
    And no side effects

  Scenario: COLLECT with UNION should work
    Given an empty graph
    When executing query:
      """
      WITH COLLECT {
       MATCH (person:Person)-[:FRIEND]->(friend:Person)
       RETURN friend.name AS friended ORDER BY friended
       UNION
       MATCH (person:Person)-[:FRIEND]->(otherPerson:Person)
       RETURN otherPerson.name AS friended ORDER BY friended
      } as foo
      RETURN foo
      """
    Then the result should be (ignoring element order for lists):
      | foo                                |
      | ['Ada', 'Bob', 'Carl', 'Danielle'] |
    And no side effects

  Scenario: COLLECT with UNION that references outer variable should work
    Given an empty graph
    When executing query:
      """
      MATCH (person)
      WITH COLLECT {
       MATCH (person)-[:FRIEND]->(friend:Person)
       RETURN friend.name AS name ORDER BY name
       UNION
       MATCH (person)-[:FRIEND]->(otherPerson:Person)
       RETURN otherPerson.name AS name ORDER BY name
      } as foo
      RETURN foo
      """
    Then the result should be (ignoring element order for lists):
      | foo                 |
      | ['Bob', 'Carl']     |
      | ['Ada', 'Danielle'] |
      | ['Danielle']        |
      | ['Carl']            |
      | []                  |
    And no side effects

  Scenario: COLLECT with UNION ALL should work
    Given an empty graph
    When executing query:
      """
      MATCH (person)
      WITH COLLECT {
       MATCH (person)-[:FRIEND]->(friend:Person)
       RETURN friend.name AS friended ORDER BY friended
       UNION ALL
       MATCH (person)-[:FRIEND]->(otherPerson:Person)
       RETURN otherPerson.name AS friended ORDER BY friended
      } as foo
      RETURN foo
      """
    Then the result should be, in any order:
      | foo                                    |
      | ['Bob', 'Carl', 'Bob', 'Carl']         |
      | ['Ada', 'Danielle', 'Ada', 'Danielle'] |
      | ['Danielle', 'Danielle']               |
      | ['Carl', 'Carl']                       |
      | []                                     |
    And no side effects

  Scenario: COLLECT with CALL should work
    Given an empty graph
    When executing query:
      """
      MATCH (person:Person)
      WITH COLLECT {
        MATCH (person)-[:FRIEND]->(friend:Person)
        CALL {
          WITH person
          MATCH (person)-[:FRIEND]->(friend:Person)
          RETURN friend.name AS name
          UNION ALL
          WITH person
          MATCH (person)-[:FRIEND]->(otherPerson:Person)
          RETURN otherPerson.name AS name
        }
        RETURN DISTINCT friend.name AS friendNames ORDER BY friendNames
      } as foo
      RETURN foo
      """
    Then the result should be, in any order:
      | foo                 |
      | ['Bob', 'Carl']     |
      | ['Ada', 'Danielle'] |
      | ['Danielle']        |
      | ['Carl']            |
      | []                  |
    And no side effects

  Scenario: COLLECT used in a nested plan
    Given an empty graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE person.age < 50
      WITH [COLLECT {
       MATCH (person)-[:FRIEND]->(:Person)
       RETURN person.name ORDER BY person.name
      }[0]][0] AS foo
      RETURN foo
      """
    Then the result should be, in any order:
      | foo        |
      | 'Bob'      |
      | 'Ada'      |
      | 'Danielle' |
      | 'Carl'     |
    And no side effects

  Scenario: COLLECT should allow the shadowing of introduced variables
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE COLLECT {
       MATCH (person)-[:FRIEND]->(p:Person)
       WHERE COLLECT {
        WITH "Ada" as x
        MATCH (person)-[:FRIEND]->(p1:Person)
        WHERE p1.name = x
        WITH "Carl" as x, p1
        MATCH (p1)-[:FRIEND]-(p2:Person)
        WHERE p2.name = x
        RETURN person.name
       } = ['Bob']
       RETURN p.name ORDER BY p.name
      } = ['Ada', 'Danielle']
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name  |
      | 'Bob' |
    And no side effects

  Scenario: COLLECT with shadowing of an outer variable should result in error
    Given any graph
    When executing query:
      """
      WITH "Bosse" as x
      MATCH (person:Person)
      WHERE COLLECT {
       WITH "Karen" AS x
       MATCH (person)-[:FRIEND]->(d:Person)
       WHERE d.name = x
       RETURN person
      } = ['Danielle']
      RETURN person.name
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Collect should allow shadowing of variables not yet introduced in outer scope
    Given any graph
    When executing query:
    """
    WITH COLLECT {
      WITH 1 AS person
      RETURN person
    } AS list
    MATCH (person:Person)
    RETURN person.name AS name, list
    """
    Then the result should be, in any order:
    | name       | list |
    | 'Ada'      | [1]  |
    | 'Bob'      | [1]  |
    | 'Carl'     | [1]  |
    | 'Danielle' | [1]  |
    | 'Eve'      | [1]  |
    And no side effects

  Scenario: COLLECT with aggregation inside should work
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {prop: 1})-[:R]->(b:B {prop: 1}),
             (a)-[:R]->(:C {prop: 2}),
             (a)-[:R]->(d:D {prop: 3}),
             (b)-[:R]->(d)
      """
    When executing query:
      """
      MATCH (n) WHERE COLLECT {
        MATCH (n)-->(m)
        WITH n, count(*) AS numConnections
        WHERE numConnections = 3
        RETURN true
      } = [true]
      RETURN n
      """
    Then the result should be, in any order:
      | n             |
      | (:A {prop:1}) |
    And no side effects

  Scenario: COLLECT with aggregation comparison inside should work
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {prop: 1})-[:R]->(b:B {prop: 1}),
             (a)-[:R]->(:C {prop: 2}),
             (a)-[:R]->(d:D {prop: 3}),
             (b)-[:R]->(d)
      """
    When executing query:
      """
      MATCH (n) WHERE COLLECT {
        MATCH (n)-->(m)
        WITH n, count(*) = 3 AS hasThreeConns
        WHERE hasThreeConns
        RETURN true
      } = [true]
      RETURN n
      """
    Then the result should be, in any order:
      | n             |
      | (:A {prop:1}) |
    And no side effects

  Scenario: COLLECT with aggregation before and after subquery
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {prop: 1})-[:R]->(b:B {prop: 1}),
             (a)-[:R]->(:C {prop: 2}),
             (a)-[:R]->(d:D {prop: 3}),
             (b)-[:R]->(d)
      """
    When executing query:
      """
      MATCH (n) WHERE COLLECT {
        WITH count(*) = 1 AS fakeCheck
        MATCH (n)-->(m)
        WITH n, count(*) = 3 AS hasThreeConns, fakeCheck
        WHERE hasThreeConns AND fakeCheck
        RETURN true
      } = [true]
      RETURN n
      """
    Then the result should be, in any order:
      | n             |
      | (:A {prop:1}) |
    And no side effects

  Scenario: COLLECT with ORDER BY
      Given an empty graph
      When executing query:
    """
    MATCH (n:Person)
    RETURN COLLECT {
     MATCH (m)-[:FRIEND]->(n)
     RETURN m.name ORDER BY m.age
    } AS friends
    """
      Then the result should be, in any order:
        | friends             |
        | ['Bob']             |
        | ['Ada']             |
        | ['Ada', 'Danielle'] |
        | ['Carl', 'Bob']     |
        | []             |
      And no side effects

  Scenario: COLLECT with SKIP
      Given an empty graph
      When executing query:
    """
    MATCH (n:Person)
    WITH n, COLLECT {
     MATCH (m)-[:FRIEND]->(n)
     RETURN m.name SKIP 1
    } AS allButOneFriend
    RETURN n.name AS name, size(allButOneFriend) AS nbr
    """
      Then the result should be, in any order:
        | name       | nbr |
        | 'Ada'      | 0   |
        | 'Bob'      | 0   |
        | 'Carl'     | 1   |
        | 'Danielle' | 1   |
        | 'Eve'      | 0   |
      And no side effects

  Scenario: COLLECT with LIMIT
      Given an empty graph
      When executing query:
    """
    MATCH (n:Person)
    WITH n, COLLECT {
     MATCH (m)-[:FRIEND]->(n)
     RETURN m.name LIMIT 1
    } AS maxOneFriend
    RETURN n.name AS name, size(maxOneFriend) AS nbr
    """
      Then the result should be, in any order:
        | name       | nbr |
        | 'Ada'      | 1   |
        | 'Bob'      | 1   |
        | 'Carl'     | 1   |
        | 'Danielle' | 1   |
        | 'Eve'      | 0   |
      And no side effects

  Scenario: COLLECT with ORDER BY, SKIP and LIMIT
      Given an empty graph
      When executing query:
    """
    MATCH (n:Person)
    WHERE 'Bob' IN  COLLECT {
     MATCH (m)-[:FRIEND]->(n)
     RETURN m.name ORDER BY m.age SKIP 1 LIMIT 1
    }
    RETURN n.name AS name
    """
      Then the result should be, in any order:
        | name       |
        | 'Danielle' |
      And no side effects

  Scenario: COLLECT with DISTINCT
      Given an empty graph
      When executing query:
    """
    MATCH (n:Person)
    RETURN COLLECT {
     MATCH ()-[:FRIEND]->(n)
     RETURN DISTINCT n.name
    } AS nameIfFriend
    """
      Then the result should be, in any order:
        | nameIfFriend |
        | ['Ada']      |
        | ['Bob']      |
        | ['Carl']     |
        | ['Danielle'] |
        |  []          |
      And no side effects

  Scenario: COLLECT inlined in node pattern with label expression on unnamed node should be supported
    Given any graph
    When executing query:
    """
    MATCH (n:Person
    WHERE 'Bob' IN COLLECT {
      MATCH (n)-[]->(:Person)
      RETURN n.name
    })
    RETURN n.name AS name
    """
    Then the result should be, in any order:
      | name  |
      | 'Bob' |
    And no side effects

  Scenario: COLLECT inlined in node pattern with label expression on named node should be supported
    Given any graph
    When executing query:
    """
    MATCH (n:Person
    WHERE COLLECT {
      MATCH (n)-[]->(m:Person)
      RETURN m.name ORDER BY m.name
    } = ['Bob', 'Carl'])
    RETURN n.name AS name
    """
    Then the result should be, in any order:
      | name  |
      | 'Ada' |
    And no side effects

  Scenario: COLLECT inlined in relationship pattern with label expression on unnamed node should be supported
    Given any graph
    When executing query:
    """
    MATCH (n:Person)-[r
    WHERE COLLECT {
      MATCH (n)-[]->(:Person)
      RETURN true
    } = [true, true]]->(m)
    RETURN n.name AS name
    """
    Then the result should be, in any order:
      | name  |
      | 'Ada' |
      | 'Ada' |
      | 'Bob' |
      | 'Bob' |
    And no side effects

  Scenario: COLLECT inlined in relationship pattern with label expression on named node should be supported
    Given any graph
    When executing query:
    """
    MATCH (n:Person)-[r
    WHERE COLLECT {
      MATCH (n)-[]->(p:Person)
      RETURN true
    } = [true, true]]->(m)
    RETURN n.name AS name
    """
    Then the result should be, in any order:
      | name  |
      | 'Ada' |
      | 'Ada' |
      | 'Bob' |
      | 'Bob' |
    And no side effects

  Scenario: COLLECT with update clause should fail
    Given any graph
    When executing query:
      """
      MATCH (n) WHERE COLLECT {
        MATCH (n)-->(m)
        SET m.prop='fail'
      } = [1]
      RETURN n
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: COLLECT with updating procedure should fail
    Given any graph
    When executing query:
      """
      MATCH (n) WHERE COLLECT {
        CALL db.createLabel("CAT")
      } = [1]
      RETURN n
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: COLLECT with updating procedure and yield should fail
    Given any graph
    When executing query:
      """
      RETURN COLLECT {
        CALL dbms.setDefaultDatabase('something')
        YIELD result
        RETURN result
      }
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: COLLECT with return * should fail
    Given any graph
    When executing query:
      """
      MATCH (n) WHERE COLLECT {
        MATCH (n)-[]->(p) RETURN *
      } = [1]
      RETURN n
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: COLLECT with multiple return items should fail
    Given any graph
    When executing query:
      """
      RETURN COLLECT {
        MATCH (n)-[]->(p) RETURN n, p
      }
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: COLLECT with FINISH should fail
    Given any graph
    When executing query:
      """
      RETURN COLLECT {
        MATCH (n)-[]->(p) FINISH
      }
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: COLLECT subquery as property inside node
    Given an empty graph
    When executing query:
      """
      WITH 0 AS n0
      MATCH ({n1:COLLECT { RETURN 0 AS x } })
      RETURN 2 as result
      """
    Then the result should be, in any order:
      | result |

  Scenario: COLLECT subquery with empty node
    Given an empty graph
    When executing query:
      """
      MERGE (x:A)
      RETURN COLLECT { MATCH () RETURN 1 } as result
      ORDER BY x
      """
    Then the result should be, in any order:
      | result             |
      | [1, 1, 1, 1, 1, 1] |

  Scenario: COLLECT subquery with aggregation inside should work
    Given an empty graph
    When executing query:
      """
      MATCH (a)
      RETURN COLLECT {
        MATCH (a)--(b)
        RETURN count(b.foo)
      } AS collect
      """
    Then the result should be, in any order:
      | collect |
      | [0]     |
      | [0]     |
      | [0]     |
      | [0]     |
      | [0]     |