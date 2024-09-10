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

Feature: CountExpressionAcceptance

  Background:
    Given an empty graph
    And having executed:
    """
    CREATE (a:Person {name: "Ada", age: 100}),
       (b:Person {name: "Bob", age: 50}),
       (c:Person:Therianthrope {name: "Cat", age: 20}),
       (d:Person {name: "Deb", age: 20}),
       (e:Person {name: "Erika", age: 26}),
       (f:OperatingSystem {name: "Deb"}),
       (a)-[:FOLLOWS]->(b),
       (a)-[:FOLLOWS]->(c),
       (b)-[:FOLLOWS]->(a),
       (c)-[:FOLLOWS]->(a),
       (c)-[:FOLLOWS]->(b),
       (e)-[:FOLLOWS]->(c),
       (b)-[:LIKES]->(c),
       (b)-[:LIKES]->(a),
       (d)-[:LIKES]->(a),
       (b)-[:LIKES]->(e)
    """

  Scenario: Simple COUNT with MATCH keyword
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      WHERE COUNT { MATCH (p)-[:FOLLOWS]->() } > 1
      RETURN p.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Ada' |
      | 'Cat' |
    And no side effects

  Scenario: Full COUNT with MATCH keyword
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      WHERE COUNT { MATCH (p)-[:FOLLOWS]->() RETURN p } > 1
      RETURN p.name AS name
      """
    Then the result should be, in any order:
      | name  |
      | 'Ada' |
      | 'Cat' |
    And no side effects

  Scenario: Full COUNT with MATCH keyword and RETURN that is null
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      WHERE COUNT { MATCH (p) RETURN p.nonExistingProp } = 1
      RETURN p.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Ada'   |
      | 'Bob'   |
      | 'Cat'   |
      | 'Deb'   |
      | 'Erika' |
    And no side effects

  Scenario: COUNT with RETURN *
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      WHERE COUNT { MATCH (p) RETURN * } = 1
      RETURN p.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Ada'   |
      | 'Bob'   |
      | 'Cat'   |
      | 'Deb'   |
      | 'Erika' |
    And no side effects

  Scenario: Standalone COUNT with RETURN *
    Given an empty graph
    When executing query:
      """
      RETURN COUNT { MATCH (p) RETURN * } AS countOfNodes
      """
    Then the result should be, in any order:
      | countOfNodes |
      | 6            |
    And no side effects

  Scenario: Simple COUNT without where clause
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      WHERE COUNT { (p)-[:FOLLOWS]->() } > 1
      RETURN p.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Ada' |
      | 'Cat' |
    And no side effects

  Scenario: Simple COUNT with single node
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN COUNT { (p) } AS pcount
      """
    Then the result should be, in any order:
      | pcount |
      | 1      |
      | 1      |
      | 1      |
      | 1      |
      | 1      |
    And no side effects

  Scenario: Simple COUNT with single unbound node
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN COUNT { (q) } AS qcount
      """
    Then the result should be, in any order:
      | qcount |
      | 6      |
      | 6      |
      | 6      |
      | 6      |
      | 6      |
    And no side effects

  Scenario: Full COUNT with reference to outer variable
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN p.name as name, COUNT { MATCH (q) WHERE q.age > p.age RETURN q } AS qcount
      """
    Then the result should be, in any order:
      | name    | qcount |
      | 'Ada'   | 0      |
      | 'Bob'   | 1      |
      | 'Cat'   | 3      |
      | 'Deb'   | 3      |
      | 'Erika' | 2      |
    And no side effects

  Scenario: Simple COUNT with single node with inlined property map predicate
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN COUNT { (p {age: 50}) } AS pcount
      """
    Then the result should be, in any order:
      | pcount |
      | 0      |
      | 1      |
      | 0      |
      | 0      |
      | 0      |
    And no side effects

  Scenario: Simple COUNT with single unbound node with inlined property map predicate
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN COUNT { ({age: 50}) } AS pcount
      """
    Then the result should be, in any order:
      | pcount |
      | 1      |
      | 1      |
      | 1      |
      | 1      |
      | 1      |
    And no side effects

  Scenario: Simple COUNT with single node with inlined predicate
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN COUNT { (p WHERE p.age > 30) } AS pcount
      """
    Then the result should be, in any order:
      | pcount |
      | 1      |
      | 1      |
      | 0      |
      | 0      |
      | 0      |
    And no side effects

  Scenario: Simple COUNT with single node with label predicate
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN COUNT { (p:Therianthrope) } AS pcount
      """
    Then the result should be, in any order:
      | pcount |
      | 0      |
      | 0      |
      | 1      |
      | 0      |
      | 0      |
    And no side effects

  Scenario: Simple COUNT with single node with predicate
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN COUNT { (p) WHERE p.age > 30 } AS pcount
      """
    Then the result should be, in any order:
      | pcount |
      | 1      |
      | 1      |
      | 0      |
      | 0      |
      | 0      |
    And no side effects

  Scenario: Full COUNT with single node with predicate
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN COUNT { MATCH (p) WHERE p.age > 30 RETURN p } AS pcount
      """
    Then the result should be, in any order:
      | pcount |
      | 1      |
      | 1      |
      | 0      |
      | 0      |
      | 0      |
    And no side effects

  Scenario: COUNT on the right side of an OR should work
    Given an empty graph
    When executing query:
      """
      MATCH (a:Person), (b:Person { name:'Bob' })
      WHERE a.name = "Ada"
      OR COUNT {
        MATCH (a)-[:FOLLOWS]->(b)
      } = 1
      RETURN a.name as name
      """
    Then the result should be, in any order:
      | name  |
      | 'Ada' |
      | 'Cat' |
    And no side effects

  Scenario: COUNT on the right side of an OR with a NOT should work
    Given an empty graph
    When executing query:
      """
      MATCH (a:Person), (b:Person { name:'Bob' })
      WHERE a.name = "Ada"
      OR NOT COUNT {
        MATCH (a)-[:FOLLOWS]->(b)
      } = 1
      RETURN a.name as name
      """
    Then the result should be, in any order:
      | name    |
      | 'Ada'   |
      | 'Bob'   |
      | 'Deb'   |
      | 'Erika' |
    And no side effects

  Scenario: COUNT on the right side of an XOR should work
    Given an empty graph
    When executing query:
    """
      MATCH (a:Person), (b:Person { name:'Bob' })
      WHERE a.name = "Ada"
      XOR COUNT {
        MATCH (a)-[:FOLLOWS]->(b)
      } = 1
      RETURN a.name as name
      """
    Then the result should be, in any order:
      | name  |
      | 'Cat' |
    And no side effects

  Scenario: COUNT on the right side of an XOR with a NOT should work
    Given an empty graph
    When executing query:
    """
      MATCH (a:Person), (b:Person { name:'Bob' })
      WHERE a.name = "Ada"
      XOR NOT COUNT {
        MATCH (a)-[:FOLLOWS]->(b)
      } = 1
      RETURN a.name as name
      """
    Then the result should be, in any order:
      | name    |
      | 'Ada'   |
      | 'Bob'   |
      | 'Deb'   |
      | 'Erika' |
    And no side effects


  Scenario: Inner query with create should fail with syntax error
    Given an empty graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE COUNT {
       CREATE (person)-[:HAS_DOG]->(:Dog)
      } > 1
      RETURN person.name
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Inner query with set should fail with syntax error
    Given an empty graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE COUNT {
       SET person.name = "Karen"
      } > 3
      RETURN person.name
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Inner query with merge should fail with syntax error
    Given an empty graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE COUNT {
        MATCH (person)
        MERGE (person)-[:HAS_DOG]->(Dog { name: "Pluto" })
      } > 1
      RETURN person.name
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Inner query with delete should fail with syntax error
    Given an empty graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE COUNT {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
        DETACH DELETE dog
      } > 1
      RETURN person.name
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Simple COUNT with where clause in a node
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      WHERE COUNT { (p)-[r]->(f WHERE f.age > 30) } > 1
      RETURN p.name AS name
      """
    Then the result should be, in any order:
      | name  |
      | 'Bob' |
      | 'Cat' |
    And no side effects

  Scenario: Simple COUNT with where clause outside the node
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      WHERE COUNT { (p)-[r]->(f) WHERE f.age > 30 } > 1
      RETURN p.name AS name
      """
    Then the result should be, in any order:
      | name  |
      | 'Bob' |
      | 'Cat' |
    And no side effects

  Scenario: Count can be nested inside another count
    Given an empty graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE COUNT {
         MATCH (person)-[:LIKES]->(otherPerson:Person)
         WHERE COUNT {
           (otherPerson)-[:FOLLOWS]->(cat:Therianthrope)
         } = 1
         RETURN person
      } > 1
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name  |
      | 'Bob' |
    And no side effects

  Scenario: Count with function use should work
    Given an empty graph
    When executing query:
      """
      MATCH (person:Person)
      WITH COUNT {
       MATCH (person)-[:LIKES]->(friend:Person)
       WHERE reverse(friend.name) = "akirE"
       RETURN person
      } as foo
      RETURN foo
      """
    Then the result should be, in any order:
      | foo |
      | 0   |
      | 1   |
      | 0   |
      | 0   |
      | 0   |
    And no side effects

  Scenario: Count with a union made of RETURNs
    Given an empty graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE COUNT {
          RETURN 1 as a
          UNION
          RETURN 2 as a
      } = 2
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name      |
      | 'Ada'     |
      | 'Bob'     |
      | 'Cat'     |
      | 'Deb'     |
      | 'Erika'   |
    And no side effects

  Scenario: Count with a RETURNING case
    Given an empty graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE COUNT {
          RETURN CASE
             WHEN true THEN 1
             ELSE 2
          END
      } = 1
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name      |
      | 'Ada'     |
      | 'Bob'     |
      | 'Cat'     |
      | 'Deb'     |
      | 'Erika'   |
    And no side effects

  Scenario: COUNT should work in a return statement
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN COUNT { (p)-[:FOLLOWS]->() } AS numFollowed,
             p.name                       AS name
      """
    Then the result should be, in any order:
      | numFollowed  | name      |
      | 2            | 'Ada'     |
      | 1            | 'Bob'     |
      | 2            | 'Cat'     |
      | 0            | 'Deb'     |
      | 1            | 'Erika'   |
    And no side effects

  Scenario: COUNT works in conjunction with algebraic operations
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN COUNT { (p)-[:FOLLOWS]->() } * 2 + 1 AS result,
             p.name AS name
      """
    Then the result should be, in any order:
      | result | name      |
      | 5      | 'Ada'     |
      | 3      | 'Bob'     |
      | 5      | 'Cat'     |
      | 1      | 'Deb'     |
      | 3      | 'Erika'   |
    And no side effects

  Scenario: COUNT should work as grouping key
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN COUNT { (p)-[:FOLLOWS]->(f) } AS numFollowed,
             avg(p.age)                    AS averageAge
      """
    Then the result should be, in any order:
      | numFollowed | averageAge |
      | 2           | 60.0       |
      | 1           | 38.0       |
      | 0           | 20.0       |
    And no side effects

  Scenario: COUNT should work together with count aggregation
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN COUNT { (p)-[:FOLLOWS]->(f) } AS numFollowed,
             count(p)                      AS numPersons
      """
    Then the result should be, in any order:
      | numFollowed | numPersons |
      | 2           | 2          |
      | 1           | 2          |
      | 0           | 1          |
    And no side effects

  Scenario: COUNT should work with a node pattern
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)-[:FOLLOWS]->(f)
      RETURN count(f) AS numFollowed1,
             COUNT{(f)} AS numFollowed2
      """
    Then the result should be, in any order:
      | numFollowed1 | numFollowed2 |
      | 6            | 1            |
    And no side effects

  Scenario: COUNT should work in a WHEN statement
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      RETURN CASE WHEN COUNT{(p:Person)<-[:FOLLOWS]-(f)} > 0 THEN p.name END AS result
      """
    Then the result should be, in any order:
      | result |
      | 'Ada'  |
      | 'Bob'  |
      | 'Cat'  |
      | null   |
      | null   |
    And no side effects

  Scenario: COUNT should work in a WITH statement
    Given an empty graph
    When executing query:
      """
      WITH COUNT{(p:Person)<-[:FOLLOWS]-(f)} AS followers
      RETURN followers
      """
    Then the result should be, in any order:
      | followers |
      | 6         |
    And no side effects

  Scenario: COUNT should work in a SET statement
    Given an empty graph
    When executing query:
      """
      MATCH (p:Person)
      SET p.numFollowers = COUNT{(p:Person)<-[:FOLLOWS]-(f)}
      RETURN p.name AS name, p.numFollowers AS numFollowers
      """
    Then the result should be, in any order:
      | name    | numFollowers |
      | 'Ada'   | 2            |
      | 'Bob'   | 2            |
      | 'Cat'   | 2            |
      | 'Deb'   | 0            |
      | 'Erika' | 0            |
    And the side effects should be:
      | +properties | 5 |

  Scenario: COUNT should return zero for NULL nodes
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH (n:DoesNotExist)
      RETURN [
        COUNT { (n) },
        COUNT { (n)-->() },
        COUNT { (n)-->({prop: 1}) }
      ] AS counts
      """
    Then the result should be, in any order:
      | counts    |
      | [0, 0, 0] |
    And no side effects

  Scenario: COUNT should return zero for NULL relationships
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH ()-[r:DOES_NOT_EXIST]->()
      RETURN [
        COUNT { ()-[r]-() },
        COUNT { ()-[r]-({prop: 1}) }
      ] AS counts
      """
    Then the result should be, in any order:
      | counts |
      | [0, 0] |
    And no side effects

  Scenario: Multiple patterns in inner MATCH should work
    Given an empty graph
    When executing query:
      """
      MATCH (person:Person)
      RETURN person.name AS name, COUNT {
       (person), (os:OperatingSystem) WHERE os.name STARTS WITH person.name
      } AS osWithSimilarName
      """
    Then the result should be, in any order:
      | name    | osWithSimilarName |
      | 'Ada'   | 0                 |
      | 'Bob'   | 0                 |
      | 'Cat'   | 0                 |
      | 'Deb'   | 1                 |
      | 'Erika' | 0                 |
    And no side effects

  Scenario: Multiple patterns with relationships in inner MATCH should work
    Given an empty graph
    When executing query:
      """
      MATCH (person:Person)
      RETURN person.name AS name, COUNT {
       (person)-[:FOLLOWS]->(other:Person), (other)-[:FOLLOWS]->(person)
      } AS digitalFriends
      """
    Then the result should be, in any order:
      | name    | digitalFriends |
      | 'Ada'   | 2              |
      | 'Bob'   | 1              |
      | 'Cat'   | 1              |
      | 'Deb'   | 0              |
      | 'Erika' | 0              |
    And no side effects

  Scenario: Count with UNION should work
    Given an empty graph
    When executing query:
      """
      WITH COUNT {
       MATCH (person:Person)-[:FOLLOWS]->(friend:Person)
       RETURN friend AS human
       UNION
       MATCH (person:Person)-[:LIKES]->(otherPerson:Person)
       RETURN otherPerson AS human
      } as foo
      RETURN foo
      """
    Then the result should be, in any order:
      | foo |
      | 4   |
    And no side effects

  Scenario: Count with UNION that references outer variable should work
    Given an empty graph
    When executing query:
      """
      MATCH (person)
      WITH COUNT {
       MATCH (person)-[:FOLLOWS]->(friend:Person)
       RETURN friend.name AS name
       UNION
       MATCH (person)-[:LIKES]->(otherPerson:Person)
       RETURN otherPerson.name AS name
      } as foo
      RETURN foo
      """
    Then the result should be, in any order:
      | foo |
      | 2   |
      | 3   |
      | 2   |
      | 1   |
      | 1   |
      | 0   |
    And no side effects

  Scenario: Count with UNION ALL should work
    Given an empty graph
    When executing query:
      """
      MATCH (person)
      WITH COUNT {
       MATCH (person)-[:FOLLOWS]->(friend:Person)
       RETURN friend AS human
       UNION ALL
       MATCH (person)-[:LIKES]->(otherPerson:Person)
       RETURN otherPerson AS human
      } as foo
      RETURN foo
      """
    Then the result should be, in any order:
      | foo |
      | 2   |
      | 4   |
      | 2   |
      | 1   |
      | 1   |
      | 0   |
    And no side effects

  Scenario: Count with UNION ALL and no returns should work
    Given an empty graph
    When executing query:
      """
      MATCH (person)
      WITH COUNT {
       MATCH (person)-[:FOLLOWS]->(friend:Person)
       UNION ALL
       MATCH (person)-[:LIKES]->(otherPerson:Person)
      } as foo
      RETURN foo
      """
    Then the result should be, in any order:
      | foo |
      | 2   |
      | 4   |
      | 2   |
      | 1   |
      | 1   |
      | 0   |
    And no side effects

  Scenario: Count with CALL should work
    Given an empty graph
    When executing query:
      """
      MATCH (person:Person)
      WITH COUNT {
       MATCH (person)-[:FOLLOWS]->(friend:Person)
       CALL {
         WITH person
         MATCH (person)-[:FOLLOWS]->(friend:Person)
         RETURN friend.name AS name
         UNION ALL
         WITH person
         MATCH (person)-[:LIKES]->(otherPerson:Person)
         RETURN otherPerson.name AS name
       }
       RETURN friend AS notHumans
      } as foo
      RETURN foo
      """
    Then the result should be, in any order:
      | foo |
      | 4   |
      | 4   |
      | 4   |
      | 0   |
      | 1   |
    And no side effects

  Scenario: Count used in an expression with a Union
    Given an empty graph
    When executing query:
      """
      MATCH (person)
      WHERE COUNT {
       MATCH (person)-[:FOLLOWS]->(friend:Person)
       RETURN friend.name AS name
       UNION
       MATCH (person)-[:LIKES]->(otherPerson:Person)
       RETURN otherPerson.name AS name
      } + 1 = 3
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name  |
      | 'Ada' |
      | 'Cat' |
    And no side effects

  Scenario: Count used in a nested plan
    Given an empty graph
    When executing query:
      """
      MATCH (person:Person)
      WITH [COUNT {
       MATCH (person)-[:FOLLOWS]->(:Person)
       RETURN person.foo
      }][0] AS foo
      RETURN foo
      """
    Then the result should be, in any order:
      | foo |
      | 2   |
      | 1   |
      | 2   |
      | 0   |
      | 1   |
    And no side effects

  Scenario: COUNT should allow omission of RETURN in more complex queries
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE COUNT {
       MATCH (person)-[:FOLLOWS]->(p:Person)
       WHERE COUNT {
        WITH "Ada" as x
        MATCH (person)-[:FOLLOWS]->(person2:Person)
        WHERE person2.name = x
        WITH "Cat" as x
        MATCH (person2)-[:LIKES]-(person3:Person)
        WHERE person3.name = x
       } = 1
      } = 1
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name  |
      | 'Bob' |
    And no side effects

  Scenario: COUNT should have same results with a RETURN as one without
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE COUNT {
       MATCH (person)-[:FOLLOWS]->(p:Person)
       WHERE COUNT {
        WITH "Ada" as x
        MATCH (person)-[:FOLLOWS]->(person2:Person)
        WHERE person2.name = x
        WITH "Cat" as x
        MATCH (person2)-[:LIKES]-(person3:Person)
        WHERE person3.name = x
        RETURN person3.name
       } = 1
      } = 1
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name  |
      | 'Bob' |
    And no side effects

  Scenario: Count should allow the shadowing of introduced variables
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE COUNT {
       MATCH (person)-[:FOLLOWS]->(p:Person)
       WHERE COUNT {
        WITH "Ada" as x
        MATCH (person)-[:FOLLOWS]->(p1:Person)
        WHERE p1.name = x
        WITH "Cat" as x, p1
        MATCH (p1)-[:FOLLOWS]-(p2:Person)
        WHERE p2.name = x
        RETURN person AS person
       } = 2
       RETURN person AS person
      } = 1
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bob' |
    And no side effects

  Scenario: Count with shadowing of an outer variable should result in error
    Given any graph
    When executing query:
      """
      WITH "Bosse" as x
      MATCH (person:Person)
      WHERE COUNT {
       WITH "Ozzy" AS x
       MATCH (person)-[:HAS_DOG]->(d:Dog)
       WHERE d.name = x
       RETURN person
      } > 3
      RETURN person.name
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Count should allow shadowing of variables not yet introduced in outer scope
    Given any graph
    When executing query:
    """
    WITH COUNT {
      WITH 1 AS person
    } AS count
    MATCH (person:Person)
    RETURN person.name AS name, count
    """
    Then the result should be, in any order:
    | name    | count |
    | 'Ada'   | 1     |
    | 'Bob'   | 1     |
    | 'Cat'   | 1     |
    | 'Deb'   | 1     |
    | 'Erika' | 1     |
    And no side effects

  Scenario: Full count subquery with aggregation inside
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
      MATCH (n) WHERE COUNT {
        MATCH (n)-->(m)
        WITH n, count(*) AS numConnections
        WHERE numConnections = 3
        RETURN true
      } = 1
      RETURN n
      """
    Then the result should be, in any order:
      | n             |
      | (:A {prop:1}) |
    And no side effects

  Scenario: Full count subquery with aggregation comparison inside
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
      MATCH (n) WHERE COUNT {
        MATCH (n)-->(m)
        WITH n, count(*) = 3 AS hasThreeConns
        WHERE hasThreeConns
        RETURN true
      } = 1
      RETURN n
      """
    Then the result should be, in any order:
      | n             |
      | (:A {prop:1}) |
    And no side effects

  Scenario: Full count subquery with aggregation before and after subquery
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
      MATCH (n) WHERE COUNT {
        WITH count(*) = 1 AS fakeCheck
        MATCH (n)-->(m)
        WITH n, count(*) = 3 AS hasThreeConns, fakeCheck
        WHERE hasThreeConns AND fakeCheck
        RETURN true
      } = 1
      RETURN n
      """
    Then the result should be, in any order:
      | n             |
      | (:A {prop:1}) |
    And no side effects

  Scenario: Count inlined in node pattern with label expression on unnamed node should be supported
    Given any graph
    When executing query:
    """
    MATCH (n:Person
    WHERE COUNT {
      MATCH (n)-[]->(:Person)
    } > 1)
    RETURN n.name AS name
    """
    Then the result should be, in any order:
      | name  |
      | 'Ada' |
      | 'Bob' |
      | 'Cat' |
    And no side effects

  Scenario: Count inlined in node pattern with label expression on named node should be supported
    Given any graph
    When executing query:
    """
    MATCH (n:Person
    WHERE COUNT {
      MATCH (n)-[]->(m:Person)
    } > 1)
    RETURN n.name AS name
    """
    Then the result should be, in any order:
      | name  |
      | 'Ada' |
      | 'Bob' |
      | 'Cat' |
    And no side effects

  Scenario: Nested inlined count in node pattern should be supported
    Given an empty graph
    When executing query:
    """
    MATCH (a
      WHERE COUNT {
        MATCH (n WHERE n.name = a.name)-[r]->()
      } > 2
    )
    RETURN a.name AS name
    """
    Then the result should be, in any order:
      | name  |
      | 'Bob' |
    And no side effects

  Scenario: Nested inlined XOR between count and other predicate in node pattern should be supported
    Given an empty graph
    When executing query:
    """
    MATCH (n:Person)
    WHERE COUNT {
      MATCH (n WHERE COUNT { MATCH (n)-[r]->() } > 2 XOR true)
    } = 1
    RETURN n.name AS name
    """
    Then the result should be, in any order:
      | name    |
      | 'Ada'   |
      | 'Cat'   |
      | 'Deb'   |
      | 'Erika' |
    And no side effects

  Scenario: Count inlined in relationship pattern with label expression on unnamed node should be supported
    Given any graph
    When executing query:
    """
    MATCH (n:Person)-[r
    WHERE COUNT {
      MATCH (n)-[]->(:Person)
    } > 1]->(m)
    RETURN n.name AS name
    """
    Then the result should be, in any order:
      | name  |
      | 'Ada' |
      | 'Ada' |
      | 'Bob' |
      | 'Bob' |
      | 'Bob' |
      | 'Bob' |
      | 'Cat' |
      | 'Cat' |
    And no side effects

  Scenario: Count inlined in relationship pattern with label expression on named node should be supported
    Given any graph
    When executing query:
    """
    MATCH (n:Person)-[r
    WHERE COUNT {
      MATCH (n)-[]->(p:Person)
    } > 1]->(m)
    RETURN n.name AS name
    """
    Then the result should be, in any order:
      | name  |
      | 'Ada' |
      | 'Ada' |
      | 'Bob' |
      | 'Bob' |
      | 'Bob' |
      | 'Bob' |
      | 'Cat' |
      | 'Cat' |
    And no side effects

  Scenario: Nested inlined count in relationship pattern should be supported
    Given an empty graph
    When executing query:
    """
    MATCH (a)-[
      WHERE COUNT {
        MATCH (n)-[r WHERE n.name = a.name]->()
      } > 2
    ]->()
    RETURN a.name AS name
    """
    Then the result should be, in any order:
      | name  |
      | 'Bob' |
      | 'Bob' |
      | 'Bob' |
      | 'Bob' |
    And no side effects

  Scenario: Nested inlined XOR between count and other predicate in relationship pattern should be supported
    Given an empty graph
    When executing query:
    """
    MATCH (n:Person)
    WHERE COUNT {
      MATCH (n)-[WHERE COUNT { MATCH (n)-[r]->() } > 2 XOR true]->()
    } = 1
    RETURN n.name AS name
    """
    Then the result should be, in any order:
      | name    |
      | 'Deb'   |
      | 'Erika' |
    And no side effects

    Scenario: COUNT in WHERE with ORDER BY
      Given an empty graph
      When executing query:
    """
    MATCH (n:Person)
    WHERE COUNT {
     MATCH (n)-[:FOLLOWS]->(m)
     RETURN m ORDER BY m.name
    } = 1
    RETURN n.name AS name
    """
      Then the result should be, in any order:
        | name    |
        | 'Bob'   |
        | 'Erika' |
      And no side effects


  Scenario: COUNT in RETURN with ORDER BY
      Given an empty graph
      When executing query:
    """
    MATCH (n:Person)
    RETURN COUNT {
     MATCH (n)-[:FOLLOWS]->(m)
     RETURN m ORDER BY m.name
    } AS nbr
    """
      Then the result should be, in any order:
        | nbr |
        | 2   |
        | 1   |
        | 2   |
        | 0   |
        | 1   |
      And no side effects

  Scenario: COUNT in WHERE with SKIP
      Given an empty graph
      When executing query:
    """
    MATCH (n:Person)
    WHERE COUNT {
     MATCH (n)-[:FOLLOWS]->(m)
     RETURN m SKIP 1
    } = 1
    RETURN n.name AS name
    """
      Then the result should be, in any order:
        | name    |
        | 'Ada'   |
        | 'Cat'   |
      And no side effects

  Scenario: COUNT in RETURN with SKIP
      Given an empty graph
      When executing query:
    """
    MATCH (n:Person)
    RETURN COUNT {
     MATCH (n)-[:FOLLOWS]->(m)
     RETURN m SKIP 1
    } AS nbr
    """
      Then the result should be, in any order:
        | nbr |
        | 1   |
        | 0   |
        | 1   |
        | 0   |
        | 0   |
      And no side effects

  Scenario: COUNT in WHERE with LIMIT
      Given an empty graph
      When executing query:
    """
    MATCH (n:Person)
    WHERE COUNT {
     MATCH (n)-[:FOLLOWS]->(m)
     RETURN m LIMIT 1
    } = 1
    RETURN n.name AS name
    """
      Then the result should be, in any order:
        | name    |
        | 'Ada'   |
        | 'Bob'   |
        | 'Cat'   |
        | 'Erika' |
      And no side effects

  Scenario: COUNT in RETURN with LIMIT
      Given an empty graph
      When executing query:
    """
    MATCH (n:Person)
    RETURN COUNT {
     MATCH (n)-[:FOLLOWS]->(m)
     RETURN m LIMIT 1
    } AS nbr
    """
      Then the result should be, in any order:
        | nbr |
        | 1   |
        | 1   |
        | 1   |
        | 0   |
        | 1   |
      And no side effects

  Scenario: COUNT in WHERE with ORDER BY, SKIP and LIMIT
      Given an empty graph
      When executing query:
    """
    MATCH (n:Person)
    WHERE COUNT {
     MATCH (n)-[]->(m)
     RETURN m ORDER BY m.name SKIP 1 LIMIT 2
    } = 1
    RETURN n.name AS name
    """
      Then the result should be, in any order:
        | name  |
        | 'Ada' |
        | 'Cat' |
      And no side effects


  Scenario: COUNT in RETURN with ORDER BY, SKIP and LIMIT
      Given an empty graph
      When executing query:
    """
    MATCH (n:Person)
    RETURN COUNT {
     MATCH (n)-[]->(m)
     RETURN m ORDER BY m.name SKIP 1 LIMIT 2
    } AS nbr
    """
      Then the result should be, in any order:
        | nbr |
        | 1   |
        | 2   |
        | 1   |
        | 0   |
        | 0   |
      And no side effects

  Scenario: COUNT in WHERE with DISTINCT
      Given an empty graph
      When executing query:
    """
    MATCH (n:Person)
    WHERE COUNT {
     MATCH (n)-[:FOLLOWS]->(m)
     RETURN DISTINCT n
    } = 1
    RETURN n.name AS name
    """
      Then the result should be, in any order:
        | name    |
        | 'Ada'   |
        | 'Bob'   |
        | 'Cat'   |
        | 'Erika' |
      And no side effects

  Scenario: COUNT in RETURN with DISTINCT
      Given an empty graph
      When executing query:
    """
    MATCH (n:Person)
    RETURN COUNT {
     MATCH (n)-[:FOLLOWS]->(m)
     RETURN DISTINCT n
    } AS nbr
    """
      Then the result should be, in any order:
        | nbr |
        | 1   |
        | 1   |
        | 1   |
        | 0   |
        | 1   |
      And no side effects

  Scenario: Full count subquery with update clause should fail
    Given any graph
    When executing query:
      """
      MATCH (n) WHERE COUNT {
        MATCH (n)-->(m)
        SET m.prop='fail'
      } = 2
      RETURN n
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Count subquery with updating procedure should fail
    Given any graph
    When executing query:
      """
      MATCH (n) WHERE COUNT {
        CALL db.createLabel("CAT")
      } > 1
      RETURN n
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Count subquery with FINISH should fail
    Given any graph
    When executing query:
      """
      MATCH (n) WHERE COUNT {
        MATCH (n)-->(m)
        FINISH
      } > 1
      RETURN n
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: COUNT subquery in join key
    Given an empty graph
    And having executed:
      """
      CREATE (:A {prop: 1})
      CREATE (:A {prop: 2})
      CREATE (:A {prop: 3})

      CREATE (b1:B {name: 'one'})-[:REL]->(:X)

      CREATE (b3:B {name: 'three'})-[:REL]->(:X)
      CREATE (b3)-[:REL]->(:X)
      CREATE (b3)-[:REL]->(:X)

      CREATE (b4:B {name: 'four'})-[:REL]->(:X)
      CREATE (b4)-[:REL]->(:X)
      CREATE (b4)-[:REL]->(:X)
      CREATE (b4)-[:REL]->(:X)
      """
    When executing query:
      """
      MATCH (a:A), (b:B)
      WHERE a.prop = COUNT { (b)-->(:X) }
      RETURN a.prop, b.name
      """
    Then the result should be, in any order:
      | a.prop | b.name  |
      | 1      | 'one'   |
      | 3      | 'three' |

  Scenario: COUNT subquery as property inside node
    Given an empty graph
    When executing query:
      """
      WITH 0 AS n0
      MATCH ({n1:COUNT { RETURN 0 AS x } })
      RETURN 2 as result
      """
    Then the result should be, in any order:
      | result |

  Scenario: COUNT subquery with empty node
    Given an empty graph
    When executing query:
      """
      MERGE (x:A)
      RETURN COUNT { () } as result
      ORDER BY x
      """
    Then the result should be, in any order:
      | result |
      | 7      |

  Scenario: COUNT subquery with aggregation inside should work
    Given an empty graph
    When executing query:
      """
      MATCH (a)
      RETURN COUNT {
        MATCH (a)--(b)
        RETURN count(b.foo)
      } AS count
      """
    Then the result should be, in any order:
      | count |
      | 1     |
      | 1     |
      | 1     |
      | 1     |
      | 1     |
      | 1     |

  Scenario: COUNT of cartesian product of a node and relationship patterns
    Given an empty graph
    When executing query:
      """
      RETURN
        // See `Background` setup at the top of the file
        COUNT { (n), (x)-[r]->(y) } AS countCartesian,
        COUNT { (n) } * COUNT { (x)-[r]->(y) } AS countMul
      """
    Then the result should be, in any order:
      | countCartesian | countMul |
      | 60             | 60       |
    And no side effects