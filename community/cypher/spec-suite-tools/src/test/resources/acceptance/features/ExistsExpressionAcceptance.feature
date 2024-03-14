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

Feature: ExistsExpressionAcceptance

  Background:
    Given an empty graph
    And having executed:
    """
    CREATE (:Person {name:'Alice', id: 0, canAffordDog: false}),
       (:Person {name:'Bosse', lastname: 'Bobson', id: 1, canAffordDog: true})-[:HAS_DOG {since: 2016}]->(:Dog {name:'Bosse'}),
       (fidoDog:Dog {name:'Fido'})<-[:HAS_DOG {since: 2010}]-(:Person {name:'Chris', id:2, canAffordDog: false})-[:HAS_DOG {since: 2018}]->(ozzyDog:Dog {name:'Ozzy'}),
       (fidoDog)-[:HAS_FRIEND]->(ozzyDog)
    """

  Scenario: Simple Exists without where clause should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
       MATCH (person)-[:HAS_DOG]->(:Dog)
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
      | 'Chris' |
    And no side effects

  Scenario: Exists with RETURN that is null
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
       MATCH (person)-[:HAS_DOG]->(:Dog)
       RETURN person.nonExistingProp
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
      | 'Chris' |
    And no side effects

  Scenario: Exists with RETURN *
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
       MATCH (person)-[:HAS_DOG]->(:Dog)
       RETURN *
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
      | 'Chris' |
    And no side effects

  Scenario: Standalone Exists with RETURN *
    Given any graph
    When executing query:
      """
      RETURN EXISTS {
       MATCH (person)-[:HAS_DOG]->(:Dog)
       RETURN *
      } as someoneHasADog
      """
    Then the result should be, in any order:
      | someoneHasADog |
      | true           |
    And no side effects

  Scenario: Exists with shadowing of an outer variable should result in error
    Given any graph
    When executing query:
      """
      WITH "Bosse" as x
      MATCH (person:Person)
      WHERE EXISTS {
       WITH "Ozzy" AS x
       MATCH (person)-[:HAS_DOG]->(d:Dog)
       WHERE d.name = x
       RETURN person
      }
      RETURN person.name
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Exists subquery with updating procedure should fail
    Given any graph
    When executing query:
      """
      MATCH (n) WHERE EXISTS {
        CALL db.createLabel("CAT")
      }
      RETURN n
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Full existential subquery with aggregation
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
      MATCH (n) WHERE EXISTS {
        MATCH (n)-->(m)
        WITH n, count(*) AS numConnections
        WHERE numConnections = 3
        RETURN true
      }
      RETURN n
      """
    Then the result should be, in any order:
      | n             |
      | (:A {prop:1}) |
    And no side effects

  Scenario: Full existential subquery with aggregation comparison
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
      MATCH (n) WHERE EXISTS {
        MATCH (n)-->(m)
        WITH n, count(*) >= 3 AS numConnections
        WHERE numConnections
        RETURN true
      }
      RETURN n
      """
    Then the result should be, in any order:
      | n             |
      | (:A {prop:1}) |
    And no side effects

  Scenario: Exists should allow the shadowing of introduced variables
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
       MATCH (person)-[:HAS_DOG]->(d:Dog)
       WHERE EXISTS {
        WITH "Ozzy" as x
        MATCH (person)-[:HAS_DOG]->(dog1:Dog)
        WHERE dog1.name = x
        WITH "Fido" as x
        MATCH (dog1)-[:HAS_FRIEND]-(dog2:Dog)
        WHERE dog2.name = x
        RETURN person AS person
       }
       RETURN person AS person
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Chris' |
    And no side effects

  Scenario: Exists should allow shadowing of variables not yet introduced in outer scope
    Given any graph
    When executing query:
    """
    WITH EXISTS {
      WITH 1 AS dog
    } AS bool
    MATCH (dog:Dog)
    RETURN dog.name AS name, bool
    """
    Then the result should be, in any order:
    | name    | bool |
    | 'Fido'  | true |
    | 'Bosse' | true |
    | 'Ozzy'  | true |
    And no side effects

  Scenario: Exists should allow non-shadowing variable reuse in complex query
    Given any graph
    When executing query:
    """
    MATCH (dog)<--({canAffordDog:
      EXISTS {
        WITH toBoolean(sum(0)) AS n1, dog AS n2 RETURN 0
      }
     }) RETURN dog.name AS name
    """
    Then the result should be, in any order:
    | name    |
    | 'Bosse' |
    And no side effects

  Scenario: Exists should allow omission of RETURN in more complex queries
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
       MATCH (person)-[:HAS_DOG]->(d:Dog)
       WHERE EXISTS {
        WITH "Ozzy" as x
        MATCH (person)-[:HAS_DOG]->(dog1:Dog)
        WHERE dog1.name = x
        WITH "Fido" as x
        MATCH (dog1)-[:HAS_FRIEND]-(dog2:Dog)
        WHERE dog2.name = x
       }
       RETURN person AS person
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Chris' |
    And no side effects

  Scenario: Exists with inner aggregation and using outer variable should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
       WITH count(*) AS c
       MATCH (person)-[:HAS_DOG]->(d:Dog)
       WHERE d.name = "Ozzy"
       RETURN person
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Chris' |
    And no side effects

  Scenario: Exists with inner aggregation inside another expression and using outer variable should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
       WITH count(*) AS c
       MATCH (person)-[:HAS_DOG]->(d:Dog)
       RETURN count(*) + 1
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Alice' |
      | 'Bosse' |
      | 'Chris' |
    And no side effects

  Scenario: Full Exists without inner where clause
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
       MATCH (person)-[:HAS_DOG]->(dog:Dog) RETURN person
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
      | 'Chris' |
    And no side effects

  Scenario: Exists without where clause but with predicate on outer match should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person {name:'Bosse'})
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
    And no side effects

  Scenario: Exists subquery with inner pattern that doesn't exist should be false
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
        MATCH (person)-[:HAS_HOUSE]->(:House)
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name |
    And no side effects

  Scenario: Exists after optional match without where clause should work
    Given any graph
    When executing query:
      """
      OPTIONAL MATCH (person:Person {name:'Bosse'})
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
    And no side effects

  Scenario: Exists after empty optional match without where clause should work
    Given any graph
    When executing query:
      """
      OPTIONAL MATCH (person:Person {name:'Charlie'}) // Charlie does not exist
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name |
      | null |
    And no side effects

  Scenario: Unfulfilled exists after optional match without where clause should work
    Given any graph
    When executing query:
      """
      OPTIONAL MATCH (person:Person {name:'Alice'})
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog) // Alice doesn't have any dogs
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name |
      | null |
    And no side effects

  Scenario: Exists after simple WITH without where clause should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person {name:'Bosse'})
      WITH person
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
    And no side effects

  Scenario: Full Exists after simple WITH without inner where clause should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person {name:'Bosse'})
      WITH person
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog) RETURN person
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
    And no side effects

  Scenario: Exists after selective WITH without where clause should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person {name:'Bosse'}), (p:Person)
      WITH person
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
      | 'Bosse' |
      | 'Bosse' |
    And no side effects

  Scenario: Exists after renaming WITH without where clause should work
    Given any graph
    When executing query:
      """
      MATCH (p:Person {name:'Bosse'})
      WITH p AS person
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
    And no side effects

  Scenario: Exists after WITH DISTINCT without where clause should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person {name:'Bosse'}), (p:Person)
      WITH DISTINCT person
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
    And no side effects

  Scenario: Exists in MATCH in horizon without where clause should work
    Given any graph
    When executing query:
      """
      MATCH (dog:Dog)
      WITH 1 AS ignore
      MATCH (person:Person {name:'Bosse'})
      WITH person
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
      | 'Bosse' |
      | 'Bosse' |
    And no side effects

  Scenario: Double Exists without where clause should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person {name:'Bosse'})
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
      }
      AND
      EXISTS {
        MATCH (dog:Dog {name: 'Ozzy'})
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
    And no side effects

  Scenario: Double Exists after WITH without where clause should work
    Given any graph
    When executing query:
      """
      MATCH (p:Person {name:'Bosse'})
      WITH p AS person
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
      }
      AND
      EXISTS {
        MATCH (dog:Dog {name: 'Ozzy'})
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
    And no side effects

  Scenario: Double Exists (no result) without where clause should return no results
    Given any graph
    When executing query:
      """
      MATCH (person:Person {name:'Bosse'})
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
      }
      AND
      EXISTS {
        MATCH (dog:Dog {name: 'Jacob'}) // Jacob does not exist
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name |
    And no side effects

  Scenario: Double Exists after WITH (no result) without where clause should return no results
    Given any graph
    When executing query:
      """
      MATCH (p:Person {name:'Bosse'})
      WITH p AS person
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
      }
      AND
      EXISTS {
        MATCH (dog:Dog {name: 'Jacob'}) // Jacob does not exist
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name |
    And no side effects

  Scenario: Exists exists subquery with predicate should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog :Dog)
        WHERE person.name = dog.name
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
    And no side effects

  Scenario: Full Exists subquery with predicate should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog :Dog)
        WHERE person.name = dog.name
        RETURN person
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
    And no side effects

  Scenario: Exists subquery with negative predicate should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog :Dog)
        WHERE NOT person.name = dog.name
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Chris' |
    And no side effects

  Scenario: Exists subquery with multiple predicates should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog)
        WHERE person.name = dog.name AND dog.name = "Bosse"
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
    And no side effects

  Scenario: Exists subquery with multiple predicates 2 should work
    Given any graph
    When executing query:
      """
      MATCH (dog:Dog)
      WHERE EXISTS {
        MATCH (person {name:'Chris'})-[:HAS_DOG]->(dog)
        WHERE dog.name < 'Karo'
      }
      RETURN dog.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Fido'  |
    And no side effects

  Scenario: Exists subquery with multiple predicates 3 should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
        MATCH (person {lastname:'Bobson'})-[:HAS_DOG]->(dog:Dog)
        WHERE person.name = dog.name
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
    And no side effects

  Scenario: Exists subquery with predicates on both outer and inner query should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person {name:'Bosse'})
      WHERE EXISTS {
        MATCH (person {lastname:'Bobson'})-[:HAS_DOG]->(dog)
        WHERE person.name = dog.name
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
    And no side effects

  Scenario: Exists subquery with complex predicates should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
        WHERE person.name = dog.name AND person.lastname = 'Bobson' AND person.id < 2
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
    And no side effects

  Scenario: Exists subquery with complex predicates 2 should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person {id:1})
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
        WHERE person.name = dog.name AND person.lastname = 'Bobson'
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
    And no side effects

  Scenario: Exists subquery with complex predicates 3 should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
        WHERE NOT person.name = dog.name OR person.lastname = 'Bobson'
      } AND person.id = 1
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
    And no side effects

  Scenario: Exists on the right side of an OR should work
    Given any graph
    When executing query:
      """
      MATCH (a:Person), (b:Dog { name:'Ozzy' })
      WHERE a.id = 0
      OR EXISTS {
        MATCH (a)-[:HAS_DOG]->(b)
      }
      RETURN a.name as name
      """
    Then the result should be, in any order:
      | name    |
      | 'Alice' |
      | 'Chris' |
    And no side effects

  Scenario: Exists on the right side of an OR with a NOT should work
    Given any graph
    When executing query:
      """
      MATCH (a:Person), (b:Dog { name:'Ozzy' })
      WHERE a.id = 0
      OR NOT EXISTS {
        MATCH (a)-[:HAS_DOG]->(b)
      }
      RETURN a.name as name
      """
    Then the result should be, in any order:
      | name    |
      | 'Alice' |
      | 'Bosse' |
    And no side effects

  Scenario: Exists on the right side of an XOR should work
    Given an empty graph
    When executing query:
    """
      MATCH (a:Person), (b:Dog { name:'Ozzy' })
      WHERE a.id = 0
      XOR EXISTS {
        MATCH (a)-[:HAS_DOG]->(b)
      }
      RETURN a.name as name
      """
    Then the result should be, in any order:
      | name    |
      | 'Alice' |
      | 'Chris' |
    And no side effects

  Scenario: Exists on the right side of an XOR with a NOT should work
    Given an empty graph
    When executing query:
    """
      MATCH (a:Person), (b:Dog { name:'Ozzy' })
      WHERE a.id = 0
      XOR NOT EXISTS {
        MATCH (a)-[:HAS_DOG]->(b)
      }
      RETURN a.name as name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
    And no side effects

  Scenario: Exists with unrelated inner pattern should work
    Given any graph
    When executing query:
      """
      MATCH (alice:Person {name:'Alice'})
      WHERE EXISTS {
        (person:Person)-[:HAS_DOG]->(dog:Dog)
        WHERE person.name = dog.name
      }
      RETURN alice.name as name
      """
    Then the result should be, in any order:
      | name    |
      | 'Alice' |
    And no side effects

  Scenario: Exists after optional match with simple predicate should work
    Given any graph
    When executing query:
      """
      OPTIONAL MATCH (person:Person)
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
        WHERE person.name = dog.name
      }
      RETURN person.name as name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
    And no side effects

  Scenario: Exists after empty optional match with simple predicate should work
    Given any graph
    When executing query:
      """
      OPTIONAL MATCH (person:Person {name:'Charlie'}) // Charlie does not exist
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
        WHERE person.name = dog.name
      }
      RETURN person.name as name
      """
    Then the result should be, in any order:
      | name |
      | null |
    And no side effects

  Scenario: Unfulfilled Exists after optional match with simple predicate should work
    Given any graph
    When executing query:
      """
      OPTIONAL MATCH (person:Person {name:'Chris'})
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
        WHERE person.name = dog.name // neither of Chris dogs are named Chris
      }
      RETURN person.name as name
      """
    Then the result should be, in any order:
      | name |
      | null |
    And no side effects

  Scenario: Exists after simple WITH with simple predicate should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WITH person
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
        WHERE person.name = dog.name
      }
      RETURN person.name as name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
    And no side effects

  Scenario: Exists after selective WITH with simple predicate should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person), (p:Person)
      WITH person
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
        WHERE person.name = dog.name
      }
      RETURN person.name as name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
      | 'Bosse' |
      | 'Bosse' |
    And no side effects

  Scenario: Exists after rename WITH with simple predicate should work
    Given any graph
    When executing query:
      """
      MATCH (p:Person)
      WITH p AS person
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
        WHERE person.name = dog.name
      }
      RETURN person.name as name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
    And no side effects

  Scenario: Exists after additive WITH with simple predicate should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WITH person, 1 AS ignore
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
        WHERE person.name = dog.name
      }
      RETURN person.name as name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
    And no side effects

  Scenario: Exists after WITH DISTINCT with simple predicate should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person),(p:Person)
      WITH DISTINCT person
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
        WHERE person.name = dog.name
      }
      RETURN person.name as name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
    And no side effects

  Scenario: Exists should handle relationship predicate
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
          MATCH (person)-[h:HAS_DOG]->(dog:Dog)
          WHERE h.since < 2016
      }
      RETURN person.name as name
      """
    Then the result should be, in any order:
      | name    |
      | 'Chris' |
    And no side effects

  Scenario: Exists should handle relationship predicate linking inner and outer relationship
    Given any graph
    When executing query:
      """
      MATCH (person:Person)-[r]->()
      WHERE EXISTS {
          MATCH ()-[h:HAS_DOG]->(dog :Dog {name:'Bosse'})
          WHERE h.since = r.since
      }
      RETURN person.name as name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
    And no side effects

  Scenario: Exists should handle more complex relationship predicate
    Given any graph
    When executing query:
      """
      MATCH (adog:Dog {name:'Ozzy'})
      WITH adog
      MATCH ()-[r]->()
      WHERE EXISTS {
          MATCH (person)-[h:HAS_DOG]->(adog)
          WHERE id(h) = id(r)
      }
      RETURN adog.name as name
      """
    Then the result should be, in any order:
      | name   |
      | 'Ozzy' |
    And no side effects

  Scenario: Simple Exists should work even if MATCH is omitted
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
        (person)-[:HAS_DOG]->(dog:Dog)
        WHERE person.name = dog.name
      }
      RETURN person.name as name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
    And no side effects

  Scenario: Simple Exists with node match should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
        MATCH (person)
        WHERE person.name = 'Chris'
      }
      RETURN person.name as name
      """
    Then the result should be, in any order:
      | name    |
      | 'Chris' |
    And no side effects

  Scenario: Simple Exists with node match and predicate should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
        MATCH (person {id:2})
        WHERE person.name = 'Chris'
      }
      RETURN person.name as name
      """
    Then the result should be, in any order:
      | name    |
      | 'Chris' |
    And no side effects

  Scenario: Simple Exists with node match that will return false should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
        MATCH (person {id:3})
        WHERE person.name = 'Chris'
      }
      RETURN person.name as name
      """
    Then the result should be, in any order:
      | name    |
    And no side effects

  Scenario: Exists with Nesting should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
        WHERE EXISTS {
          MATCH (dog)
          WHERE dog.name = 'Ozzy'
        }
      }
      RETURN person.name as name
      """
    Then the result should be, in any order:
      | name    |
      | 'Chris' |
    And no side effects

  Scenario: Exists with nested Exists should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
        WHERE EXISTS {
          MATCH (dog)<-[]-()
          WHERE dog.name = 'Ozzy'
        }
      }
      RETURN person.name as name
      """
    Then the result should be, in any order:
      | name    |
      | 'Chris' |
    And no side effects

  Scenario: Exists should handle several layers of nesting
    Given any graph
    When executing query:
      """
      MATCH (person:Person)-[]->()
      WHERE EXISTS {
        MATCH (person)
        WHERE person.id > 0 AND EXISTS {
          MATCH (person)-[:HAS_DOG]->(dog:Dog)
          WHERE EXISTS {
           MATCH (dog)
           WHERE dog.name = 'Ozzy'
          }
        }
      }
      RETURN person.name as name
      """
    Then the result should be, in any order:
      | name    |
      | 'Chris' |
      | 'Chris' |
    And no side effects

  Scenario: Not Exists should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE NOT EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog)
      }
      RETURN person.name as name
      """
    Then the result should be, in any order:
      | name    |
      | 'Alice' |
    And no side effects

  Scenario: Where Not Exists should work
    Given any graph
    When executing query:
      """
      WITH 1 AS x
      WHERE NOT EXISTS {
        MATCH (:Badger)
      }
      RETURN x
      """
    Then the result should be, in any order:
      | x |
      | 1 |
    And no side effects

  Scenario: Where Not Exists with multiple WITH should work
    Given any graph
    When executing query:
      """
      WITH 1 AS x
      WITH x, 2 as y
      WHERE NOT EXISTS {
        MATCH (:Badger)
      }
      RETURN x
      """
    Then the result should be, in any order:
      | x |
      | 1 |
    And no side effects

  Scenario: Where Not Exists with WITH should work
    Given any graph
    When executing query:
      """
      WITH 1 AS x
      WHERE NOT NOT EXISTS {
        MATCH ()
      }
      RETURN x
      """
    Then the result should be, in any order:
      | x |
      | 1 |
    And no side effects

  Scenario: Where Not Exists with OR should work
    Given any graph
    When executing query:
      """
      WITH 1 AS x
      WHERE x = 1 OR NOT EXISTS {
      MATCH (:Badger)
      }
      RETURN x
      """
    Then the result should be, in any order:
      | x |
      | 1 |
    And no side effects

  Scenario: Where Not pattern predicate Exists with OR should work
    Given any graph
    When executing query:
      """
      MATCH (n)
      WITH 1 AS x, n AS n
      WHERE x = 1 OR NOT exists((n)-->(n))
      RETURN x
      LIMIT 1
      """
    Then the result should be, in any order:
      | x |
      | 1 |
    And no side effects

  Scenario: Where EXISTS WITH OR and horizon should work
    Given any graph
    When executing query:
      """
      WITH 1 AS x
      WHERE x = 1 OR EXISTS {
      MATCH (:Badger)
      }
      RETURN x
      """
    Then the result should be, in any order:
      | x |
      | 1 |
    And no side effects

  Scenario: WHERE EXISTS or WHERE NOT EXISTS WITH OR and horizon should work
    Given any graph
    When executing query:
      """
      WITH 1 AS x
      WHERE x = 1 OR EXISTS {
      MATCH (:Badger)
      } OR NOT EXISTS {
       MATCH (:Snake)
      }
      RETURN x
      """
    Then the result should be, in any order:
      | x |
      | 1 |
    And no side effects

  Scenario: WHERE NOT EXISTS WITH AND and horizon should work
    Given any graph
    When executing query:
      """
      WITH 1 AS x
      WHERE x = 1 AND NOT EXISTS {
      MATCH (:Badger)
      }
      RETURN x
      """
    Then the result should be, in any order:
      | x |
      | 1 |
    And no side effects

  Scenario: WHERE NOT EXISTS WITH XOR and horizon should work
    Given an empty graph
    When executing query:
      """
      WITH 1 AS x
      WHERE x = 42 XOR NOT EXISTS {
      MATCH (:Badger)
      }
      RETURN x
      """
    Then the result should be, in any order:
      | x |
      | 1 |
    And no side effects


  Scenario: WHERE NOT EXISTS WITH OR, AND and horizon should work
    Given any graph
    When executing query:
      """
      WITH 1 AS x
      WHERE x = 1 OR NOT EXISTS {
      MATCH (:Badger)
      } AND x < 64
      RETURN x
      """
    Then the result should be, in any order:
      | x |
      | 1 |
    And no side effects

  Scenario: WHERE NOT EXISTS WITH OR and multiple horizons should work
    Given any graph
    When executing query:
      """
      WITH 1 AS x
      WITH 2 AS y, x
      WHERE x = 1 OR NOT EXISTS {
      MATCH (:Badger)
      } OR y < 64
      RETURN x
      """
    Then the result should be, in any order:
      | x |
      | 1 |
    And no side effects

  Scenario: NOT EXISTS with single node should work should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE NOT EXISTS {
        MATCH (person)
        WHERE person.name = 'Alice'
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
      | 'Chris' |
    And no side effects

  Scenario: Nesting with NOT EXISTS should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
        WHERE NOT EXISTS {
          MATCH (dog)
          WHERE dog.name = 'Bosse'
        }
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Chris' |
    And no side effects

  Scenario: Multiple patterns in outer MATCH should be supported
    Given any graph
    When executing query:
      """
      MATCH (person:Person), (dog:Dog)
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog)
        WHERE NOT EXISTS {
          MATCH (dog)
          WHERE dog.name = 'Bosse'
        }
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Chris' |
      | 'Chris' |
    And no side effects

  Scenario: Multiple patterns in inner MATCH should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
       MATCH (person), (car:Car)
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name |
    And no side effects

  Scenario: Multiple patterns in inner MATCH with WHERE clause should be supported
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
       MATCH (person), (person)-[:HAS_DOG]->(dog:Dog)
       WHERE dog.name = "Bosse"
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
    And no side effects

  Scenario: Multiple patterns in inner MATCH without external variables should be supported
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
       MATCH (anything), (allOther)
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Alice' |
      | 'Bosse' |
      | 'Chris' |
    And no side effects

  Scenario: Multiple patterns in inner MATCH should be supported
    Given any graph
    When executing query:
      """
      MATCH (dog:Dog)
      WHERE EXISTS {
       MATCH (person)-[:HAS_DOG]->(dog:Dog), (person)-[:HAS_DOG]->(dog2:Dog)
       WHERE dog.name <> dog2.name
      }
      RETURN dog.name AS name
      """
    Then the result should be, in any order:
      | name   |
      | 'Fido' |
      | 'Ozzy' |
    And no side effects

  Scenario: RETURN in inner MATCH should allow for aliased return
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
       MATCH (person)-[:HAS_DOG]->(dog:Dog)
       RETURN dog.name as dogName
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
      | 'Chris' |
    And no side effects

  Scenario: Inner query with MATCH -> WHERE -> WITH -> WHERE should parse and return results
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
       MATCH (person)-[:HAS_DOG]->(dog:Dog)
       WHERE person.name = 'Chris'
       WITH dog
       WHERE dog.name = 'Ozzy'
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Chris' |
    And no side effects

  Scenario: Full Exists without RETURN should parse and return results
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
       MATCH (person)-[:HAS_DOG]->(dog:Dog)
       WITH dog
       MATCH (dog {name: 'Ozzy'})
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Chris' |
    And no side effects

  Scenario: Exists inlined in node pattern with label expression on unnamed node should be supported
    Given any graph
    When executing query:
    """
    MATCH (n:Person
    WHERE EXISTS {
      MATCH (n)-[]->(:Dog)
    })
    RETURN n.name AS name
    """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
      | 'Chris' |
    And no side effects

  Scenario: Exists inlined in node pattern with label expression on named node should be supported
    Given any graph
    When executing query:
    """
    MATCH (n:Person
    WHERE EXISTS {
      MATCH (n)-[]->(dog:Dog)
    })
    RETURN n.name AS name
    """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
      | 'Chris' |
    And no side effects

  Scenario: Nested inlined exists in node pattern should be supported
    Given an empty graph
    When executing query:
    """
    MATCH (a
      WHERE EXISTS {
        MATCH (n WHERE n.name = a.name)-[r:HAS_DOG]->()
      }
    )
    RETURN a.name AS name
    """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
      | 'Bosse' |
      | 'Chris' |
    And no side effects

  Scenario: Nested inlined XOR between exists and other predicate in node pattern should be supported
    Given an empty graph
    When executing query:
    """
    MATCH (n:Person)
    WHERE EXISTS {
      MATCH (n WHERE EXISTS { MATCH (n)-[r]->() } XOR true)
    }
    RETURN n.name AS name
    """
    Then the result should be, in any order:
      | name    |
      | 'Alice' |
    And no side effects

  Scenario: EXISTS in WHERE with ORDER BY
      Given an empty graph
      When executing query:
    """
    MATCH (n:Person)
    WHERE EXISTS {
     MATCH (n)-[:HAS_DOG]->(m)
     RETURN m ORDER BY m.name
    }
    RETURN n.name AS name
    """
      Then the result should be, in any order:
        | name    |
        | 'Bosse' |
        | 'Chris' |
      And no side effects

  Scenario: EXISTS in RETURN with ORDER BY
      Given an empty graph
      When executing query:
    """
    MATCH (n:Person)
    RETURN EXISTS {
     MATCH (n)-[:HAS_DOG]->(m)
     RETURN m ORDER BY m.name
    } AS hasDog
    """
      Then the result should be, in any order:
        | hasDog |
        | false  |
        | true   |
        | true   |
      And no side effects

    Scenario: EXISTS in WHERE with SKIP
      Given an empty graph
      When executing query:
    """
    MATCH (n:Person)
    WHERE EXISTS {
     MATCH (n)-[:HAS_DOG]->(m)
     RETURN m SKIP 1
    }
    RETURN n.name AS name
    """
      Then the result should be, in any order:
        | name    |
        | 'Chris' |
      And no side effects

  Scenario: EXISTS in RETURN with SKIP
      Given an empty graph
      When executing query:
    """
    MATCH (n:Person)
    RETURN EXISTS {
     MATCH (n)-[:HAS_DOG]->(m)
     RETURN m SKIP 1
    } AS hasDogs
    """
      Then the result should be, in any order:
        | hasDogs |
        | false   |
        | false   |
        | true    |
      And no side effects

  Scenario: EXISTS in WHERE with LIMIT
      Given an empty graph
      When executing query:
    """
    MATCH (n:Person)
    WHERE EXISTS {
     MATCH (n)-[:HAS_DOGS]->(m)
     RETURN m LIMIT 0
    }
    RETURN n.name AS name
    """
      Then the result should be, in any order:
        | name    |
      And no side effects

  Scenario: EXISTS in RETURN with LIMIT
      Given an empty graph
      When executing query:
    """
    MATCH (n:Person)
    RETURN EXISTS {
     MATCH (n)-[:HAS_DOG]->(m)
     RETURN m LIMIT 0
    } AS hasDog
    """
      Then the result should be, in any order:
        | hasDog |
        | false  |
        | false  |
        | false  |

      And no side effects

  Scenario: EXISTS in WHERE with ORDER BY, SKIP and LIMIT
      Given an empty graph
      When executing query:
    """
    MATCH (n:Person)
    WHERE EXISTS {
     MATCH (n)-[:HAS_DOG]->(m)
     RETURN m ORDER BY m.name SKIP 1 LIMIT 1
    }
    RETURN n.name AS name
    """
      Then the result should be, in any order:
        | name    |
        | 'Chris' |
      And no side effects

  Scenario: EXISTS in RETURN with ORDER BY, SKIP and LIMIT
      Given an empty graph
      When executing query:
    """
    MATCH (n:Person)
    RETURN EXISTS {
     MATCH (n)-[:HAS_DOG]->(m)
     RETURN m ORDER BY m.name SKIP 1 LIMIT 1
    } AS hasDogs
    """
      Then the result should be, in any order:
        | hasDogs |
        | false   |
        | false   |
        | true    |
      And no side effects

  Scenario: EXISTS in WHERE with DISTINCT
      Given an empty graph
      When executing query:
    """
    MATCH (n:Person)
    WHERE EXISTS {
     MATCH (n)-[:HAS_DOG]->(m)
     RETURN DISTINCT m
    }
    RETURN n.name AS name
    """
      Then the result should be, in any order:
        | name    |
        | 'Bosse' |
        | 'Chris' |
      And no side effects

  Scenario: EXISTS in RETURN with DISTINCT
      Given an empty graph
      When executing query:
    """
    MATCH (n:Person)
    RETURN EXISTS {
     MATCH (n)-[:HAS_DOG]->(m)
     RETURN DISTINCT m
    } AS hasDog
    """
      Then the result should be, in any order:
        | hasDog |
        | false  |
        | true   |
        | true   |
      And no side effects

  Scenario: Exists function inlined in node pattern with label expression should be supported
    Given any graph
    When executing query:
    """
    MATCH (n:Person
    WHERE exists((n)-[]->(:Dog)))
    RETURN n.name AS name
    """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
      | 'Chris' |
    And no side effects

  Scenario: Exists inlined in relationship pattern with label expression on unnamed node should be supported
    Given any graph
    When executing query:
    """
    MATCH (n:Person)-[r
    WHERE EXISTS {
      MATCH (n)-[]->(:Dog)
    }]->(m)
    RETURN n.name AS name
    """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
      | 'Chris' |
      | 'Chris' |
    And no side effects

  Scenario: Exists inlined in relationship pattern with label expression on named node should be supported
    Given any graph
    When executing query:
    """
    MATCH (n:Person)-[r
    WHERE EXISTS {
      MATCH (n)-[]->(dog:Dog)
    }]->(m)
    RETURN n.name AS name
    """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
      | 'Chris' |
      | 'Chris' |
    And no side effects

  Scenario: Nested inlined exists in relationship pattern should be supported
    Given an empty graph
    When executing query:
    """
    MATCH (a)-[
      WHERE EXISTS {
        MATCH (n:Person)-[r WHERE n.name = a.name]->()
      }
    ]->()
    RETURN a.name AS name
    """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
      | 'Chris' |
      | 'Chris' |
    And no side effects

  Scenario: Nested inlined XOR between exists and other predicate in relationship pattern should be supported
    Given an empty graph
    When executing query:
    """
    MATCH (n:Person)
    WHERE EXISTS {
      MATCH (n)-[WHERE EXISTS { MATCH (n)-[r]->() } XOR true]->()
    }
    RETURN n.name AS name
    """
    Then the result should be, in any order:
      | name    |
    And no side effects

  Scenario: Exists function inlined in relationship pattern with label expression should be supported
    Given any graph
    When executing query:
    """
    MATCH (n:Person)-[r
    WHERE exists((n)-[]->(:Dog))]->(m)
    RETURN n.name AS name
    """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
      | 'Chris' |
      | 'Chris' |
    And no side effects

  Scenario: Exists with create should fail with syntax error
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
       CREATE (person)-[:HAS_DOG]->(:Dog)
      }
      RETURN person.name
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Exists with set should fail with syntax error
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
       SET person.name = "Karen"
      }
      RETURN person.name
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Exists with delete should fail with syntax error
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
        DETACH DELETE dog
      }
      RETURN person.name
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Exists with merge should fail with syntax error
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
        MATCH (person)
        MERGE (person)-[:HAS_DOG]->(Dog { name: "Pluto" })
      }
      RETURN person.name
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Exists with FINISH should fail with syntax error
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(Dog { name: "Pluto" })
        FINISH
      }
      RETURN person.name
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Exists can be nested inside another exists
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
         MATCH (person)-[:HAS_DOG]->(dog:Dog)
         WHERE EXISTS {
           (dog)-[:HAS_FRIEND]->(otherDog:Dog)
         }
         RETURN person
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Chris' |
    And no side effects

  Scenario: Aggregation with exists in horizon of tail should plan
    Given any graph
    When executing query:
      """
      MATCH (p:Person)-[:HAS_DOG]->(d:Dog)
      WITH p, collect(d.name) as names
      WITH p.name as walker
      WHERE EXISTS { MATCH (n) }
      RETURN walker AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
      | 'Chris' |
    And no side effects

  Scenario: Exists inside a WITH should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WITH EXISTS {
       MATCH (person)-[:HAS_DOG]->(:Dog)
      } as foo
      RETURN foo
      """
    Then the result should be, in any order:
      | foo   |
      | false |
      | true  |
      | true  |
    And no side effects

  Scenario: Exists with a Union should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WITH EXISTS {
       MATCH (person)-[:HAS_DOG]->(dog:Dog)
       RETURN dog AS pet
       UNION
       MATCH (person)-[:HAS_CAT]->(cat:Cat)
       RETURN cat AS pet
      } as foo
      RETURN foo
      """
    Then the result should be, in any order:
      | foo   |
      | false |
      | true  |
      | true  |
    And no side effects

  Scenario: Exists with a Union and no Returns should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WITH EXISTS {
       MATCH (person)-[:HAS_DOG]->(dog:Dog)
       UNION
       MATCH (person)-[:HAS_CAT]->(cat:Cat)
      } as foo
      RETURN foo
      """
    Then the result should be, in any order:
      | foo   |
      | false |
      | true  |
      | true  |
    And no side effects

  Scenario: Exists with a Union ALL should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WITH EXISTS {
       MATCH (person)-[:HAS_DOG]->(dog:Dog)
       RETURN dog AS pet
       UNION ALL
       MATCH (person)-[:HAS_CAT]->(cat:Cat)
       RETURN cat AS pet
      } as foo
      RETURN foo
      """
    Then the result should be, in any order:
      | foo   |
      | false |
      | true  |
      | true  |
    And no side effects

  Scenario: Exists with a Union ALL and no returns should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WITH EXISTS {
       MATCH (person)-[:HAS_DOG]->(dog:Dog)
       RETURN dog AS pet
       UNION ALL
       MATCH (person)-[:HAS_CAT]->(cat:Cat)
       RETURN cat AS pet
      } as foo
      RETURN foo
      """
    Then the result should be, in any order:
      | foo   |
      | false |
      | true  |
      | true  |
    And no side effects

  Scenario: Exists as a WITH and with an inside Exists should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WITH EXISTS {
       MATCH (person)-[:HAS_DOG]->(dog:Dog)
       WHERE EXISTS {
         MATCH (dog)-[:HAS_FRIEND]-(otherDog:Dog)
         WHERE dog.name <> otherDog.name
       }
      } as foo
      RETURN foo
      """
    Then the result should be, in any order:
      | foo   |
      | false |
      | false |
      | true  |
    And no side effects

  Scenario: Exists with CALL should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WITH EXISTS {
       MATCH (person)-[:HAS_DOG]->(dog:Dog)
       CALL {
        WITH dog
        MATCH (dog2:Dog)
        RETURN dog2 AS d
        UNION
        WITH dog
        MATCH (dog3:Dog)
        RETURN dog3 AS d
      }
      RETURN d AS dogs
      } as foo
      RETURN foo
      """
    Then the result should be, in any order:
      | foo   |
      | false |
      | true |
      | true  |
    And no side effects

  Scenario: Exists with Function use should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WITH EXISTS {
       MATCH (person)-[:HAS_DOG]->(dog:Dog)
       WHERE reverse(dog.name) = "odiF"
       RETURN person
      } as foo
      RETURN foo
      """
    Then the result should be, in any order:
      | foo   |
      | false |
      | false |
      | true  |
    And no side effects

  Scenario: Exists in RETURN should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      RETURN EXISTS {
       MATCH (person)-[:HAS_DOG]->(:Dog)
      } AS foo
      """
    Then the result should be, in any order:
      | foo   |
      | false |
      | true |
      | true  |
    And no side effects

  Scenario: Exists deep in RETURN should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      RETURN person.name AS name, false OR person.canAffordDog = EXISTS {
       MATCH (person)-[:HAS_DOG]->(:Dog)
      } AS reasonableLifeChoices
      """
    Then the result should be, in any order:
      | name    | reasonableLifeChoices   |
      | 'Alice' | true                    |
      | 'Bosse' | true                    |
      | 'Chris' | false                   |
    And no side effects

  Scenario: Exists is valid as part of an equality check
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE person.canAffordDog = EXISTS {
       MATCH (person)-[:HAS_DOG]->(:Dog)
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Alice' |
      | 'Bosse' |
    And no side effects

  Scenario: Not Exists is valid as part of an equality check
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE person.canAffordDog = (NOT EXISTS {
       MATCH (person)-[:HAS_DOG]->(:Dog)
      })
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Chris' |
    And no side effects

  Scenario: Can use exists expression as a function parameter
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE TOSTRING(EXISTS {
       MATCH (person)-[:HAS_DOG]->(:Dog)
      }) = "true"
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
      | 'Chris' |
    And no side effects

  Scenario: Can set a property to the value of an exists expression
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      SET person.hasDog = EXISTS {
       MATCH (person)-[:HAS_DOG]->(:Dog)
      }
      RETURN person.hasDog
      """
    Then the result should be, in any order:
      | person.hasDog |
      | false         |
      | true          |
      | true          |
    And the side effects should be:
      | +properties | 3 |

  Scenario: Can set two properties to the value of an exists expression
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      SET person.hasDog = EXISTS {
       MATCH (person)-[:HAS_DOG]->(:Dog)
      }, person.hasCat = EXISTS {
       MATCH (person)-[:HAS_CAT]->(:Cat)
      }
      RETURN person.hasDog, person.hasCat
      """
    Then the result should be, in any order:
      | person.hasDog | person.hasCat |
      | false         | false         |
      | true          | false         |
      | true          | false         |
    And the side effects should be:
      | +properties | 6 |

  Scenario: Can use exists as part of a create
    Given any graph
    When executing query:
      """
      CREATE (badger:Badger{isAlive: EXISTS {
        MATCH (person)-[:HAS_DOG]->(:Dog)
      }})
      RETURN badger.isAlive
      """
    Then the result should be, in any order:
      | badger.isAlive |
      | true           |
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 1 |
      | +properties | 1 |

  Scenario: Ensure we don't leak variables to the outside
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
        MATCH (person)-[:HAS_DOG]->(dog:Dog)
        WHERE person.name = dog.name
      }
      RETURN person.name, dog.name
      """
    Then a SyntaxError should be raised at compile time: UndefinedVariable

  Scenario: Should support variable length pattern
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
        MATCH (person)-[*]->(dog)
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
      | 'Chris' |
    And no side effects

  Scenario: Should handle scoping and dependencies properly when EXISTS is in horizon
    Given any graph
    When executing query:
      """
      MATCH (adog:Dog {name:'Ozzy'})
      WITH adog
      MATCH (person:Person)
      WHERE EXISTS {
          MATCH (person)-[:HAS_DOG]->(adog)
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Chris' |
    And no side effects

  Scenario: Exists with a union made of RETURNs should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
          RETURN 1 as a
          UNION
          RETURN 2 as a
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Alice' |
      | 'Bosse' |
      | 'Chris' |
    And no side effects

  Scenario: Exists with a RETURNING case should work
    Given any graph
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
          RETURN CASE
             WHEN true THEN 1
             ELSE 2
          END
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Alice' |
      | 'Bosse' |
      | 'Chris' |
    And no side effects

  Scenario: Reuse of variable after exists should not affect result
    Given any graph
    When executing query:
      """
      MATCH (dog:Dog {name:'Bosse'})

      // Since Bosse's owner doesn't have other dogs, person should not be null
      OPTIONAL MATCH (person:Person)-[:HAS_DOG]->(dog)
      WHERE NOT EXISTS {
         MATCH (person)-[:HAS_DOG]->(d:Dog)
          WHERE NOT d = dog
      }

      // since person isn't NULL the result should be 2
      WITH CASE WHEN person IS NULL THEN 1 ELSE 2 END AS person
      RETURN person
      """
    Then the result should be, in any order:
      | person |
      | 2      |
    And no side effects

  Scenario: Inner where clause should consider outer variables
    Given an empty graph
    And having executed:
      """
      CREATE (:Node1 {prop: 1}) -[:REL1]-> (:Node2 {prop: 2}) -[:REL2]-> (:Node2 {prop: 3})
      """
    When executing query:
      """
      MATCH (n1 :Node1) -[:REL1]-> (n2 :Node2) -[:REL2]-> (n3)
       WHERE NOT EXISTS {
        MATCH (n4 :Node1)
        WHERE (n4) -[:REL1]-> (n3)
      }
      RETURN n3.prop
      """
    Then the result should be, in any order:
      | n3.prop |
      | 3       |
    And no side effects

  Scenario: Inner where clause should consider outer variables with property
    Given an empty graph
    And having executed:
      """
      CREATE (:Node1 {prop: 1}) -[:REL1]-> (:Node2 {prop: 2}) -[:REL2]-> (:Node2 {prop: 3})
      """
    When executing query:
      """
      MATCH (n1 :Node1) -[:REL1]-> (n2 :Node2) -[:REL2]-> (n3)
       WHERE EXISTS {
        MATCH (n4 :Node2) -[:REL2]-> (n3)
        WHERE n3.prop = 3
      }
      RETURN n3.prop
      """
    Then the result should be, in any order:
      | n3.prop |
      | 3       |
    And no side effects

  Scenario: Recursive inner where clause should consider outer variables
    Given an empty graph
    And having executed:
      """
      CREATE (:Node) -[:REL1]-> (:Node) -[:REL1]-> (n3:Node {prop: 3})
      CREATE (c1:Node) -[:REL2]-> (n3)
      CREATE (c2:Node) -[:REL3]-> (n3), (c2) -[:REL3]-> (c1)
      """
    When executing query:
      """
      MATCH (n1 :Node) -[:REL1]-> (n2 :Node) -[:REL1]-> (n3)
       WHERE EXISTS {
        MATCH (c1 :Node)
        WHERE (c1) -[:REL2]-> (n3) and EXISTS {
           MATCH (c2: Node)
           WHERE (c2) -[:REL3]-> (n3) and  (c2) -[:REL3]-> (c1)
        }
      }
      RETURN n3.prop
      """
    Then the result should be, in any order:
      | n3.prop |
      | 3       |
    And no side effects

  Scenario: Recursive inner where clause should consider outer relation variables
    Given an empty graph
    And having executed:
      """
      CREATE (:Node {prop: 1}) -[:REL1]-> (n2:Node {prop: 2}) -[:REL1]-> (n3:Node {prop: 3})
      CREATE (c1:Node) <-[:REL2]- (n2)
      CREATE (c1) <-[:REL3]- (n3)
      """
    When executing query:
      """
      MATCH (n1 :Node) -[rel1:REL1]-> (n2 :Node) -[rel2:REL1]-> (n3)
       WHERE EXISTS {
        MATCH () -[rel1]-> () -[:REL2]-> ()
        WHERE EXISTS {
           MATCH () -[rel2]-> () -[:REL3]-> ()
        }
      }
      RETURN n3.prop
      """
    Then the result should be, in any order:
      | n3.prop |
      | 3       |
    And no side effects

  Scenario: Inner clause variables of same hierarchy are not mixed
    Given an empty graph
    And having executed:
      """
      CREATE (:Node {prop: 1}) -[:REL1]-> (n2:Node {prop: 2}) -[:REL2]-> (n3:Node {prop: 3})
      """
    When executing query:
      """
      MATCH (n1 :Node) -[rel1:REL1]-> (n2 :Node) -[rel2:REL2]-> (n3)
       WHERE EXISTS {
        MATCH (temp1) -[:REL1]-> (temp2)
      } AND EXISTS {
        MATCH (temp1) -[:REL2] -> (temp2)
      }
      RETURN n3.prop
      """
    Then the result should be, in any order:
      | n3.prop |
      | 3       |
    And no side effects

  Scenario: Inner clause variables with shadowed variable name
    Given an empty graph
    And having executed:
      """
      CREATE (:Node {prop: 1}) -[:REL1]-> (n2:Node {prop: 2}) -[:REL2]-> (n3:Node {prop: 3})
      """
    When executing query:
      """
      MATCH (n1 :Node) -[:REL1]-> (n2 :Node) -[:REL2]-> (n3)
      WITH n1, n2, n1 as n3
      WHERE NOT EXISTS {
        MATCH (n4 :Node)
        WHERE (n4) -[:REL1]-> (n3)
      }
      RETURN n3.prop
      """
    Then the result should be, in any order:
      | n3.prop |
      | 1       |
    And no side effects

  Scenario: Inner query with UNWIND should work
    Given an empty graph
    And having executed:
      """
      CREATE (:Node {prop: 1}) -[:REL1]-> (n2:Node {prop: 2}) -[:REL2]-> (n3:Node {prop: 3})
      """
    And parameters are:
      | dogNames | ['Fido', 'Bosse'] |
    When executing query:
      """
      MATCH (person:Person)
      WHERE EXISTS {
       UNWIND $dogNames AS name
         MATCH (person)-[:HAS_DOG]->(dog:Dog)
         WHERE dog.name = name
         RETURN name
      }
      RETURN person.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | 'Bosse' |
      | 'Chris' |
    And no side effects

  Scenario: EXISTS subquery in join key
    Given an empty graph
    And having executed:
      """
      CREATE (:A {prop: true})
      CREATE (:A {prop: false})

      CREATE (b0:B {name: 'zero'})

      CREATE (b1:B {name: 'one'})-[:REL]->(:X)

      CREATE (b3:B {name: 'three'})-[:REL]->(:X)
      CREATE (b3)-[:REL]->(:X)
      CREATE (b3)-[:REL]->(:X)
      """
    When executing query:
      """
      MATCH (a:A), (b:B)
      WHERE a.prop = EXISTS { (b)-->(:X) }
      RETURN a.prop, b.name
      """
    Then the result should be, in any order:
      | a.prop | b.name  |
      | false  | 'zero'  |
      | true   | 'one'   |
      | true   | 'three' |

  Scenario: EXISTS subquery as property inside node
    Given an empty graph
    When executing query:
      """
      WITH 0 AS n0
      MATCH ({n1:EXISTS { RETURN 0 AS x } })
      RETURN 2 as result
      """
    Then the result should be, in any order:
      | result |

  Scenario: EXISTS subquery with empty node
    Given an empty graph
    When executing query:
      """
      MERGE (x:A)
      RETURN EXISTS { () } as result
      ORDER BY x
      """
    Then the result should be, in any order:
      | result |
      | true   |

  Scenario: EXISTS subquery with aggregation inside should work
    Given an empty graph
    When executing query:
      """
      MATCH (a)
      RETURN EXISTS {
        MATCH (a)--(b)
        RETURN count(b.foo)
      } AS exists
      """
    Then the result should be, in any order:
      | exists |
      | true   |
      | true   |
      | true   |
      | true   |
      | true   |
      | true   |