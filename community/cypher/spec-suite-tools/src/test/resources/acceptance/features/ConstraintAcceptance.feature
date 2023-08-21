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

Feature: ConstraintAcceptance

  Scenario: Merge node with prop and label and unique index
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT FOR (n:Label) REQUIRE n.prop IS UNIQUE
      """
    And having executed:
      """
      CREATE (:Label {prop: 42})
      """
    When executing query:
      """
      MERGE (a:Label {prop: 42})
      RETURN a.prop
      """
    Then the result should be, in any order:
      | a.prop |
      | 42     |
    And no side effects

  Scenario: Merge node with prop and label and unique index when no match
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT FOR (n:Label) REQUIRE n.prop IS UNIQUE
      """
    And having executed:
      """
      CREATE (:Label {prop: 42})
      """
    When executing query:
      """
      MERGE (a:Label {prop: 11})
      RETURN a.prop
      """
    Then the result should be, in any order:
      | a.prop |
      | 11     |
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 1 |

  Scenario: Merge node with prop and label and unique index with match and miss
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT FOR (n:Label) REQUIRE n.prop IS UNIQUE
      """
    When executing query:
      """
      CREATE (:Label {prop: 42})
      WITH 0 AS bla
      UNWIND [41, 42] AS x
      MERGE (a:Label {prop: x})
      RETURN a.prop
      """
    Then the result should be, in any order:
      | a.prop |
      | 41     |
      | 42     |
    And the side effects should be:
      | +nodes      | 2 |
      | +properties | 2 |
      | +labels     | 1 |

  Scenario: Merge using unique constraint should update existing node
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE (:Person {id: 23, country: 'Sweden'})
      """
    When executing query:
      """
      MERGE (a:Person {id: 23, country: 'Sweden'})
        ON MATCH SET a.name = 'Emil'
      RETURN a
      """
    Then the result should be, in any order:
      | a                                                   |
      | (:Person {id: 23, country: 'Sweden', name: 'Emil'}) |
    And the side effects should be:
      | +properties | 1 |

  Scenario: Merge using unique constraint should create missing node
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE
      """
    When executing query:
      """
      MERGE (a:Person {id: 23, country: 'Sweden'})
        ON CREATE SET a.name = 'Emil'
      RETURN a
      """
    Then the result should be, in any order:
      | a                                                   |
      | (:Person {id: 23, country: 'Sweden', name: 'Emil'}) |
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 1 |
      | +properties | 3 |

  Scenario: Merge using unique constraint should update existing and create missing nodes
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE (:Person {id: 1, country: 'Sweden'})
      """
    When executing query:
      """
      UNWIND [1, 500] AS id
      MERGE (a:Person {id: id, country: 'Sweden'})
        ON MATCH SET a.name = 'Emil'
        ON CREATE SET a.name = 'New node'
      RETURN a
      """
    Then the result should be, in any order:
      | a                                                        |
      | (:Person {id: 1, country: 'Sweden', name: 'Emil'})       |
      | (:Person {id: 500, country: 'Sweden', name: 'New node'}) |
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 4 |

  Scenario: Should match on merge using multiple unique indexes if only found single node for both indexes
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.email IS UNIQUE
      """
    And having executed:
      """
      CREATE (:Person {id: 23, email: 'smth@neo.com'})
      """
    When executing query:
      """
      MERGE (a:Person {id: 23, email: 'smth@neo.com'})
        ON MATCH SET a.country = 'Sweden'
      RETURN a
      """
    Then the result should be, in any order:
      | a                                                            |
      | (:Person {id: 23, country: 'Sweden', email: 'smth@neo.com'}) |
    And the side effects should be:
      | +properties | 1 |

  Scenario: Should match on merge using multiple unique indexes and labels if only found single node for both indexes
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE CONSTRAINT FOR (u:User) REQUIRE u.email IS UNIQUE
      """
    And having executed:
      """
      CREATE (:Person:User {id: 23, email: 'smth@neo.com'})
      """
    When executing query:
      """
      MERGE (a:Person:User {id: 23, email: 'smth@neo.com'})
        ON MATCH SET a.country = 'Sweden'
      RETURN a
      """
    Then the result should be, in any order:
      | a                                                                 |
      | (:Person:User {id: 23, country: 'Sweden', email: 'smth@neo.com'}) |
    And the side effects should be:
      | +properties | 1 |

  Scenario: Should match on merge using multiple unique indexes using same key if only found single node for both indexes
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE CONSTRAINT FOR (u:User) REQUIRE u.email IS UNIQUE
      """
    And having executed:
      """
      CREATE (:Person:User {id: 23})
      """
    When executing query:
      """
      MERGE (a:Person:User {id: 23})
        ON MATCH SET a.country = 'Sweden'
      RETURN a
      """
    Then the result should be, in any order:
      | a                                          |
      | (:Person:User {id: 23, country: 'Sweden'}) |
    And the side effects should be:
      | +properties | 1 |

  Scenario: Should create on merge using multiple unique indexes if found no nodes
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.email IS UNIQUE
      """
    When executing query:
      """
      MERGE (a:Person {id: 23, email: 'smth@neo.com'})
        ON CREATE SET a.country = 'Sweden'
      RETURN a
      """
    Then the result should be, in any order:
      | a                                                            |
      | (:Person {id: 23, email: 'smth@neo.com', country: 'Sweden'}) |
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 1 |
      | +properties | 3 |

  Scenario: Should create on merge using multiple unique indexes and labels if found no nodes
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE CONSTRAINT FOR (u:User) REQUIRE u.email IS UNIQUE
      """
    When executing query:
      """
      MERGE (a:Person:User {id: 23, email: 'smth@neo.com'})
        ON CREATE SET a.country = 'Sweden'
      RETURN a
      """
    Then the result should be, in any order:
      | a                                                                 |
      | (:Person:User {id: 23, email: 'smth@neo.com', country: 'Sweden'}) |
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 2 |
      | +properties | 3 |

  Scenario: Should match and create on merge using multiple unique indexes if only found single node for both indexes
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.email IS UNIQUE
      """
    And having executed:
      """
      CREATE (:Person {id: 23, email: '23@neo.com'})
      """
    When executing query:
      """
      UNWIND [23, 24] AS id
      MERGE (a:Person {id: id, email: id + '@neo.com'})
        ON MATCH SET a.country = 'Sweden'
        ON CREATE SET a.country = 'Norway'
      RETURN a
      """
    Then the result should be, in any order:
      | a                                                            |
      | (:Person {id: 23, country: 'Sweden', email: '23@neo.com'}) |
      | (:Person {id: 24, country: 'Norway', email: '24@neo.com'}) |
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 4 |

  Scenario: Should match and create on merge using multiple unique indexes and labels if only found single node for both indexes
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE CONSTRAINT FOR (u:User) REQUIRE u.email IS UNIQUE
      """
    And having executed:
      """
      CREATE (:Person:User {id: 23, email: '23@neo.com'})
      """
    When executing query:
      """
      UNWIND [22, 23] AS id
      MERGE (a:Person:User {id: id, email: id + '@neo.com'})
        ON MATCH SET a.country = 'Sweden'
        ON CREATE SET a.country = 'Norway'
      RETURN a
      """
    Then the result should be, in any order:
      | a                                                               |
      | (:Person:User {id: 22, country: 'Norway', email: '22@neo.com'}) |
      | (:Person:User {id: 23, country: 'Sweden', email: '23@neo.com'}) |
    And the side effects should be:
      | +properties | 4 |
      | +nodes      | 1 |

  Scenario: Should match and create on merge using multiple unique indexes using same key if only found single node for both indexes
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE CONSTRAINT FOR (u:User) REQUIRE u.email IS UNIQUE
      """
    And having executed:
      """
      CREATE (:Person:User {id: 23})
      """
    When executing query:
      """
      UNWIND [23, 24] AS id
      MERGE (a:Person:User {id: id})
        ON MATCH SET a.country = 'Sweden'
        ON CREATE SET a.country = 'Norway'
      RETURN a
      """
    Then the result should be, in any order:
      | a                                          |
      | (:Person:User {id: 23, country: 'Sweden'}) |
      | (:Person:User {id: 24, country: 'Norway'}) |
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 3 |

  Scenario: Should match and create on merge using three unique indexes using same key if only found single node for both indexes
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE CONSTRAINT FOR (u:User) REQUIRE u.email IS UNIQUE
      """
    And having executed:
      """
      CREATE CONSTRAINT FOR (u:Human) REQUIRE u.age IS UNIQUE
      """
    And having executed:
      """
      CREATE (:Person:User:Human {id: 23, email: '23@example', age: 46})
      """
    When executing query:
      """
      UNWIND [23, 24] AS id
      MERGE (a:Person:User:Human {id: id, email: id + '@example', age: id * 2})
        ON MATCH SET a.country = 'Sweden'
        ON CREATE SET a.country = 'Norway'
      RETURN a
      """
    Then the result should be, in any order:
      | a                                                                       |
      | (:Person:User:Human {id: 23, email: '23@example', age: 46, country: 'Sweden'}) |
      | (:Person:User:Human {id: 24, email: '24@example', age: 48, country: 'Norway'}) |
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 5 |

  @allowCustomErrors
  Scenario: Should fail on merge using multiple unique indexes using same key if found different nodes
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE CONSTRAINT FOR (u:User) REQUIRE u.id IS UNIQUE
      """
    And having executed:
      """
      CREATE (:Person {id: 23}), (:User {id: 23})
      """
    When executing query:
      """
      MERGE (a:Person:User {id: 23})
      """
    Then a ConstraintValidationFailed should be raised at runtime: CreateBlockedByConstraint

  @allowCustomErrors
  Scenario: Should fail on merge using multiple unique indexes if found different nodes
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.email IS UNIQUE
      """
    And having executed:
      """
      CREATE (:Person {id: 23}), (:Person {email: 'smth@neo.com'})
      """
    When executing query:
      """
      MERGE (a:Person {id: 23, email: 'smth@neo.com'})
      """
    Then a ConstraintValidationFailed should be raised at runtime: CreateBlockedByConstraint

  @allowCustomErrors
  Scenario: Should fail on merge using multiple unique indexes if it found a node matching single property only
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.email IS UNIQUE
      """
    And having executed:
      """
      CREATE (:Person {id: 23})
      """
    When executing query:
      """
      MERGE (a:Person {id: 23, email: 'smth@neo.com'})
      """
    Then a ConstraintValidationFailed should be raised at runtime: CreateBlockedByConstraint

  @allowCustomErrors
  Scenario: Should fail on merge using multiple unique indexes if it found a node matching single property only flipped order
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.email IS UNIQUE
      """
    And having executed:
      """
      CREATE (:Person {email: 'smth@neo.com'})
      """
    When executing query:
      """
      MERGE (a:Person {id: 23, email: 'smth@neo.com'})
      """
    Then a ConstraintValidationFailed should be raised at runtime: CreateBlockedByConstraint

  @allowCustomErrors
  Scenario: Should fail on merge using multiple unique indexes and labels if found different nodes
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE CONSTRAINT FOR (u:User) REQUIRE u.email IS UNIQUE
      """
    And having executed:
      """
      CREATE (:Person {id: 23}), (:User {email: 'smth@neo.com'})
      """
    When executing query:
      """
      MERGE (a:Person:User {id: 23, email: 'smth@neo.com'})
      """
    Then a ConstraintValidationFailed should be raised at runtime: CreateBlockedByConstraint

  @allowCustomErrors
  Scenario: Merge with uniqueness constraints must properly handle multiple labels
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT FOR (l:L) REQUIRE l.prop IS UNIQUE
      """
    And having executed:
      """
      CREATE (:L {prop: 42})
      """
    When executing query:
      """
      MERGE (:L:B {prop: 42})
      """
    Then a ConstraintValidationFailed should be raised at runtime: CreateBlockedByConstraint

  Scenario: Unrelated nodes with same property should not clash
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE (a:Item {id: 1}),
        (b:Person {id: 1})
      """
    When executing query:
      """
      MERGE (a:Item {id: 1})
      MERGE (b:Person {id: 1})
      """
    Then the result should be empty
    And no side effects

  Scenario: Works fine with index and constraint
    Given an empty graph
    And having executed:
      """
      CREATE INDEX FOR (n:Person) ON (n.name)
      """
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE
      """
    When executing query:
      """
      MERGE (person:Person {name: 'Lasse', id: 42})
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 1 |
      | +properties | 2 |

  Scenario: Works fine with index and constraint and multiple rows
    Given an empty graph
    And having executed:
      """
      CREATE INDEX FOR (n:Person) ON (n.name)
      """
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:User) REQUIRE p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE INDEX FOR (n:Human) ON (n.age)
      """
    And having executed:
      """
      CREATE (:Person:User:Human {name: 'Tom', id: 0, age: 13})
      """
    When executing query:
      """
      UNWIND [0, 1] AS id
      MERGE (:Person:User:Human {name: 'Tom', id: id, age: 13})
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 3 |

  Scenario: Works with property repeated in literal map in set
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.ssn IS UNIQUE
      """
    When executing query:
      """
      MERGE (person:Person {ssn: 42})
        ON CREATE SET person = {ssn: 42, name: 'Robert Paulsen'}
      RETURN person.ssn
      """
    Then the result should be, in any order:
      | person.ssn |
      | 42         |
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 1 |
      | +properties | 2 |

  Scenario: Works with property in map that gets set
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.ssn IS UNIQUE
      """
    And parameters are:
      | p | {ssn: 42, name: 'Robert Paulsen'} |
    When executing query:
      """
      MERGE (person:Person {ssn: $p.ssn})
        ON CREATE SET person = $p
      RETURN person.ssn
      """
    Then the result should be, in any order:
      | person.ssn |
      | 42         |
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 1 |
      | +properties | 2 |

  @allowCustomErrors
  Scenario: Failing when creation would violate constraint
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE (:Person {id: 666})
      """
    When executing query:
      """
      CREATE (a:A)
      MERGE (a)-[:KNOWS]->(b:Person {id: 666})
      """
    Then a ConstraintValidationFailed should be raised at runtime: CreateBlockedByConstraint
