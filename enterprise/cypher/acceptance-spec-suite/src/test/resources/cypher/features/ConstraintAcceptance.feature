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

Feature: ConstraintAcceptance

  Scenario: Merge node with prop and label and unique index
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT ON (n:Label) ASSERT n.prop IS UNIQUE
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
    Then the result should be:
      | a.prop |
      | 42     |
    And no side effects

  Scenario: Merge node with prop and label and unique index when no match
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT ON (n:Label) ASSERT n.prop IS UNIQUE
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
    Then the result should be:
      | a.prop |
      | 11     |
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 1 |
      | +properties | 1 |

  Scenario: Merge using unique constraint should update existing node
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT ON (p:Person) ASSERT p.id IS UNIQUE
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
    Then the result should be:
      | a                                                   |
      | (:Person {id: 23, country: 'Sweden', name: 'Emil'}) |
    And the side effects should be:
      | +properties | 1 |

  Scenario: Merge using unique constraint should create missing node
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT ON (p:Person) ASSERT p.id IS UNIQUE
      """
    When executing query:
      """
      MERGE (a:Person {id: 23, country: 'Sweden'})
        ON CREATE SET a.name = 'Emil'
      RETURN a
      """
    Then the result should be:
      | a                                                   |
      | (:Person {id: 23, country: 'Sweden', name: 'Emil'}) |
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 1 |
      | +properties | 3 |

  Scenario: Should match on merge using multiple unique indexes if only found single node for both indexes
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT ON (p:Person) ASSERT p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE CONSTRAINT ON (p:Person) ASSERT p.email IS UNIQUE
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
    Then the result should be:
      | a                                                            |
      | (:Person {id: 23, country: 'Sweden', email: 'smth@neo.com'}) |
    And the side effects should be:
      | +properties | 1 |

  Scenario: Should match on merge using multiple unique indexes and labels if only found single node for both indexes
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT ON (p:Person) ASSERT p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE CONSTRAINT ON (u:User) ASSERT u.email IS UNIQUE
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
    Then the result should be:
      | a                                                                 |
      | (:Person:User {id: 23, country: 'Sweden', email: 'smth@neo.com'}) |
    And the side effects should be:
      | +properties | 1 |

  Scenario: Should match on merge using multiple unique indexes using same key if only found single node for both indexes
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT ON (p:Person) ASSERT p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE CONSTRAINT ON (u:User) ASSERT u.email IS UNIQUE
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
    Then the result should be:
      | a                                          |
      | (:Person:User {id: 23, country: 'Sweden'}) |
    And the side effects should be:
      | +properties | 1 |

  Scenario: Should create on merge using multiple unique indexes if found no nodes
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT ON (p:Person) ASSERT p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE CONSTRAINT ON (p:Person) ASSERT p.email IS UNIQUE
      """
    When executing query:
      """
      MERGE (a:Person {id: 23, email: 'smth@neo.com'})
        ON CREATE SET a.country = 'Sweden'
      RETURN a
      """
    Then the result should be:
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
      CREATE CONSTRAINT ON (p:Person) ASSERT p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE CONSTRAINT ON (u:User) ASSERT u.email IS UNIQUE
      """
    When executing query:
      """
      MERGE (a:Person:User {id: 23, email: 'smth@neo.com'})
        ON CREATE SET a.country = 'Sweden'
      RETURN a
      """
    Then the result should be:
      | a                                                                 |
      | (:Person:User {id: 23, email: 'smth@neo.com', country: 'Sweden'}) |
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 2 |
      | +properties | 3 |

  Scenario: Should fail on merge using multiple unique indexes using same key if found different nodes
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT ON (p:Person) ASSERT p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE CONSTRAINT ON (u:User) ASSERT u.id IS UNIQUE
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

  Scenario: Should fail on merge using multiple unique indexes if found different nodes
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT ON (p:Person) ASSERT p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE CONSTRAINT ON (p:Person) ASSERT p.email IS UNIQUE
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

  Scenario: Should fail on merge using multiple unique indexes if it found a node matching single property only
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT ON (p:Person) ASSERT p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE CONSTRAINT ON (p:Person) ASSERT p.email IS UNIQUE
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

  Scenario: Should fail on merge using multiple unique indexes if it found a node matching single property only flipped order
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT ON (p:Person) ASSERT p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE CONSTRAINT ON (p:Person) ASSERT p.email IS UNIQUE
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

  Scenario: Should fail on merge using multiple unique indexes and labels if found different nodes
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT ON (p:Person) ASSERT p.id IS UNIQUE
      """
    And having executed:
      """
      CREATE CONSTRAINT ON (u:User) ASSERT u.email IS UNIQUE
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

  Scenario: Merge with uniqueness constraints must properly handle multiple labels
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT ON (l:L) ASSERT l.prop IS UNIQUE
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
      CREATE CONSTRAINT ON (p:Person) ASSERT p.id IS UNIQUE
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
      CREATE INDEX ON :Person(name)
      """
    And having executed:
      """
      CREATE CONSTRAINT ON (p:Person) ASSERT p.id IS UNIQUE
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

  Scenario: Works with property repeated in literal map in set
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT ON (p:Person) ASSERT p.ssn IS UNIQUE
      """
    When executing query:
      """
      MERGE (person:Person {ssn: 42})
        ON CREATE SET person = {ssn: 42, name: 'Robert Paulsen'}
      RETURN person.ssn
      """
    Then the result should be:
      | person.ssn |
      | 42         |
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 1 |
      | +properties | 3 |

  Scenario: Works with property in map that gets set
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT ON (p:Person) ASSERT p.ssn IS UNIQUE
      """
    And parameters are:
      | p | {ssn: 42, name: 'Robert Paulsen'} |
    When executing query:
      """
      MERGE (person:Person {ssn: {p}.ssn})
        ON CREATE SET person = {p}
      RETURN person.ssn
      """
    Then the result should be:
      | person.ssn |
      | 42         |
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 1 |
      | +properties | 3 |

  Scenario: Failing when creation would violate constraint
    Given an empty graph
    And having executed:
      """
      CREATE CONSTRAINT ON (p:Person) ASSERT p.id IS UNIQUE
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
