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

Feature: OptionalMatchAcceptance

  Scenario: Id on null
    Given an empty graph
    And having executed:
      """
      UNWIND range(1,10) AS i CREATE (:L1 {prop:i})<-[:R]-(:L2)
      """
    When executing query:
      """
      MATCH (n1 :L1 {prop: 3}) OPTIONAL MATCH (n2 :L2)<-[r]-(n1) RETURN id(n2), id(r)
      """
    Then the result should be:
      | id(n2) | id(r) |
      | null   | null  |
    And no side effects

  Scenario: type on null
    Given an empty graph
    And having executed:
      """
      UNWIND range(1,10) AS i CREATE (:L1 {prop:i})<-[:R]-(:L2)
      """
    When executing query:
      """
      MATCH (n1 :L1 {prop: 3}) OPTIONAL MATCH (n2 :L2)<-[r]-(n1) RETURN type(r)
      """
    Then the result should be:
      | type(r) |
      | null    |
    And no side effects

  Scenario: optional equality with boolean lists
    Given an empty graph
    And having executed:
      """
      CREATE ({prop: [false]})
      """
    When executing query:
      """
      OPTIONAL MATCH (n {prop: false}) RETURN n
      """
    Then the result should be:
      | n    |
      | null |
    And no side effects

  Scenario: optional match with distinct
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:Y]->(:B)-[:X]->(:B)
      """
    When executing query:
      """
      OPTIONAL MATCH (:A)-[:Y]->(:B)-[:X]->(b:B) RETURN DISTINCT labels(b)
      """
    Then the result should be:
      | labels(b) |
      | ['B']     |
    And no side effects

  Scenario: optional match with OR where clause
    Given an empty graph
    And having executed:
    # Setup: (a)<->(b)->(c)
    """
    CREATE (b {prop: 'b'})-[:REL]->({prop: 'c'})
    CREATE (b)-[:REL]->({prop: 'a'})-[:REL]->(b)
    """
    When executing query:
    """
    MATCH (n1)-->(n2)
    OPTIONAL MATCH (n2)-->(n3)
    WHERE n3 IS NULL OR n3 <> n1
    RETURN n1.prop, n2.prop, n3.prop
    """
    Then the result should be:
      | n1.prop | n2.prop | n3.prop |
      | 'a'     | 'b'     | 'c'     |
      | 'b'     | 'c'     | null    |
      | 'b'     | 'a'     | null    |
    And no side effects
