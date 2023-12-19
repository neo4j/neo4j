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

Feature: ExplainAcceptance

  Background:
    Given an empty graph

  Scenario: Explanation of standalone procedure call
    And there exists a procedure test.labels() :: (label :: STRING?):
      | label |
      | 'A'   |
      | 'B'   |
      | 'C'   |
    When executing query:
    """
    EXPLAIN CALL test.labels()
    """
    Then the result should be:
      | label |
    And no side effects

  Scenario: Explanation of in-query procedure call
    And there exists a procedure test.labels() :: (label :: STRING?):
      | label |
      | 'A'   |
      | 'B'   |
      | 'C'   |
    When executing query:
    """
    EXPLAIN
    CALL test.labels() YIELD label
    RETURN *
    """
    Then the result should be:
      | label |
    And no side effects
