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

Feature: BooleanExpression

  Scenario: Combining IN predicates intersected with OR
    Given any graph
    When executing query:
      """
      RETURN (1 IN [1] AND FALSE) OR 1 in [2] AS result
      """
    Then the result should be:
      | result |
      | false  |
    And no side effects

  Scenario: Combining IN predicates intersected with AND
    Given any graph
    When executing query:
      """
      RETURN (1 IN [2] OR TRUE) AND 1 in [1] AS result
      """
    Then the result should be:
      | result |
      | true   |
    And no side effects

