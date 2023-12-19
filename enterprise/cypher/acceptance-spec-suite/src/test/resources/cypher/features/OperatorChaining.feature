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

Feature: OperatorChaining

  Scenario: Long chains of integer comparisons
    Given any graph
    When executing query:
      """
      RETURN 1 < 2 < 3 < 4 AS t1,
             1 < 3 < 2 < 4 AS t2,
             1 < 2 < 2 < 4 AS t3,
             1 < 2 <= 2 < 4 AS t4
      """
    Then the result should be:
      | t1   | t2    | t3    | t4   |
      | true | false | false | true |
    And no side effects

  Scenario: Long chains of floating point comparisons
    Given any graph
    When executing query:
      """
      RETURN 1.0 < 2.1 < 3.2 < 4.6 AS t1,
             1.0 < 3.2 < 2.1 < 4.6 AS t2,
             1.0 < 2.1 < 2.1 < 4.6 AS t3,
             1.0 < 2.1 <= 2.1 < 4.6 AS t4
      """
    Then the result should be:
      | t1   | t2    | t3    | t4   |
      | true | false | false | true |
    And no side effects

  Scenario: Long chains of string comparisons
    Given any graph
    When executing query:
      """
      RETURN "a" < "b" < "c" < "d" AS t1,
             "a" < "c" < "b" < "d" AS t2,
             "a" < "b" < "b" < "d" AS t3,
             "a" < "b" <= "b" < "d" AS t4
      """
    Then the result should be:
      | t1   | t2    | t3    | t4   |
      | true | false | false | true |
    And no side effects
