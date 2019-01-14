#
# Copyright (c) 2002-2019 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j Enterprise Edition. The included source
# code can be redistributed and/or modified under the terms of the
# GNU AFFERO GENERAL PUBLIC LICENSE Version 3
# (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
# Commons Clause, as found in the associated LICENSE.txt file.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# Neo4j object code can be licensed independently from the source
# under separate terms from the AGPL. Inquiries can be directed to:
# licensing@neo4j.com
#
# More information is also available at:
# https://neo4j.com/licensing/
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

