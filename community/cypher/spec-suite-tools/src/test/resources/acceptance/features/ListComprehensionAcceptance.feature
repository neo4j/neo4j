#
# Copyright (c) 2002-2018 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

#encoding: utf-8

Feature: ListComprehensionAcceptance
  Background:
    Given an empty graph
  Scenario: Should find all the variables in list comprehension
    When executing query:
      """
      RETURN
        [x IN [1] WHERE x > 0 ] AS res1,
        [x IN [1] WHERE x > 0 ] AS res2,
        [x IN [1]] AS res3
      """
    Then the result should be:
      | res1 | res2 | res3 |
      | [1]  | [1]  | [1]  |
    And no side effects