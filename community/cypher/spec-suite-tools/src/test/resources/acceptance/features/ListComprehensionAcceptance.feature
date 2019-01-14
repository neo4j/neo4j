#
# Copyright (c) 2002-2019 "Neo4j,"
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

  Scenario: Should find all the variables in list comprehension
    Given an empty graph
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

  Scenario: Should handle list comprehension with AND and one NOT
    Given an empty graph
    And having executed:
        """
        CREATE (:Company {nbr: 1, status: 'ACTIVE'})
        CREATE (:Company {nbr: 2, status: 'INACTIVE'})
        CREATE (:Company {nbr: 3, status: 'DELETED'})
        CREATE (:Company {nbr: 4, status: 'X'})
        """
    When executing query:
        """
        MATCH (company :Company)
        WHERE company.status IN ["ACTIVE", "INACTIVE"] AND NOT company.status IN ["INACTIVE", "DELETED"]
        RETURN company.nbr AS nbr
        """
    Then the result should be:
      | nbr |
      | 1   |
    And no side effects

  Scenario: Should handle list comprehension with AND and two NOT
    Given an empty graph
    And having executed:
        """
        CREATE (:Company {nbr: 1, status: 'ACTIVE'})
        CREATE (:Company {nbr: 2, status: 'INACTIVE'})
        CREATE (:Company {nbr: 3, status: 'DELETED'})
        CREATE (:Company {nbr: 4, status: 'X'})
        """
    When executing query:
        """
        MATCH (company :Company)
        WHERE NOT company.status IN ["ACTIVE", "INACTIVE"] AND NOT company.status IN ["INACTIVE", "DELETED"]
        RETURN company.nbr AS nbr
        """
    Then the result should be:
      | nbr |
      | 4   |
    And no side effects

  Scenario: Should handle list comprehension with OR and one NOT
    Given an empty graph
    And having executed:
        """
        CREATE (:Company {nbr: 1, status: 'ACTIVE'})
        CREATE (:Company {nbr: 2, status: 'INACTIVE'})
        CREATE (:Company {nbr: 3, status: 'DELETED'})
        CREATE (:Company {nbr: 4, status: 'X'})
        """
    When executing query:
        """
        MATCH (company :Company)
        WHERE company.status IN ["ACTIVE", "INACTIVE"] OR NOT company.status IN ["INACTIVE", "DELETED"]
        WITH company.nbr AS nbr
        RETURN nbr ORDER BY nbr
        """
    Then the result should be:
      | nbr |
      | 1   |
      | 2   |
      | 4   |
    And no side effects

  Scenario: Should handle list comprehension with OR and two NOT
    Given an empty graph
    And having executed:
        """
        CREATE (:Company {nbr: 1, status: 'ACTIVE'})
        CREATE (:Company {nbr: 2, status: 'INACTIVE'})
        CREATE (:Company {nbr: 3, status: 'DELETED'})
        CREATE (:Company {nbr: 4, status: 'X'})
        """
    When executing query:
        """
        MATCH (company :Company)
        WHERE NOT company.status IN ["ACTIVE", "INACTIVE"] OR NOT company.status IN ["INACTIVE", "DELETED"]
        WITH company.nbr AS nbr
        RETURN nbr ORDER BY nbr
        """
    Then the result should be:
      | nbr |
      | 1   |
      | 3   |
      | 4   |
    And no side effects
