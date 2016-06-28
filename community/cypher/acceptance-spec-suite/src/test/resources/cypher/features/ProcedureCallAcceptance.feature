#
# Copyright (c) 2002-2016 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
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

Feature: ProcedureCallAcceptance

  Background:
    Given an empty graph

  Scenario: Standalone call to procedure without arguments
    And there exists a procedure test.labels() :: (label :: STRING?):
      | label |
      | 'A'   |
      | 'B'   |
      | 'C'   |
    When executing query:
    """
    CALL test.labels()
    """
    Then the result should be, in order:
      | label |
      | 'A'   |
      | 'B'   |
      | 'C'   |

  Scenario: Standalone call to VOID procedure
    And there exists a procedure test.doNothing() :: VOID:
      |
    When executing query:
    """
    CALL test.doNothing()
    """
    Then the result should be empty

  Scenario: Standalone call to VOID procedure without arguments
    And there exists a procedure test.doNothing() :: VOID:
      |
    When executing query:
    """
    CALL test.doNothing
    """
    Then the result should be empty

  Scenario: Standalone call to empty procedure
    And there exists a procedure test.doNothing() :: ():
      |
    When executing query:
    """
    CALL test.doNothing()
    """
    Then the result should be empty

  Scenario: Standalone call to empty procedure without arguments
    And there exists a procedure test.doNothing() :: ():
      |
    When executing query:
    """
    CALL test.doNothing
    """
    Then the result should be empty

  Scenario: Standalone call to procedure with explicit arguments
    And there exists a procedure test.my.proc(name :: STRING?, id :: INTEGER?) :: (city :: STRING?, country_code :: INTEGER?):
      | name     | id | city      | country_code |
      | 'Andres' | 1  | 'Malmö'   | 46           |
      | 'Tobias' | 1  | 'Malmö'   | 46           |
      | 'Mats'   | 1  | 'Malmö'   | 46           |
      | 'Stefan' | 1  | 'Berlin'  | 49           |
      | 'Stefan' | 2  | 'München' | 49           |
      | 'Petra'  | 1  | 'London'  | 44           |
    When executing query:
    """
    CALL test.my.proc('Stefan', 1)
    """
    Then the result should be, in order:
      | city     | country_code |
      | 'Berlin' | 49           |

  @pending
  Scenario: Standalone call to procedure with implicit arguments
    Given this scenario is pending on: decision to change semantics to be in line with the explicit argument form of standalone calls
    And there exists a procedure test.my.proc(name :: STRING?, id :: INTEGER?) :: (city :: STRING?, country_code :: INTEGER?):
      | name     | id | city      | country_code |
      | 'Andres' | 1  | 'Malmö'   | 46           |
      | 'Tobias' | 1  | 'Malmö'   | 46           |
      | 'Mats'   | 1  | 'Malmö'   | 46           |
      | 'Stefan' | 1  | 'Berlin'  | 49           |
      | 'Stefan' | 2  | 'München' | 49           |
      | 'Petra'  | 1  | 'London'  | 44           |
    And parameters are:
      | name | 'Stefan' |
      | id   | 1        |
    When executing query:
    """
    CALL test.my.proc
    """
    Then the result should be, in order:
      | city     | country_code |
      | 'Berlin' | 49           |

  Scenario: Standalone call to procedure with argument of type NUMBER accepts value of type INTEGER
    And there exists a procedure test.my.proc(in :: NUMBER?) :: (out :: STRING?):
      | in   | out           |
      | 42   | 'wisdom'      |
      | 42.3 | 'about right' |
    When executing query:
    """
    CALL test.my.proc(42)
    """
    Then the result should be, in order:
      | out      |
      | 'wisdom' |

  Scenario: Standalone call to procedure with argument of type FLOAT accepts value of type INTEGER
    And there exists a procedure test.my.proc(in :: FLOAT?) :: (out :: STRING?):
      | in   | out            |
      | 42.0 | 'close enough' |
    When executing query:
    """
    CALL test.my.proc(42)
    """
    Then the result should be, in order:
      | out            |
      | 'close enough' |

  Scenario: Standalone call to procedure with argument of type NUMBER accepts value of type FLOAT
    And there exists a procedure test.my.proc(in :: NUMBER?) :: (out :: STRING?):
      | in   | out           |
      | 42   | 'wisdom'      |
      | 42.3 | 'about right' |
    When executing query:
    """
    CALL test.my.proc(42.3)
    """
    Then the result should be, in order:
      | out           |
      | 'about right' |

  Scenario: Standalone call to procedure with argument of type INTEGER accepts value of type FLOAT
    And there exists a procedure test.my.proc(in :: INTEGER?) :: (out :: STRING?):
      | in | out            |
      | 42 | 'close enough' |
    When executing query:
    """
    CALL test.my.proc(42.0)
    """
    Then the result should be, in order:
      | out            |
      | 'close enough' |

  Scenario: Standalone call to procedure with null argument
    And there exists a procedure test.my.proc(in :: INTEGER?) :: (out :: STRING?):
      | in   | out   |
      | null | 'nix' |
    When executing query:
    """
    CALL test.my.proc(null)
    """
    Then the result should be, in order:
      | out   |
      | 'nix' |
