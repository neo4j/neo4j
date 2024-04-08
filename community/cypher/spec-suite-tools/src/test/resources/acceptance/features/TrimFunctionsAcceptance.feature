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
# Please note that this file contains normal spaces (U+0020) and thin space characters (U+2009), but they may
# not be visible in all editors :)
Feature: TrimFunctionsAcceptance

  Background:
    Given an empty graph
    And having executed:
    """
    CREATE (a:A {whiteSpaceFilledString: "   hello world   ", messyString: "xyxyxhelloXYXworldxyXxyxyx"})-[r:Rel]->(b:B {whiteSpaceFilledString: "  hello world    ", messyString: " xhello therex "})
    CREATE (n:BadData {whiteSpaceFilledString: 1234, messyString: 5.7})
    """

  Scenario: Trim functions trimming whitespace strings
    When executing query:
      """
      WITH "   hello world   " AS s
      RETURN trim(s) AS trim,
        ltrim(s) AS ltrim,
        rtrim(s) AS rtrim,
        btrim(s) AS btrim
      """
    Then the result should be, in any order:
      | trim          | ltrim                 | rtrim                 | btrim         |
      | 'hello world' | 'hello world   '   | '   hello world'   | 'hello world' |
    And no side effects

  Scenario: Trim functions trimming other chars strings
    When executing query:
      """
      WITH "xy xyxhelloXYXworldxyXxyx yx" AS s
      RETURN trim(BOTH 'x' FROM s) AS trim,
        ltrim(s, ' xy') AS ltrim,
        rtrim(s, ' xy') AS rtrim,
        btrim(s, ' xy') AS btrim
      """
    Then the result should be, in any order:
      | trim                         | ltrim                    | rtrim                    | btrim              |
      | 'y xyxhelloXYXworldxyXxyx y' | 'helloXYXworldxyXxyx yx' | 'xy xyxhelloXYXworldxyX' | 'helloXYXworldxyX' |
    And no side effects

  Scenario: Trim functions trimming whitespace node property strings
    When executing query:
      """
      MATCH (n:!BadData)
      RETURN trim(n.whiteSpaceFilledString) AS trim,
        ltrim(n.whiteSpaceFilledString) AS ltrim,
        rtrim(n.whiteSpaceFilledString) AS rtrim,
        btrim(n.whiteSpaceFilledString) AS btrim
      """
    Then the result should be, in any order:
      | trim          | ltrim                       | rtrim                 | btrim         |
      | 'hello world' | 'hello world   '         | '   hello world'   | 'hello world' |
      | 'hello world' | 'hello world    '     | '  hello world'       | 'hello world' |
    And no side effects

  Scenario: Trim functions trimming other chars node property strings
    When executing query:
      """
      MATCH (n:!BadData)
      RETURN trim(BOTH 'x' FROM n.messyString) AS trim,
        ltrim(n.messyString, ' xy') AS ltrim,
        rtrim(n.messyString, ' xy') AS rtrim,
        btrim(n.messyString, ' xy') AS btrim
      """
    Then the result should be, in any order:
      | trim                       | ltrim                   | rtrim                   | btrim              |
      | 'yxyxhelloXYXworldxyXxyxy' | 'helloXYXworldxyXxyxyx' | 'xyxyxhelloXYXworldxyX' | 'helloXYXworldxyX' |
      | ' xhello therex '          | 'hello therex '         | ' xhello there'         | 'hello there'      |
    And no side effects

  Scenario: Trim functions with different trim specifications
    When executing query:
      """
      WITH 'xxxhelloxyworldxxx' AS s
      RETURN trim('x' FROM s) AS trim,
        trim(LEADING 'x' FROM s) AS ltrim,
        trim(TRAILING 'x' FROM s) AS rtrim,
        trim(BOTH 'x' FROM s) AS btrim
      """
    Then the result should be, in any order:
      | trim           | ltrim             | rtrim             | btrim          |
      | 'helloxyworld' | 'helloxyworldxxx' | 'xxxhelloxyworld' | 'helloxyworld' |
    And no side effects

  Scenario: Trim functions with trim source from different expressions
    When executing query:
      """
      RETURN
        trim('x' + 'y' + 'x') AS trim1,
        trim('x' FROM 'x' + 'y' + 'x') AS trim2,
        ltrim(rtrim('xyx', 'x'), 'x') AS lrtrim,
        btrim(normalize('xyx'), 'x') AS btrim,
        rtrim(ltrim('x' || 'y' || 'x', 'x'), 'x') AS rltrim
      """
    Then the result should be, in any order:
      | trim1 | trim2 | lrtrim | btrim | rltrim |
      | 'xyx' | 'y'   | 'y'    | 'y'   | 'y'    |
    And no side effects

  Scenario: Trim functions with default handling
    When executing query:
      """
      WITH '   hello world   ' AS s
      RETURN trim(FROM s) AS trim,
        trim(LEADING FROM s) AS ltrim,
        trim(TRAILING FROM s) AS rtrim,
        trim(BOTH FROM s) AS btrim
      """
    Then the result should be, in any order:
      | trim          | ltrim                 | rtrim                 | btrim         |
      | 'hello world' | 'hello world   '   | '   hello world'   | 'hello world' |
    And no side effects

  Scenario: Trim functions with null handling for 1 argument options
    When executing query:
      """
      RETURN trim(null) AS trim,
        ltrim(null) AS ltrim,
        rtrim(null) AS rtrim,
        btrim(null) AS btrim
      """
    Then the result should be, in any order:
      | trim | ltrim | rtrim | btrim |
      | null | null  | null  | null  |
    And no side effects

  Scenario: Trim functions with null handling for trimCharacterString
    When executing query:
      """
      WITH '   hello world   ' AS s
      RETURN trim(null FROM s) AS trim,
        trim(LEADING null FROM s) AS ltrim,
        trim(TRAILING null FROM s) AS rtrim,
        trim(BOTH null FROM s) AS btrim
      """
    Then the result should be, in any order:
      | trim | ltrim | rtrim | btrim |
      | null | null  | null  | null  |
    And no side effects

  Scenario: Trim functions with null handling for input
    When executing query:
      """
      WITH null AS s
      RETURN trim('x' FROM s) AS trim,
        trim(LEADING 'x' FROM s) AS ltrim,
        trim(TRAILING 'x' FROM s) AS rtrim,
        trim(BOTH 'x' FROM s) AS btrim
      """
    Then the result should be, in any order:
      | trim | ltrim | rtrim | btrim |
      | null | null  | null  | null  |
    And no side effects

  Scenario: Trim functions with zero length trim character strings
    When executing query:
      """
      WITH ' hello world ' AS s
      RETURN ltrim(s, '') AS ltrim,
        rtrim(s, '') AS rtrim,
        btrim(s, '') AS btrim
      """
    Then the result should be, in any order:
      | ltrim            | rtrim            | btrim            |
      | ' hello world '  | ' hello world '  | ' hello world '  |

  Scenario: Trim functions with zero length source strings
    When executing query:
      """
      WITH '' AS s
      RETURN ltrim(s, 'abc') AS ltrim,
        rtrim(s, 'abc') AS rtrim,
        btrim(s, 'abc') AS btrim
      """
    Then the result should be, in any order:
      | ltrim | rtrim | btrim |
      | ''    | ''    | ''    |
    And no side effects

  Scenario: Trim functions should fail with multi-char trim string (1)
    When executing query:
      """
      RETURN trim('xy' FROM 'xyhelloxy') AS trim
      """
    Then a ArgumentError should be raised at runtime: *

  Scenario: Trim functions should fail with multi-char trim string (2)
    When executing query:
      """
      RETURN trim(LEADING '✂️' FROM 'xyhelloxy') AS trim
      """
    Then a ArgumentError should be raised at runtime: *

  Scenario: Trim functions should fail with empty-char trim string
    When executing query:
      """
      RETURN trim(TRAILING '' FROM 'xyhelloxy') AS trim
      """
    Then a ArgumentError should be raised at runtime: *

  Scenario: Trim functions should fail with wrong type
    When executing query:
      """
      MATCH (n:BadData)
      RETURN trim(n.whiteSpaceFilledString) AS trim
      """
    Then a TypeError should be raised at runtime: *

  Scenario: Trim functions should fail with wrong type (1)
    When executing query:
      """
      MATCH (n:BadData)
      RETURN ltrim(n.whiteSpaceFilledString) AS trim
      """
    Then a TypeError should be raised at runtime: *

  Scenario: Trim functions should fail with wrong type (2)
    When executing query:
      """
      MATCH (n:BadData)
      RETURN rtrim(n.whiteSpaceFilledString) AS trim
      """
    Then a TypeError should be raised at runtime: *

  Scenario: Trim functions should fail with wrong type (3)
    When executing query:
      """
      MATCH (n:BadData)
      RETURN btrim(n.whiteSpaceFilledString) AS trim
      """
    Then a TypeError should be raised at runtime: *