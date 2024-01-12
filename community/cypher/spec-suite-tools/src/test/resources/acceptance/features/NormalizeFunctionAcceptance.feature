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

Feature: NormalizeFunctionAcceptance

  Background:
    Given an empty graph
    And having executed:
    """
    CREATE (a:A {isNormalized: true, prop: "\u00C5ra"})-[r:Rel]->(b:B {isNormalized: false, prop: "\u212Bra"})
    """

  Scenario: Testing Simple Literals
    When executing query:
      """
      MATCH (n) WHERE normalize(n.prop) = "\u00C5ra" RETURN n.isNormalized
      """
    Then the result should be, in any order:
      | n.isNormalized |
      | true           |
      | false          |
    And no side effects

  Scenario: Inside an EXISTS
    When executing query:
      """
      RETURN EXISTS { RETURN normalize("hello") AS normalized } as exists
      """
    Then the result should be, in any order:
      | exists |
      | true   |

    And no side effects

  Scenario: Inside a case statement
    Given an empty graph
    When executing query:
      """
      UNWIND ["\u0051\u0300\u0323", "hello world", "\uFE64"] as value
      RETURN CASE
          WHEN normalize(value, NFC) <> value THEN "Normalizes in NFC"
          WHEN normalize(value, NFD) <> value THEN "Normalizes in NFD"
          WHEN normalize(value, NFKC) <> value THEN "Normalizes in NFKC"
          WHEN normalize(value, NFKD) <> value THEN "Normalizes in NFKD"
          ELSE "Doesn't normalize!"
       END
       AS result
      """
    Then the result should be, in any order:
      | result                |
      | 'Normalizes in NFC'   |
      | 'Doesn\'t normalize!' |
      | 'Normalizes in NFKC'  |

    And no side effects

  Scenario: Test normalization of something that is different in all forms
    Given an empty graph
    When executing query:
      """
      WITH "\u03D3" AS greekUpsilonWithAcuteAndHookSymbol
      RETURN normalize(greekUpsilonWithAcuteAndHookSymbol, NFC) as NFC,
             normalize(greekUpsilonWithAcuteAndHookSymbol, NFD) as NFD,
             normalize(greekUpsilonWithAcuteAndHookSymbol, NFKC) as NFKC,
             normalize(greekUpsilonWithAcuteAndHookSymbol, NFKD) as NFKD
      """
    Then the result should be, in any order:
      | NFC | NFD | NFKC | NFKD |
      | 'ϓ' | 'ϓ' | 'Ύ'  | 'Ύ'  |

    And no side effects