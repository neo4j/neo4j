/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast.Statement

class WhitespaceParserTest extends JavaccParserAstTestBase[Statement] {

  implicit private val parser: JavaccRule[Statement] = JavaccRule.Statement

  private val whitespaceCharacters =
    Seq(
      // Space Separator
      '\u00A0', '\u2007', '\u202F',

      // Unicode General Category Zp
      '\u2029',

      // Unicode General Category Zs
      '\u0020', '\u1680', '\u2000', '\u2001', '\u2002', '\u2003', '\u2004', '\u2005', '\u2006', '\u2008', '\u2009',
      '\u200A', '\u205F', '\u3000',

      // Unicode General Category Zl (Line Separator)
      '\u2028',

      // Horizontal Tabulation \t
      '\u0009',

      // Line Feed \n
      '\u000A',

      // Vertical Tabulation
      '\u000B',

      // Form Feed \f
      '\u000C',

      // Carriage Return \r
      '\u000D',

      // File Separator
      '\u001C',

      // Group Separator
      '\u001D',

      // Record Separator
      '\u001E',

      // Unit Separator
      '\u001F'
    )

  /**
   * Tests accepted whitespace characters from CIP-44
   */
  for (whitespace <- whitespaceCharacters) {
    test(
      testName =
        s"Accept the whitespace unicode character in defined set: \\u" + Integer.valueOf(whitespace).toString
    ) {
      assert(Character.isWhitespace(whitespace) || Character.isSpaceChar(whitespace))
      givesIncludingPositions(
        {
          singleQuery(
            match_(nodePat(name = Some("m"))),
            return_(variableReturnItem("m"))
          )
        },
        s"MATCH$whitespace(m) RETURN m"
      )
    }
  }

  /**
   * According to CIP-44 we accept:
   *  - the whitespace categories: Zs (Space Separator), Zl (Line Separator), Zp (Paragraph Separator), see Character.isSpaceChar()
   *  - additional whitespace separators which seem to be aligned with the Character.isWhitespace definition.
   *
   *  These tests should fail if new unicode characters would be added to these categories.
   */
  for (i <- 1 to 0xffff) {
    if (Character.isWhitespace(i) || Character.isSpaceChar(i)) {
      test(
        testName = f"Accept the whitespace character included in Character.isWhistepsace: \\u$i%04X"
      ) {
        givesIncludingPositions(
          {
            singleQuery(
              match_(nodePat(name = Some("m"))),
              return_(variableReturnItem("m"))
            )
          },
          s"MATCH${i.toChar}(m) RETURN m"
        )
      }
    }
  }

  /**
   * test whitespaces in different locations
   */
  test("  MATCH ( m    ) RETURN m  ") {
    givesIncludingPositions(
      {
        singleQuery(
          match_(nodePat(name = Some("m"))),
          return_(variableReturnItem("m"))
        )
      }
    )
  }
}
