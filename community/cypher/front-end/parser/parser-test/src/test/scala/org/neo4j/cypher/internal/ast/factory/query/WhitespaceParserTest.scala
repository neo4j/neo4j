/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.ast.factory.query

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase

class WhitespaceParserTest extends AstParsingTestBase {

  private val whitespaceCharacters =
    Seq(

      // Unicode General Category Zp
      '\u2029',

      // Unicode General Category Zs
      '\u0020', '\u00A0', '\u1680', '\u2000', '\u2001', '\u2002', '\u2003', '\u2004', '\u2005', '\u2006', '\u2007',
      '\u2008', '\u2009', '\u200A', '\u202F', '\u205F', '\u3000',

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

  private val escapedWhitespaceCharacters =
    Seq(
      // Unicode General Category Zp
      "\\u2029",

      // Unicode General Category Zs
      "\\u0020",
      "\\u00A0",
      "\\u1680",
      "\\u2000",
      "\\u2001",
      "\\u2002",
      "\\u2003",
      "\\u2004",
      "\\u2005",
      "\\u2006",
      "\\u2007",
      "\\u2008",
      "\\u2009",
      "\\u200A",
      "\\u202F",
      "\\u205F",
      "\\u3000",

      // Unicode General Category Zl (Line Separator)
      "\\u2028",

      // Horizontal Tabulation \t
      "\\u0009",

      // Line Feed \n
      "\\u000A",

      // Vertical Tabulation
      "\\u000B",

      // Form Feed \f
      "\\u000C",

      // Carriage Return \r
      "\\u000D",

      // File Separator
      "\\u001C",

      // Group Separator
      "\\u001D",

      // Record Separator
      "\\u001E",

      // Unit Separator
      "\\u001F"
    )

  private val nonJavaWhitespace = Seq('\u00A0', '\u2007', '\u202F')

  /**
   * Tests accepted whitespace characters from CIP-88
   */
  for (whitespace <- whitespaceCharacters) {
    test(
      testName =
        f"Accept the whitespace unicode character in defined set: \\u${Integer.valueOf(whitespace)}%04X"
    ) {
      assert(Character.isWhitespace(whitespace) || nonJavaWhitespace.contains(whitespace))
      s"MATCH$whitespace(m) RETURN m" should parse[Statement].toAstPositioned {
        singleQuery(
          match_(nodePat(name = Some("m"))),
          return_(variableReturnItem("m"))
        )
      }
    }
  }

  for (whitespace <- escapedWhitespaceCharacters) {
    test(
      testName =
        s"Accept the escaped whitespace unicode character in defined set: $whitespace"
    ) {
      s"MATCH$whitespace(m) RETURN m" should parse[Statement].toAstPositioned {
        singleQuery(
          match_(nodePat(name = Some("m"))),
          return_(variableReturnItem("m"))
        )
      }
    }
  }

  /**
   * According to CIP-88 we accept:
   *  - the whitespace categories: Zs (Space Separator), Zl (Line Separator), Zp (Paragraph Separator), see Character.isSpaceChar()
   *  - additional whitespace separators which seem to be aligned with the Character.isWhitespace definition.
   *
   *  These tests should fail if new unicode characters would be added to these categories.
   */
  for (i <- 1 to 0x2fff) {
    if (Character.isWhitespace(i) || Character.isSpaceChar(i)) {
      test(
        testName = f"Accept the whitespace character included in Character.isWhitespace: \\u$i%04X"
      ) {
        s"MATCH${i.toChar}(m) RETURN m" should parse[Statement].toAstPositioned {
          singleQuery(
            match_(nodePat(name = Some("m"))),
            return_(variableReturnItem("m"))
          )
        }
      }
    }
  }

  /**
   * test whitespaces in different locations
   */
  test("  MATCH ( m    ) RETURN m  ") {
    parses[Statement].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("m"))),
        return_(variableReturnItem("m"))
      )
    }
  }

  /**
   * \u0085 is not supported as whitespace until 6.0
   */
  test("u0085 is nt allowed as whitespace") {
    s"MATCHs\\0085(m) RETURN m" should notParse[Statement]
  }
}
