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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher6
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.util.UnicodeHelper
import org.neo4j.cypher.internal.util.symbols.CTAny

class ParameterIdentifiersParserTest extends AstParsingTestBase {

  // These chars are characters that will change the meaning of the query, making it a different test, so skip them.
  val specialChars: Seq[Char] = List('"', '\'', '/', '*', '%', '+', '-', ',', '.', ':', '<', '>', '=', '^', '`')

  // In Cypher6 Extended Identifiers are allowed in the first position as well.
  test("Identifier Start characters are allowed in first position") {
    for (c <- Character.MIN_VALUE to Character.MAX_VALUE) {
      if (Character.getType(c) != Character.SURROGATE && !specialChars.contains(c)) {
        val paramWithCharName = f"paramWithChar_${Integer.valueOf(c)}%04X"

        s"RETURN $$${c}abc AS `$paramWithCharName`" should parseIn[Statements] {
          case Cypher6 if UnicodeHelper.isIdentifierPart(c, CypherVersion.Cypher6) =>
            _.toAstPositioned(
              singleQuery(
                return_(aliasedReturnItem(parameter(s"${c}abc", CTAny), paramWithCharName))
              )
            )
          case _ if Character.isWhitespace(c) || Character.isSpaceChar(c) =>
            _.toAstPositioned(
              singleQuery(
                return_(aliasedReturnItem(parameter(s"abc", CTAny), paramWithCharName))
              )
            )
          case Cypher6 if c == '\u0085' =>
            _.toAstPositioned(
              singleQuery(
                return_(aliasedReturnItem(parameter(s"abc", CTAny), paramWithCharName))
              )
            )
          case Cypher6 => _.withSyntaxErrorContaining("Invalid input")
          case _ if UnicodeHelper.isIdentifierStart(c, CypherVersion.Cypher5) || (c >= 0x31 && c <= 0x39) =>
            _.toAstPositioned(
              singleQuery(
                return_(aliasedReturnItem(parameter(s"${c}abc", CTAny), paramWithCharName))
              )
            )
          case _ => _.withSyntaxErrorContaining("Invalid input")
        }
      }
    }
  }

  test("Extended Identifier characters should be allowed in second position") {
    for (c <- Character.MIN_VALUE to Character.MAX_VALUE) {
      val paramWithCharName = f"paramWithChar_${Integer.valueOf(c)}%04X"
      if (Character.getType(c) != Character.SURROGATE && !specialChars.contains(c)) {
        s"RETURN $$a${c}abc AS `$paramWithCharName`" should parseIn[Statements] {
          case Cypher6 if UnicodeHelper.isIdentifierPart(c, CypherVersion.Cypher6) =>
            _.toAstPositioned(
              singleQuery(
                return_(aliasedReturnItem(parameter(s"a${c}abc", CTAny), paramWithCharName))
              )
            )
          case Cypher6 => _.withSyntaxErrorContaining("Invalid input")
          case _ if UnicodeHelper.isIdentifierPart(c, CypherVersion.Cypher5) =>
            _.toAstPositioned(
              singleQuery(
                return_(aliasedReturnItem(parameter(s"a${c}abc", CTAny), paramWithCharName))
              )
            )
          case _ => _.withSyntaxErrorContaining("Invalid input")
        }
      }
    }

  }
}
