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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.util.UnicodeHelper
import org.neo4j.cypher.internal.util.symbols.CTAny

class ParameterIdentifiersParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test("Identifier Start characters are allowed in first position") {
    for (c <- Character.MIN_VALUE to Character.MAX_VALUE) {
      val paramWithCharName = f"paramWithChar_${Integer.valueOf(c)}%04X"
      if (
        (UnicodeHelper.isIdentifierStart(c) && Character.getType(c) != Character.CURRENCY_SYMBOL) ||
        (c >= 0x31 && c <= 0x39) // Start with a a digit 1-9
      ) {
        s"RETURN $$${c}abc AS `$paramWithCharName`" should parse[Statement].toAstPositioned(
          singleQuery(
            return_(aliasedReturnItem(parameter(s"${c}abc", CTAny), paramWithCharName))
          )
        )
      } else if (Character.isWhitespace(c) || Character.isSpaceChar(c)) {
        // Whitespace gets ignored
        s"RETURN $$${c}abc AS `$paramWithCharName`" should parse[Statement].toAstPositioned(
          singleQuery(
            return_(aliasedReturnItem(parameter(s"abc", CTAny), paramWithCharName))
          )
        )
      } else if (Character.getType(c) != Character.SURROGATE) { // Surrogate characters fail completely so can't test
        s"RETURN $$${c}abc AS `$paramWithCharName`" should notParse[Statements]
      }
    }
  }

  test("Extended Identifier characters should be allowed in second position") {
    for (c <- Character.MIN_VALUE to Character.MAX_VALUE) {
      val paramWithCharName = f"paramWithChar_${Integer.valueOf(c)}%04X"
      if (UnicodeHelper.isIdentifierPart(c)) {
        s"RETURN $$a${c}abc AS `$paramWithCharName`" should parse[Statement].toAstPositioned(
          singleQuery(
            return_(aliasedReturnItem(parameter(s"a${c}abc", CTAny), paramWithCharName))
          )
        )
      } else if (
        Character.isWhitespace(c) ||
        Character.isSpaceChar(c) ||
        Character.getType(c) != Character.SURROGATE &&
        !List('"', '\'', '/', '*', '%', '+', '-', ',', '.', ':', '<', '>', '=', '^', '`').contains(c)
      ) {
        s"RETURN $$a${c}abc AS `$paramWithCharName`" should notParse[Statements]
      }
    }
  }
}
