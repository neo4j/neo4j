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

import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Antlr
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.JavaCc
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.expressions.Expression

class NormalizeFunctionParserTest extends AstParsingTestBase {

  // Normalize defaults to NFC
  test("normalize(\"hello\")") {
    parsesTo[Expression](function("normalize", literalString("hello"), literalString("NFC")))
  }

  // All normal form keywords parse as expected
  Seq("NFC", "NFD", "NFKC", "NFKD").foreach { normalForm =>
    test(s"normalize(foo, $normalForm)") {
      parsesTo[Expression](function("normalize", varFor("foo"), literalString(normalForm)))
    }
    test(s"return my.own.normalize(foo, $normalForm)") {
      parsesTo[Statements] {
        Statements(Seq(singleQuery(return_(returnItem(
          function(Seq("my", "own"), "normalize", varFor("foo"), varFor(normalForm)),
          s"my.own.normalize(foo, $normalForm)"
        )))))
      }
    }
  }

  test("normalize with namespace") {
    "return my.own.normalize('1')" should parseTo[Statements] {
      Statements(Seq(singleQuery(return_(returnItem(
        function(Seq("my", "own"), "normalize", literal("1")),
        "my.own.normalize('1')"
      )))))
    }
    "return my.own.normalize('1',2)" should parseTo[Statements] {
      Statements(Seq(singleQuery(return_(returnItem(
        function(Seq("my", "own"), "normalize", literal("1"), literal(2)),
        "my.own.normalize('1',2)"
      )))))
    }
    "return my.own.normalize('1', notANormalForm)" should parseTo[Statements] {
      Statements(Seq(singleQuery(return_(returnItem(
        function(Seq("my", "own"), "normalize", literal("1"), varFor("notANormalForm")),
        "my.own.normalize('1', notANormalForm)"
      )))))
    }
  }

  // Failing tests
  test("RETURN normalize(\"hello\", \"NFC\")") {
    failsParsing[Statements].in {
      case JavaCc => _.withMessageStart(
          "Invalid input 'NFC': expected \"NFC\", \"NFD\", \"NFKC\" or \"NFKD\" (line 1, column 27 (offset: 26))"
        )
      case Antlr => _.withMessage(
          """Invalid normal form, expected NFC, NFD, NFKC, NFKD (line 1, column 27 (offset: 26))
            |"RETURN normalize("hello", "NFC")"
            |                           ^""".stripMargin
        )
    }
  }

  test("RETURN normalize(\"hello\", null)") {
    failsParsing[Statements].in {
      case JavaCc => _.withMessageStart(
          "Invalid input 'null': expected \"NFC\", \"NFD\", \"NFKC\" or \"NFKD\" (line 1, column 27 (offset: 26))"
        )
      case Antlr => _.withSyntaxError(
          """Invalid normal form, expected NFC, NFD, NFKC, NFKD (line 1, column 27 (offset: 26))
            |"RETURN normalize("hello", null)"
            |                           ^""".stripMargin
        )
    }
  }

  test("RETURN normalize(\"hello\", NFF)") {
    failsParsing[Statements].in {
      case JavaCc => _.withMessageStart(
          "Invalid input 'NFF': expected \"NFC\", \"NFD\", \"NFKC\" or \"NFKD\" (line 1, column 27 (offset: 26))"
        )
      case Antlr => _.withSyntaxError(
          """Invalid normal form, expected NFC, NFD, NFKC, NFKD (line 1, column 27 (offset: 26))
            |"RETURN normalize("hello", NFF)"
            |                           ^""".stripMargin
        )
    }
  }

  test("normalize(\"hello\", NFC, anotherVar)") {
    parsesIn[Expression] {
      case JavaCc => _.withAnyFailure
      case _      => _.withoutErrors
    }
  }

  test("normalize()") {
    parsesIn[Expression] {
      case JavaCc => _.withAnyFailure
      case _      => _.withoutErrors
    }
  }
}
