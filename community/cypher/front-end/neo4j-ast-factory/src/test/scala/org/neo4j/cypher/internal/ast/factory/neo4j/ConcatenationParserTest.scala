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

class ConcatenationParserTest extends AstParsingTestBase {

  test("a || b") {
    parsesTo[Expression] {
      concatenate(
        varFor("a"),
        varFor("b")
      )
    }
  }

  test("a || b || c") {
    parsesTo[Expression] {
      concatenate(
        concatenate(
          varFor("a"),
          varFor("b")
        ),
        varFor("c")
      )
    }
  }

  test("a + b || c - d || e") {
    parsesTo[Expression] {
      concatenate(
        subtract(
          concatenate(
            add(
              varFor("a"),
              varFor("b")
            ),
            varFor("c")
          ),
          varFor("d")
        ),
        varFor("e")
      )
    }
  }

  test("RETURN a ||") {
    failsParsing[Statements].in {
      case JavaCc => _.withMessageStart("Invalid input '': expected \"+\" or \"-\"")
      case Antlr => _.withSyntaxError(
          """Invalid input '': expected an expression (line 1, column 12 (offset: 11))
            |"RETURN a ||"
            |            ^""".stripMargin
        )
    }
  }

  test("RETURN || b") {
    failsParsing[Statements].in {
      case JavaCc => _.withMessageStart("Invalid input '||': expected \"*\", \"DISTINCT\" or an expression")
      case Antlr => _.withSyntaxError(
          """Invalid input '||': expected an expression, '*' or 'DISTINCT' (line 1, column 8 (offset: 7))
            |"RETURN || b"
            |        ^""".stripMargin
        )
    }
  }

  test("RETURN a ||| b") {
    failsParsing[Statements].in {
      case JavaCc => _.withMessageStart("Invalid input '|': expected \"+\" or \"-\"")
      case Antlr => _.withSyntaxError(
          """Invalid input '|': expected an expression (line 1, column 12 (offset: 11))
            |"RETURN a ||| b"
            |            ^""".stripMargin
        )
    }
  }

  test("RETURN a || || b") {
    failsParsing[Statements].in {
      case JavaCc => _.withMessageStart("Invalid input '||': expected \"+\" or \"-\"")
      case Antlr => _.withSyntaxError(
          """Invalid input '||': expected an expression (line 1, column 13 (offset: 12))
            |"RETURN a || || b"
            |             ^""".stripMargin
        )
    }
  }
}
