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
package org.neo4j.cypher.internal.ast.factory.expression

import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.ast.test.util.AstParsing.ParserInTest
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks

class VariableParserTest extends AstParsingTestBase
    with CypherScalaCheckDrivenPropertyChecks {

  test("test variable can contain ascii") {
    "abc" should parseTo[Variable](varFor("abc"))
    "a123" should parseTo[Variable](varFor("a123"))
    "ABC" should parseTo[Variable](varFor("ABC"))
    "_abc" should parseTo[Variable](varFor("_abc"))
    "abc_de" should parseTo[Variable](varFor("abc_de"))
  }

  test("test variable can contain utf8") {
    "aé" should parseTo[Variable](varFor("aé"))
    "⁔" should parseTo[Variable](varFor("⁔"))
    "＿test" should parseTo[Variable](varFor("＿test"))
    "a＿test" should parseTo[Variable](varFor("a＿test"))
  }

  test("float literals parse as a variable name") {
    "NaN" should parseTo[Variable](varFor("NaN"))
    "nan" should parseTo[Variable](varFor("nan"))
    "nAn" should parseTo[Variable](varFor("nAn"))
    "Inf" should parseTo[Variable](varFor("Inf"))
    "inf" should parseTo[Variable](varFor("inf"))
    "Infinity" should parseTo[Variable](varFor("Infinity"))
    "infinity" should parseTo[Variable](varFor("infinity"))
  }

  test("keywords parse as a variable name") {
    "NOT" should parseTo[Variable](varFor("NOT"))
    "and" should parseTo[Variable](varFor("and"))
    "Match" should parseTo[Variable](varFor("Match"))
    "CREATE" should parseTo[Variable](varFor("CREATE"))
  }

  test("escaped variable name") {
    "`abc`" should parseIn[Variable] {
      case Cypher5 => _.toAst(varFor("abc", isIsolated = true))
      case _       => _.toAst(varFor("abc", isIsolated = false))
    }
    "`This isn\\'t a common variable`" should parseIn[Variable] {
      case Cypher5 => _.toAst(varFor("This isn\\'t a common variable", isIsolated = true))
      case _       => _.toAst(varFor("This isn\\'t a common variable", isIsolated = false))
    }
    "`a``b`" should parseIn[Variable] {
      case Cypher5 => _.toAst(varFor("a`b", isIsolated = true))
      case _       => _.toAst(varFor("a`b", isIsolated = false))
    }
    "`````abc```" should parseIn[Variable] {
      case Cypher5 => _.toAst(varFor("``abc`", isIsolated = true))
      case _       => _.toAst(varFor("``abc`", isIsolated = false))
    }
  }

  test("variables are not allowed uneven number of backticks") {
    "RETURN `a`b`" should notParse[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'b': expected an expression, ',', 'AS', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'FOREACH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 11 (offset: 10))
            |"RETURN `a`b`"
            |           ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'b': expected an expression, ',', 'AS', 'GROUP BY', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 11 (offset: 10))
            |"RETURN `a`b`"
            |           ^""".stripMargin
        )
    }
  }

  test("variables are now allowed start with number") {
    "1bcd" should notParse[Variable].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input '1bcd': expected an identifier (line 1, column 1 (offset: 0))
            |"1bcd"
            | ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '1bcd': expected a variable name (line 1, column 1 (offset: 0))
            |"1bcd"
            | ^""".stripMargin
        )
    }
  }

  test("variables are not allowed to start with currency symbols") {
    Seq("$", "¢", "£", "₲", "₶", "\u20BD", "＄", "﹩").foreach { curr =>
      s"${curr}var" should notParse[Variable].in {
        case Cypher5 => _.withSyntaxError(
            s"""Invalid input '$curr': expected an identifier (line 1, column 1 (offset: 0))
               |"${curr}var"
               | ^""".stripMargin
          )
        case _ =>
          val invalid =
            if (curr == "$" || curr == "¢" || curr == "£") curr
            else s"${curr}var"
          _.withSyntaxError(
            s"""Invalid input '$invalid': expected a variable name (line 1, column 1 (offset: 0))
               |"${curr}var"
               | ^""".stripMargin
          )
      }
    }
  }

  test("keywords can be variables") {
    val names = for {
      parser <- ParserInTest.AllParsers
      vocab = parser.vocabulary
      tokenType <- Range.inclusive(1, vocab.getMaxTokenType)
      name <- Seq(vocab.getSymbolicName(tokenType), vocab.getDisplayName(tokenType), vocab.getLiteralName(tokenType))
      if name != null
      massagedName <- Seq(name, name.replace("_", ""))
    } yield massagedName

    names.distinct.foreach { name =>
      val cypher = cleanName(name)
      if (Character.isAlphabetic(cypher.charAt(0))) {
        cypher should parseTo[Variable](varFor(cypher))
      }
      if (cypher != "``") {
        s"`$cypher`" should parseIn[Variable] {
          case Cypher5 => _.toAst(varFor(cypher, isIsolated = true))
          case _       => _.toAst(varFor(cypher, isIsolated = false))
        }
      }
    }
  }

  private def cleanName(input: String): String = {
    if (input == null) null
    else if (input.startsWith("'") && input.endsWith("'")) input.substring(1, input.length - 1)
    else input
  }
}
