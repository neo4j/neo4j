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
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.parser.v5.CypherParser
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks

class VariableParserTest extends AstParsingTestBase
    with CypherScalaCheckDrivenPropertyChecks {
  private val t = DummyPosition(0)

  test("test variable can contain ascii") {
    "abc" should parseTo[Variable](expressions.Variable("abc")(t))
    "a123" should parseTo[Variable](expressions.Variable("a123")(t))
    "ABC" should parseTo[Variable](expressions.Variable("ABC")(t))
    "_abc" should parseTo[Variable](expressions.Variable("_abc")(t))
    "abc_de" should parseTo[Variable](expressions.Variable("abc_de")(t))
  }

  test("test variable can contain utf8") {
    "aé" should parseTo[Variable](expressions.Variable("aé")(t))
    "⁔" should parseTo[Variable](expressions.Variable("⁔")(t))
    "＿test" should parseTo[Variable](expressions.Variable("＿test")(t))
    "a＿test" should parseTo[Variable](expressions.Variable("a＿test")(t))
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
    "`abc`" should parseTo[Variable](varFor("abc"))
    "`This isn\\'t a common variable`" should parseTo[Variable](varFor("This isn\\'t a common variable"))
    "`a``b`" should parseTo[Variable](varFor("a`b"))
    "`````abc```" should parseTo[Variable](varFor("``abc`"))
  }

  test("variables are not allowed uneven number of backticks") {
    "RETURN `a`b`" should notParse[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'b'")
      case _ => _.withSyntaxError(
          """Invalid input 'b': expected an expression, 'FOREACH', ',', 'AS', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 11 (offset: 10))
            |"RETURN `a`b`"
            |           ^""".stripMargin
        )
    }
  }

  test("variables are now allowed start with number") {
    "1bcd" should notParse[Variable].in {
      case Cypher5JavaCc => _.withMessageContaining("Was expecting one of:")
      case _ => _.withSyntaxError(
          """Invalid input '1bcd': expected an identifier (line 1, column 1 (offset: 0))
            |"1bcd"
            | ^""".stripMargin
        )
    }
  }

  test("variables are not allowed to start with currency symbols") {
    Seq("$", "¢", "£", "₲", "₶", "\u20BD", "＄", "﹩").foreach { curr =>
      s"${curr}var" should notParse[Variable].in {
        case Cypher5JavaCc => _.withMessageContaining("Was expecting one of:")
        case _ => _.withSyntaxError(
            s"""Invalid input '$curr': expected an identifier (line 1, column 1 (offset: 0))
               |"${curr}var"
               | ^""".stripMargin
          )
      }
    }
  }

  test("keywords can be variables") {
    val vocab = CypherParser.VOCABULARY
    Range.inclusive(1, vocab.getMaxTokenType)
      .flatMap { tokenType =>
        Option(vocab.getSymbolicName(tokenType)) ++
          Option(vocab.getDisplayName(tokenType)) ++
          Option(vocab.getLiteralName(tokenType))
      }
      .flatMap(n => Seq(n, n.replace("_", "")))
      .distinct
      .foreach { name =>
        val cypher = cleanName(name)
        if (Character.isAlphabetic(cypher.charAt(0))) {
          cypher should parseTo[Variable](varFor(cypher))
        }
        if (cypher != "``") {
          s"`$cypher`" should parseTo[Variable](varFor(cypher))
        }
      }
  }

  private def cleanName(input: String): String = {
    if (input == null) null
    else if (input.startsWith("'") && input.endsWith("'")) input.substring(1, input.length - 1)
    else input
  }
}
