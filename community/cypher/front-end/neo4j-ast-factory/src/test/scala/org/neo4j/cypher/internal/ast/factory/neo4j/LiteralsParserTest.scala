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

import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.NumberLiteral
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedHexIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedOctalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.symbols.CTAny

class LiteralsParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {
  private val t = DummyPosition(0)

  test("test variable can contain ascii") {
    parsing[Variable]("abc") shouldGive expressions.Variable("abc")(t)
    parsing[Variable]("a123") shouldGive expressions.Variable("a123")(t)
    parsing[Variable]("ABC") shouldGive expressions.Variable("ABC")(t)
    parsing[Variable]("_abc") shouldGive expressions.Variable("_abc")(t)
    parsing[Variable]("abc_de") shouldGive expressions.Variable("abc_de")(t)
  }

  test("test variable can contain utf8") {
    parsing[Variable]("aé") shouldGive expressions.Variable("aé")(t)
    parsing[Variable]("⁔") shouldGive expressions.Variable("⁔")(t)
    parsing[Variable]("＿test") shouldGive expressions.Variable("＿test")(t)
    parsing[Variable]("a＿test") shouldGive expressions.Variable("a＿test")(t)
  }

  test("test variable name can not start with number") {
    assertFails[Variable]("1bcd")
  }

  test("can parse numbers") {
    val validInts = Seq("123", "0", "-23", "-0")
    for (i <- validInts) withClue(i) {
      parsing[NumberLiteral](i) shouldGive SignedDecimalIntegerLiteral(i)(t)
    }

    val validOctalInts = Seq("0234", "-0234", "01", "0o1", "0_2")
    for (o <- validOctalInts) withClue(o) {
      parsing[NumberLiteral](o) shouldGive SignedOctalIntegerLiteral(o)(t)
    }

    val validHexInts = Seq("0x1", "0X1", "0xffff", "-0x45FG")
    for (h <- validHexInts) withClue(h) {
      parsing[NumberLiteral](h) shouldGive SignedHexIntegerLiteral(h)(t)
    }

    val validDoubles = Seq(
      "1.23",
      "13434.23399",
      ".3454",
      "-0.0",
      "-54366.4",
      "-0.3454",
      "1E23",
      "1e23",
      "1E+23",
      "1.34E99",
      "9E-443",
      "0.0d",
      ".0d",
      "1e0d",
      "0.0f",
      "0.0somegibberish",
      "0.0"
    )
    for (d <- validDoubles) withClue(d) {
      parsing[NumberLiteral](d) shouldGive DecimalDoubleLiteral(d)(t)
    }
    parsing[NumberLiteral]("- 1.4") shouldGive DecimalDoubleLiteral("-1.4")(t)

    val invalid = Seq("NaN", "Infinity", "Ox", "0_.0", "1_._1", "._2", "1_.0001", "1._0001")
    for (i <- invalid) withClue(i) {
      assertFails[NumberLiteral](i)
    }
  }

  test("can parse parameter syntax") {
    parsing[Parameter]("$p") shouldGive expressions.ExplicitParameter("p", CTAny)(t)
    parsing[Parameter]("$`the funny horse`") shouldGive expressions.ExplicitParameter("the funny horse", CTAny)(t)
    parsing[Parameter]("$0") shouldGive expressions.ExplicitParameter("0", CTAny)(t)

    // parameter number boundaries

    parsing[Parameter]("$1_2") shouldGive parameter("1_2", CTAny)
    parsing[Parameter]("$1") shouldGive parameter("1", CTAny)
    parsing[Parameter]("$1gibberish") shouldGive parameter("1gibberish", CTAny)

    assertFails[Parameter]("$0_2")
    assertFails[Parameter]("$1.0f")
  }

  test("variables are not allowed to start with currency symbols") {
    Seq("$", "¢", "£", "₲", "₶", "\u20BD", "＄", "﹩").foreach { curr =>
      assertFails[Variable](s"${curr}var")
    }
  }
}
