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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedHexIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedOctalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.symbols.CTAny

class LiteralsJavaccParserTest extends JavaccParserTestBase[Any, Any] with AstConstructionTestSupport {

  private val Variable = JavaccRule.Variable
  private val NumberLiteral = JavaccRule.NumberLiteral

  private val t = DummyPosition(0)

  test("test variable can contain ascii") {
    implicit val parserToTest: JavaccRule[Variable] = Variable

    parsing("abc") shouldGive expressions.Variable("abc")(t)
    parsing("a123") shouldGive expressions.Variable("a123")(t)
    parsing("ABC") shouldGive expressions.Variable("ABC")(t)
    parsing("_abc") shouldGive expressions.Variable("_abc")(t)
    parsing("abc_de") shouldGive expressions.Variable("abc_de")(t)
  }

  test("test variable can contain utf8") {
    implicit val parserToTest: JavaccRule[Variable] = Variable

    parsing("aé") shouldGive expressions.Variable("aé")(t)
    parsing("⁔") shouldGive expressions.Variable("⁔")(t)
    parsing("＿test") shouldGive expressions.Variable("＿test")(t)
    parsing("a＿test") shouldGive expressions.Variable("a＿test")(t)
  }

  test("test variable name can not start with number") {
    implicit val parserToTest: JavaccRule[Variable] = Variable

    assertFails("1bcd")
  }

  test("can parse numbers") {
    implicit val parserToTest: JavaccRule[Expression] = NumberLiteral

    val validInts = Seq("123", "0", "-23", "-0")
    for (i <- validInts) withClue(i) {
      parsing(i) shouldGive SignedDecimalIntegerLiteral(i)(t)
    }

    val validOctalInts = Seq("0234", "-0234", "01", "0o1", "0_2")
    for (o <- validOctalInts) withClue(o) {
      parsing(o) shouldGive SignedOctalIntegerLiteral(o)(t)
    }

    val validHexInts = Seq("0x1", "0X1", "0xffff", "-0x45FG")
    for (h <- validHexInts) withClue(h) {
      parsing(h) shouldGive SignedHexIntegerLiteral(h)(t)
    }

    val validDoubles = Seq(
      "1.23", "13434.23399", ".3454", "-0.0", "-54366.4", "-0.3454",
      "1E23", "1e23", "1E+23", "1.34E99", "9E-443", "0.0d", ".0d",
      "1e0d", "0.0f", "0.0somegibberish", "0.0")
    for (d <- validDoubles) withClue(d) {
      parsing(d) shouldGive DecimalDoubleLiteral(d)(t)
    }
    parsing("- 1.4") shouldGive DecimalDoubleLiteral("-1.4")(t)

    val invalid = Seq("NaN", "Infinity", "Ox")
    for (i <- invalid) withClue(i) {
      assertFails(i)
    }
  }

  test("can parse parameter syntax") {
    implicit val parserToTest: JavaccRule[Parameter] = JavaccRule.Parameter

    parsing("$p") shouldGive expressions.Parameter("p", CTAny)(t)
    parsing("$`the funny horse`") shouldGive expressions.Parameter("the funny horse", CTAny)(t)
    parsing("$0") shouldGive expressions.Parameter("0", CTAny)(t)

    //parameter number boundaries

    parsing("$1_2") shouldGive parameter("1_2", CTAny)
    parsing("$1") shouldGive parameter("1", CTAny)
    parsing("$1gibberish") shouldGive parameter("1gibberish", CTAny)

    assertFails("$0_2")
    assertFails("$1.0f")
  }

  test("variables are not allowed to start with currency symbols") {
    implicit val parserToTest: JavaccRule[Variable] = Variable

    Seq("$", "¢", "£", "₲", "₶", "\u20BD", "＄", "﹩").foreach { curr =>
      assertFails(s"${curr}var")
    }
  }

  override def convert(result: Any): Any = result
}
