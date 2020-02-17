/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.parser

import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.parboiled.scala.Rule1

class LiteralsTest extends ParserTest[Any, Any] with Literals {

  def Expression: Rule1[expressions.Expression] = ???
  val t = DummyPosition(0)

  test("test variable can contain ascii") {
    implicit val parserToTest = Variable

    parsing("abc") shouldGive expressions.Variable("abc")(t)
    parsing("a123") shouldGive expressions.Variable("a123")(t)
    parsing("ABC") shouldGive expressions.Variable("ABC")(t)
    parsing("_abc") shouldGive expressions.Variable("_abc")(t)
    parsing("abc_de") shouldGive expressions.Variable("abc_de")(t)
  }

  test("test variable can contain utf8") {
    implicit val parserToTest = Variable

    parsing("aé") shouldGive expressions.Variable("aé")(t)
    parsing("⁔") shouldGive expressions.Variable("⁔")(t)
    parsing("＿test") shouldGive expressions.Variable("＿test")(t)
    parsing("a＿test") shouldGive expressions.Variable("a＿test")(t)
  }

  test("test variable name can not start with number") {
    implicit val parserToTest = Variable

    assertFails("1bcd")
  }

  test("can parse numbers") {
    implicit val parserToTest = NumberLiteral

    parsing("123") shouldGive expressions.SignedDecimalIntegerLiteral("123")(t)
    parsing("0") shouldGive expressions.SignedDecimalIntegerLiteral("0")(t)
    parsing("-23") shouldGive expressions.SignedDecimalIntegerLiteral("-23")(t)
    parsing("-0") shouldGive expressions.SignedDecimalIntegerLiteral("-0")(t)

    parsing("0234") shouldGive expressions.SignedOctalIntegerLiteral("0234")(t)
    parsing("-0234") shouldGive expressions.SignedOctalIntegerLiteral("-0234")(t)

    parsing("0x1") shouldGive expressions.SignedHexIntegerLiteral("0x1")(t)
    parsing("0xffff") shouldGive expressions.SignedHexIntegerLiteral("0xffff")(t)
    parsing("-0x45FG") shouldGive expressions.SignedHexIntegerLiteral("-0x45FG")(t)

    parsing("1.23") shouldGive expressions.DecimalDoubleLiteral("1.23")(t)
    parsing("13434.23399") shouldGive expressions.DecimalDoubleLiteral("13434.23399")(t)
    parsing(".3454") shouldGive expressions.DecimalDoubleLiteral(".3454")(t)
    parsing("-0.0") shouldGive expressions.DecimalDoubleLiteral("-0.0")(t)
    parsing("-54366.4") shouldGive expressions.DecimalDoubleLiteral("-54366.4")(t)
    parsing("-0.3454") shouldGive expressions.DecimalDoubleLiteral("-0.3454")(t)

    parsing("1E23") shouldGive expressions.DecimalDoubleLiteral("1E23")(t)
    parsing("1e23") shouldGive expressions.DecimalDoubleLiteral("1e23")(t)
    parsing("1E+23") shouldGive expressions.DecimalDoubleLiteral("1E+23")(t)
    parsing("1.34E99") shouldGive expressions.DecimalDoubleLiteral("1.34E99")(t)
    parsing("9E-443") shouldGive expressions.DecimalDoubleLiteral("9E-443")(t)
  }

  test("can parse parameter syntax") {
    implicit val parserToTest = Parameter

    parsing("$p") shouldGive expressions.Parameter("p", CTAny)(t)
    parsing("$`the funny horse`") shouldGive expressions.Parameter("the funny horse", CTAny)(t)
    parsing("$0") shouldGive expressions.Parameter("0", CTAny)(t)
  }

  test("can parse legacy parameter syntax") {
    implicit val parserToTest = OldParameter

    parsing("{p}") shouldGive expressions.ParameterWithOldSyntax("p", CTAny)(t)
    parsing("{`the funny horse`}") shouldGive expressions.ParameterWithOldSyntax("the funny horse", CTAny)(t)
    parsing("{0}") shouldGive expressions.ParameterWithOldSyntax("0", CTAny)(t)
  }

  test("variables are not allowed to start with currency symbols") {
    implicit val parserToTest = Variable

    Seq("$", "¢", "£", "₲", "₶", "\u20BD", "＄", "﹩").foreach { curr =>
      assertFails(s"${curr}var")
    }
  }

  def convert(result: Any): Any = result
}
