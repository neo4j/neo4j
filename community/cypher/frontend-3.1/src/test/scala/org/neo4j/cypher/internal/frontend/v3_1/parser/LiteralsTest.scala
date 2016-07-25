/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_1.parser

import org.neo4j.cypher.internal.frontend.v3_1.{DummyPosition, ast}
import org.parboiled.scala._

class LiteralsTest extends ParserTest[Any, Any] with Literals {

  def Expression: Rule1[ast.Expression] = ???
  val t = DummyPosition(0)

  test("test variable can contain ascii") {
    implicit val parserToTest = Variable

    parsing("abc") shouldGive ast.Variable("abc")(t)
    parsing("a123") shouldGive ast.Variable("a123")(t)
    parsing("ABC") shouldGive ast.Variable("ABC")(t)
    parsing("_abc") shouldGive ast.Variable("_abc")(t)
    parsing("abc_de") shouldGive ast.Variable("abc_de")(t)
  }

  test("test variable can contain utf8") {
    implicit val parserToTest = Variable

    parsing("aé") shouldGive ast.Variable("aé")(t)
    parsing("⁔") shouldGive ast.Variable("⁔")(t)
    parsing("＿test") shouldGive ast.Variable("＿test")(t)
    parsing("a＿test") shouldGive ast.Variable("a＿test")(t)
  }

  test("test variable name can not start with number") {
    implicit val parserToTest = Variable

    assertFails("1bcd")
  }

  test("can parse numbersz") {
    implicit val parserToTest = NumberLiteral

    parsing("123") shouldGive ast.SignedDecimalIntegerLiteral("123")(t)
    parsing("0") shouldGive ast.SignedDecimalIntegerLiteral("0")(t)
    parsing("-23") shouldGive ast.SignedDecimalIntegerLiteral("-23")(t)
    parsing("-0") shouldGive ast.SignedDecimalIntegerLiteral("-0")(t)

    parsing("0234") shouldGive ast.SignedOctalIntegerLiteral("0234")(t)
    parsing("-0234") shouldGive ast.SignedOctalIntegerLiteral("-0234")(t)

    parsing("0x1") shouldGive ast.SignedHexIntegerLiteral("0x1")(t)
    parsing("0xffff") shouldGive ast.SignedHexIntegerLiteral("0xffff")(t)
    parsing("-0x45FG") shouldGive ast.SignedHexIntegerLiteral("-0x45FG")(t)

    parsing("1.23") shouldGive ast.DecimalDoubleLiteral("1.23")(t)
    parsing("13434.23399") shouldGive ast.DecimalDoubleLiteral("13434.23399")(t)
    parsing(".3454") shouldGive ast.DecimalDoubleLiteral(".3454")(t)
    parsing("-0.0") shouldGive ast.DecimalDoubleLiteral("-0.0")(t)
    parsing("-54366.4") shouldGive ast.DecimalDoubleLiteral("-54366.4")(t)
    parsing("-0.3454") shouldGive ast.DecimalDoubleLiteral("-0.3454")(t)

    parsing("1E23") shouldGive ast.DecimalDoubleLiteral("1E23")(t)
    parsing("1.34E99") shouldGive ast.DecimalDoubleLiteral("1.34E99")(t)
    parsing("9E-443") shouldGive ast.DecimalDoubleLiteral("9E-443")(t)
  }

  def convert(result: Any): Any = result
}
