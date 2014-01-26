/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.parser

import org.neo4j.cypher.internal.compiler.v2_1._
import org.junit.Test
import org.parboiled.scala._

class LiteralsTest extends ParserTest[Any, Any] with Literals {

  def Expression: Rule1[ast.Expression] = ???
  val t = DummyToken(0, 1)

  @Test def testIdentifierCanContainASCII() {
    implicit val parserToTest = Identifier

    parsing("abc") shouldGive ast.Identifier("abc")(t)
    parsing("a123") shouldGive ast.Identifier("a123")(t)
    parsing("ABC") shouldGive ast.Identifier("ABC")(t)
    parsing("_abc") shouldGive ast.Identifier("_abc")(t)
    parsing("abc_de") shouldGive ast.Identifier("abc_de")(t)
  }

  @Test def testIdentifierCanContainUTF8() {
    implicit val parserToTest = Identifier

    parsing("aé") shouldGive ast.Identifier("aé")(t)
    parsing("⁔") shouldGive ast.Identifier("⁔")(t)
    parsing("＿test") shouldGive ast.Identifier("＿test")(t)
    parsing("a＿test") shouldGive ast.Identifier("a＿test")(t)
  }

  @Test
  def testIdentifierCannotStartWithNumber() {
    implicit val parserToTest = Identifier

    assertFails("1bcd")
  }

  @Test
  def testCanParseNumbers() {
    implicit val parserToTest = NumberLiteral

    parsing("123") shouldGive ast.SignedIntegerLiteral("123")(t)
    parsing("-23") shouldGive ast.SignedIntegerLiteral("-23")(t)
    parsing("-0") shouldGive ast.SignedIntegerLiteral("-0")(t)

    parsing("1.23") shouldGive ast.DoubleLiteral("1.23")(t)
    parsing("13434.23399") shouldGive ast.DoubleLiteral("13434.23399")(t)
    parsing(".3454") shouldGive ast.DoubleLiteral(".3454")(t)
    parsing("-0.0") shouldGive ast.DoubleLiteral("-0.0")(t)
    parsing("-54366.4") shouldGive ast.DoubleLiteral("-54366.4")(t)
    parsing("-0.3454") shouldGive ast.DoubleLiteral("-0.3454")(t)

    parsing("1E23") shouldGive ast.DoubleLiteral("1E23")(t)
    parsing("1.34E99") shouldGive ast.DoubleLiteral("1.34E99")(t)
    parsing("9E-443") shouldGive ast.DoubleLiteral("9E-443")(t)
  }

  def convert(result: Any): Any = result
}
