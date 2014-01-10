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
package org.neo4j.cypher.internal.compiler.v2_0.parser

import org.neo4j.cypher.internal.compiler.v2_0._
import org.junit.Test
import org.parboiled.scala._

class LiteralsTest extends ParserTest[Any, Any] with Literals {

  def Expression: Rule1[ast.Expression] = ???

  @Test def testIdentifierCanContainASCII() {
    implicit val parserToTest = Identifier

    parsing("abc") shouldMatch { case ast.Identifier("abc", _) => }
    parsing("a123") shouldMatch { case ast.Identifier("a123", _) => }
    parsing("ABC") shouldMatch { case ast.Identifier("ABC", _) => }
    parsing("_abc") shouldMatch { case ast.Identifier("_abc", _) => }
    parsing("abc_de") shouldMatch { case ast.Identifier("abc_de", _) => }
  }

  @Test def testIdentifierCanContainUTF8() {
    implicit val parserToTest = Identifier

    parsing("aé") shouldMatch { case ast.Identifier("aé", _) => }
    parsing("⁔") shouldMatch { case ast.Identifier("⁔", _) => }
    parsing("＿test") shouldMatch { case ast.Identifier("＿test", _) => }
    parsing("a＿test") shouldMatch { case ast.Identifier("a＿test", _) => }
  }

  @Test
  def testIdentifierCannotStartWithNumber() {
    implicit val parserToTest = Identifier

    assertFails("1bcd")
  }

  @Test
  def testCanParseNumbers() {
    implicit val parserToTest = NumberLiteral

    parsing("123") shouldMatch { case ast.SignedIntegerLiteral("123", _) => }
    parsing("-23") shouldMatch { case ast.SignedIntegerLiteral("-23", _) => }
    parsing("-0") shouldMatch { case ast.SignedIntegerLiteral("-0", _) => }

    parsing("1.23") shouldMatch { case ast.DoubleLiteral("1.23", _) => }
    parsing("13434.23399") shouldMatch { case ast.DoubleLiteral("13434.23399", _) => }
    parsing(".3454") shouldMatch { case ast.DoubleLiteral(".3454", _) => }
    parsing("-0.0") shouldMatch { case ast.DoubleLiteral("-0.0", _) => }
    parsing("-54366.4") shouldMatch { case ast.DoubleLiteral("-54366.4", _) => }
    parsing("-0.3454") shouldMatch { case ast.DoubleLiteral("-0.3454", _) => }

    parsing("1E23") shouldMatch { case ast.DoubleLiteral("1E23", _) => }
    parsing("1.34E99") shouldMatch { case ast.DoubleLiteral("1.34E99", _) => }
    parsing("9E-443") shouldMatch { case ast.DoubleLiteral("9E-443", _) => }
  }

  def convert(result: Any): Any = result
}
