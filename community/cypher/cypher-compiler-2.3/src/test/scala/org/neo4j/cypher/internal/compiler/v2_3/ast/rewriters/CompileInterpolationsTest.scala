/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters

import org.neo4j.cypher.internal.frontend.v2_3.SyntaxException
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class CompileInterpolationsTest extends CypherFunSuite with AstConstructionTestSupport {

  // We use scala string interpolations here to produce Cypher string interpolations in order to get around
  // a really annoying scalac warning about possibly having forgotten to interpolate a string literal that
  // contains a '$' (warning cannot be disables as of 25-08-2015)

  test("should compile correct interpolations") {
    compilingInterpolationLiteral(s"").produces(Right(""))
    compilingInterpolationLiteral(s"prefix").produces(Right("prefix"))
    compilingInterpolationLiteral(s"$${param}").produces(Left(ident("param")))
    compilingInterpolationLiteral(s"Hello, $${{param}}!").produces(Right("Hello, "), Left(Parameter("param")_), Right("!"))
    compilingInterpolationLiteral(s"Hello, $$$${Mats}!").produces(Right(s"Hello, $${Mats}!"))
    compilingInterpolationLiteral(s"n.prop is $${n.prop}").produces(Right("n.prop is "), Left(Property(ident("n"), PropertyKeyName("prop")_)_))
    compilingInterpolationLiteral(s"$${a}$${b}").produces(Left(ident("a")), Left(ident("b")))
    compilingInterpolationLiteral(s"Hello, $${{name}}! Would you like to win $$$$$${amount}?").produces(
      Right("Hello, "), Left(Parameter("name")_), Right("! Would you like to win $"), Left(ident("amount")), Right("?")
    )
    compilingInterpolationLiteral(s"$${prefix}%").produces(Left(ident("prefix")), Right("%"))
    compilingInterpolationLiteral(s"$${''}").produces(Left(StringLiteral("") _))
  }

  test("should fail to compile unbalanced interpolations") {
    compilingInterpolationLiteral(s"$$").throwsSyntaxException()
    compilingInterpolationLiteral(s"$${").throwsSyntaxException()
    compilingInterpolationLiteral(s"Mixed $${{param}").throwsSyntaxException()
    compilingInterpolationLiteral(s"$${}").throwsSyntaxException()
    compilingInterpolationLiteral(s"$$a").throwsSyntaxException()
    compilingInterpolationLiteral(s"characters in a string $$ continuation").throwsSyntaxException()
  }

  private case class compilingInterpolationLiteral(pattern: String) {
    def produces(head: Either[Expression, String], tail: Either[Expression, String]*) = {
      val actual = compileInterpolation(pattern)
      val expected = Interpolation(NonEmptyList(head, tail: _*))(pos)

      actual should equal(expected)
    }

    def throwsSyntaxException() = {
      a[SyntaxException] should be thrownBy compileInterpolation(pattern)
    }
  }

  private def compileInterpolation(pattern: String) =
    compileInterpolations(InterpolationLiteral(pattern)(pos))
}
