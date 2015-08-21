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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.InterpolationValue
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryStateHelper
import org.neo4j.cypher.internal.frontend.v2_3.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class InterpolationValueTest extends CypherFunSuite {

  implicit val state = QueryStateHelper.empty

  test("should compile literals") {
    evaluateInterpolation(Right("string"))().to(InterpolatedLiteral("string"))
    evaluateInterpolation(Right("$"))().to(InterpolatedLiteral("$"))
    evaluateInterpolation(Right("\'\\"))().to(InterpolatedLiteral("\'\\"))
  }

  test("should compile identifiers") {
    evaluateInterpolation(Left(Identifier("n")))("n" -> 1).to(InterpolatedExpression("1"))
    evaluateInterpolation(Left(Identifier("n")))("n" -> "1").to(InterpolatedExpression("1"))
    evaluateInterpolation(Left(Identifier("n")))("n" -> 0.5).to(InterpolatedExpression("0.5"))
    evaluateInterpolation(Left(Identifier("n")))("n" -> Math.PI).to(InterpolatedExpression(Math.PI.toString))
    evaluateInterpolation(Left(Identifier("n")))("n" -> null).toNull()
    evaluateInterpolation(Right("prefix"), Left(Identifier("n")), Right("suffix"))("n" -> null).toNull()
    evaluateInterpolation(Right("prefix"), Left(Identifier("n")), Right("suffix"), Left(Identifier("m")))("n" -> 0.5, "m" -> null).toNull()
  }

  test("should compile nested interpolations") {
    evaluateInterpolation(Left(Identifier("n")))("n" -> InterpolationValue(NonEmptyList(InterpolatedLiteral("string")))).to(InterpolatedExpression("string"))
    evaluateInterpolation(Left(Identifier("n")))("n" -> InterpolationValue(NonEmptyList(InterpolatedExpression("15"), InterpolatedLiteral("suffix")))).to(InterpolatedExpression("15suffix"))
  }

  test("should compile additions") {
    evaluateInterpolation(Left(Add(Identifier("n"), Literal(12))))("n" -> 1).to(InterpolatedExpression("13"))
    evaluateInterpolation(Left(Add(Identifier("n"), Identifier("m"))))("n" -> 1, "m" -> 2).to(InterpolatedExpression("3"))
  }

  private case class evaluateInterpolation(head: Either[Expression, String], tail: Either[Expression, String]*)(values: (String, Any)*) {
    def to(toHead: InterpolatedString, toTail: InterpolatedString*) = {
      val actual = Interpolation(NonEmptyList(head, tail:_*)).apply(ExecutionContext.from(values:_*))
      val expected = InterpolationValue(NonEmptyList(toHead, toTail:_*))

      actual should equal(expected)
    }

    def toNull() = {
      val actual = Interpolation(NonEmptyList(head, tail:_*)).apply(ExecutionContext.from(values:_*))

      actual should be(null.asInstanceOf[InterpolationValue])
    }
  }
}
