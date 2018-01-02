/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.ast.conditions

import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class NormalizedEqualsArgumentsTest extends CypherFunSuite with AstConstructionTestSupport {

  private val condition: (Any => Seq[String]) = normalizedEqualsArguments

  test("happy if the property in equals is normalized") {
    val ast: Equals = Equals(Property(ident("a"), PropertyKeyName("prop")_)_, SignedDecimalIntegerLiteral("12")_)_

    condition(ast) shouldBe empty
  }

  test("unhappy if the property in equals is not normalized") {
    val ast: Equals = Equals(SignedDecimalIntegerLiteral("12")_, Property(ident("a"), PropertyKeyName("prop")_)_)_

    condition(ast) should equal(Seq(s"Equals at ${ast.position} is not normalized: $ast"))
  }

  test("happy if the Id-function in equals is normalized") {
    val ast: Equals = Equals(id("a"), SignedDecimalIntegerLiteral("12")_)_

    condition(ast) shouldBe empty
  }

  test("unhappy if the Id-function in equals is not normalized") {
    val ast: Equals = Equals(SignedDecimalIntegerLiteral("12")_, id("a"))_

    condition(ast) should equal(Seq(s"Equals at ${ast.position} is not normalized: $ast"))
  }

  test("happy if the Id-function and the property in equals are normalized") {
    val ast: Equals = Equals(id("a"), Property(ident("a"), PropertyKeyName("prop")_)_)_

    condition(ast) shouldBe empty
  }

  test("unhappy if the Id-function and the property in equals are not normalized") {
    val ast: Equals = Equals(Property(ident("a"), PropertyKeyName("prop")_)_, id("a"))_

    condition(ast) should equal(Seq(s"Equals at ${ast.position} is not normalized: $ast"))
  }

  private def id(name: String): FunctionInvocation =
    FunctionInvocation(FunctionName("id")(pos), distinct = false, Array(Identifier(name)(pos)))(pos)
}
