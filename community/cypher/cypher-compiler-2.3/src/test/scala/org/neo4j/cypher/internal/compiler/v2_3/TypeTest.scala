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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions._
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryStateHelper
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class TypeTest extends CypherFunSuite {

  test("plus int int") {
    val op = Add(Literal(1), Literal(2))

    val result = calc(op)

    result shouldBe a [java.lang.Long]
  }

  test("plus double int") {
    val op = Add(Literal(1.2), Literal(2))

    val result = calc(op)

    result shouldBe a [java.lang.Double]
  }

  test("minus int int") {
    val op = Subtract(Literal(1), Literal(2))

    val result = calc(op)

    result shouldBe a [java.lang.Long]
  }

  test("minus double int") {
    val op = Subtract(Literal(1.2), Literal(2))

    val result = calc(op)

    result shouldBe a [java.lang.Double]
  }

  test("multiply int int") {
    val op = Multiply(Literal(1), Literal(2))

    val result = calc(op)

    result shouldBe a [java.lang.Long]
  }

  test("multiply double int") {
    val op = Multiply(Literal(1.2), Literal(2))

    val result = calc(op)

    result shouldBe a [java.lang.Double]
  }

  test("divide int int") {
    val op = Divide(Literal(1), Literal(2))

    val result = calc(op)

    result shouldBe a [java.lang.Integer]
  }

  test("divide double int") {
    val op = Divide(Literal(1.2), Literal(2))

    val result = calc(op)

    result shouldBe a [java.lang.Double]
  }

  private def calc(e:Expression) = e.apply(ExecutionContext.empty)(QueryStateHelper.empty)
}
