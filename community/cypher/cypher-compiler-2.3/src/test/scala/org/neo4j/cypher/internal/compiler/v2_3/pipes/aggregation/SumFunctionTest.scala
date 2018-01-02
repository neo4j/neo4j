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
package org.neo4j.cypher.internal.compiler.v2_3.pipes.aggregation

import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Expression
import org.neo4j.cypher.internal.frontend.v2_3.CypherTypeException
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class SumFunctionTest extends CypherFunSuite with AggregateTest {
  def createAggregator(inner: Expression) = new SumFunction(inner)

  test("singleValueReturnsThatNumber") {
    val result = aggregateOn(1)

    result should equal(1)
    result shouldBe a [java.lang.Long]
  }

  test("singleValueOfDecimalReturnsDecimal") {
    val result = aggregateOn(1.0d)

    result should equal(1.0)
    result shouldBe a [java.lang.Double]
  }

  test("mixOfIntAndDoubleYieldsDouble") {
    val result = aggregateOn(1, 1.0d)

    result should equal(2.0)
    result shouldBe a [java.lang.Double]
  }

  test("mixedLotsOfStuff") {
    val result = aggregateOn(1.byteValue(), 1.shortValue())

    result should equal(2)
    result shouldBe a [java.lang.Long]
  }

  test("noNumbersEqualsZero") {
    val result = aggregateOn()

    result should equal(0)
    result shouldBe a [java.lang.Long]
  }

  test("nullDoesNotChangeTheSum") {
    val result = aggregateOn(1, null)

    result should equal(1)
    result shouldBe a [java.lang.Long]
  }

  test("noNumberValuesThrowAnException") {
    intercept[CypherTypeException](aggregateOn(1, "wut"))
  }

  test("intOverflowTransformsSumToLong") {
    val halfInt= Int.MaxValue
    val result = aggregateOn(halfInt, halfInt, halfInt)
    val expected = 3L * halfInt
    result should equal(expected)
  }

  test("typesArentUnnecessaryWidened") {
    val thirdOfMaxInt: Int = Int.MaxValue / 3
    val result = aggregateOn(thirdOfMaxInt, thirdOfMaxInt)
    val expected = thirdOfMaxInt + thirdOfMaxInt
    result should equal(expected)
    result shouldBe a [java.lang.Long]
  }
}
