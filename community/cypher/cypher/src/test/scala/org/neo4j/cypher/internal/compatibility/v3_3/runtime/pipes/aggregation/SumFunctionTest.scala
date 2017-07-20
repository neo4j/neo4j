/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.aggregation

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.Expression
import org.neo4j.cypher.internal.frontend.v3_3.CypherTypeException
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values._
import org.neo4j.values.storable.{DoubleValue, LongValue, Values}

class SumFunctionTest extends CypherFunSuite with AggregateTest {
  def createAggregator(inner: Expression) = new SumFunction(inner)

  test("singleValueReturnsThatNumber") {
    val result = aggregateOn(longValue(1))

    result should equal(longValue(1))
    result shouldBe a [LongValue]
  }

  test("singleValueOfDecimalReturnsDecimal") {
    val result = aggregateOn(doubleValue(1.0d))

    result should equal(doubleValue(1.0))
    result shouldBe a [DoubleValue]
  }

  test("mixOfIntAndDoubleYieldsDouble") {
    val result = aggregateOn(intValue(1), doubleValue(1.0d))

    result should equal(doubleValue(2.0))
    result shouldBe a [DoubleValue]
  }

  test("mixedLotsOfStuff") {
    val result = aggregateOn(Values.byteValue(1.byteValue()), Values.shortValue(1.shortValue()))

    result should equal(longValue(2))
    result shouldBe a [LongValue]
  }

  test("noNumbersEqualsZero") {
    val result = aggregateOn()

    result should equal(longValue(0))
    result shouldBe a [LongValue]
  }

  test("nullDoesNotChangeTheSum") {
    val result = aggregateOn(intValue(1), NO_VALUE)

    result should equal(longValue(1))
    result shouldBe a [LongValue]
  }

  test("noNumberValuesThrowAnException") {
    intercept[CypherTypeException](aggregateOn(intValue(1), stringValue("wut")))
  }

  test("intOverflowTransformsSumToLong") {
    val halfInt= Int.MaxValue
    val result = aggregateOn(intValue(halfInt), intValue(halfInt), intValue(halfInt))
    val expected = 3L * halfInt
    result should equal(longValue(expected))
  }

  test("typesArentUnnecessaryWidened") {
    val thirdOfMaxInt: Int = Int.MaxValue / 3
    val result = aggregateOn(intValue(thirdOfMaxInt), intValue(thirdOfMaxInt))
    val expected = thirdOfMaxInt + thirdOfMaxInt
    result should equal(longValue(expected))
    result shouldBe a [LongValue]
  }
}
