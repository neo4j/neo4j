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
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values.{doubleValue, intValue, stringValue}
import org.neo4j.values.storable.{IntValue, Values}

class MinFunctionTest extends CypherFunSuite with AggregateTest {
  test("singleValueReturnsThatNumber") {
    val result = aggregateOn(intValue(1))

    result should equal(intValue(1))
    result shouldBe an [IntValue]
  }

  test("singleValueOfDecimalReturnsDecimal") {
    val result = aggregateOn(doubleValue(1.0d))

    result should equal(doubleValue(1.0))
  }

  test("mixesOfTypesIsOK") {
    val result = aggregateOn(intValue(1), doubleValue(2.0d))

    result should equal(intValue(1))
  }

  test("longListOfMixedStuff") {
    val result = aggregateOn(intValue(100), doubleValue(230.0d), intValue(56), intValue(237), intValue(23))

    result should equal(intValue(23))
  }

  test("nullDoesNotChangeTheSum") {
    val result = aggregateOn(intValue(1), Values.NO_VALUE, intValue(10))

    result should equal(intValue(1))
  }

  test("mixed numbers and strings works fine") {
    val result = aggregateOn(intValue(1), stringValue("wut"))

    result shouldBe stringValue("wut")
  }

  test("aggregating strings work") {
    val result = aggregateOn(stringValue("abc"), stringValue("a"), stringValue("b"), stringValue("B"), stringValue("abc1"))

    result should equal(stringValue("B"))
  }

  test("nulls are simply skipped") {
    val result = aggregateOn(stringValue("abc"), stringValue("a"), stringValue("b"), Values.NO_VALUE, stringValue("abc1"))

    result should equal(stringValue("a"))
  }

  def createAggregator(inner: Expression) = new MinFunction(inner)
}
