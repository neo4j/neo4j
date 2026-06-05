/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation

import org.neo4j.cypher.internal.runtime.interpreted.InterpretedRuntimeTestSuite
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.functionArgumentGqlException
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.doubleValue
import org.neo4j.values.storable.Values.intValue
import org.neo4j.values.storable.Values.longValue

class AvgFunctionTest extends InterpretedRuntimeTestSuite with AggregateTest {
  def createAggregator(inner: Expression) = new AvgFunction(inner)

  test("singleOne") {
    val result = aggregateOn(intValue(1))

    result should equal(doubleValue(1.0))
  }

  test("singleDur") {
    val durationValue = DurationValue.duration(0, 0, 1, 0)
    val result = aggregateOn(durationValue)

    result should equal(durationValue)
  }

  test("correctDurAvg") {
    val durationValue = DurationValue.duration(0, 3, 0, 1)
    val durationValue2 = DurationValue.duration(0, 2, 2, 1)
    val result = aggregateOn(durationValue, durationValue2)

    result should equal(DurationValue.duration(0, 2, 12 * 3600 + 1, 1))
  }

  test("cantMixDurationAndNumber - number after duration") {
    val durationValue = DurationValue.duration(0, 0, 0, 1)
    val numberValue = longValue(1)
    val exception = intercept[CypherTypeException] {
      aggregateOn(durationValue, numberValue)
    }
    exception should be(functionArgumentGqlException(
      "AVG(x) cannot mix number and duration",
      "AVG(x)",
      "Expected the value 1 to be of type DURATION, but was of type INTEGER NOT NULL."
    ))
  }

  test("cantMixDurationAndNumber - duration after number") {
    val durationValue = DurationValue.duration(0, 0, 1, 0)
    val numberValue = longValue(1)
    val exception = intercept[CypherTypeException] {
      aggregateOn(numberValue, durationValue)
    }
    exception should be(functionArgumentGqlException(
      "AVG(x) cannot mix number and duration",
      "AVG(x)",
      "Expected the value PT1S to be of type INTEGER or FLOAT, but was of type DURATION NOT NULL."
    ))
  }

  test("allOnesAvgIsOne") {
    val result = aggregateOn(intValue(1), intValue(1))

    result should equal(doubleValue(1.0))
  }

  test("twoAndEightAvgIs10") {
    val result = aggregateOn(intValue(2), intValue(8))

    result should equal(doubleValue(5.0))
  }

  test("negativeOneIsStillOk") {
    val result = aggregateOn(intValue(-1))

    result should equal(doubleValue(-1.0))
  }

  test("ZeroIsAnOKAvg") {
    val result = aggregateOn(intValue(-10), intValue(10))

    result should equal(doubleValue(0.0))
  }

  test("ADoubleInTheListTurnsTheAvgToDouble") {
    val result = aggregateOn(intValue(1), doubleValue(1.0), intValue(1))

    result should equal(doubleValue(1.0))
  }

  test("nullDoesntChangeThings") {
    val result = aggregateOn(intValue(3), NO_VALUE, intValue(6))

    result should equal(doubleValue(4.5))
  }

  test("noOverflowOnLongListOfLargeNumbers") {
    val result = aggregateOn(longValue(Long.MaxValue / 2), longValue(Long.MaxValue / 2), longValue(Long.MaxValue / 2))

    result should equal(doubleValue((Long.MaxValue / 2).toDouble))
  }

  test("onEmpty") {
    val result = aggregateOn()

    result should equal(NO_VALUE)
  }
}
