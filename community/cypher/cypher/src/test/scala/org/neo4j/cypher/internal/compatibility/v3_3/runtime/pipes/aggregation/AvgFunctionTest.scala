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
import org.neo4j.values.storable.Values.{NO_VALUE, doubleValue, intValue, longValue}

class AvgFunctionTest extends CypherFunSuite with AggregateTest {
  def createAggregator(inner: Expression) = new AvgFunction(inner)

  test("singleOne") {
    val result = aggregateOn(intValue(1))

    result should equal(doubleValue(1.0))
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
    val result = aggregateOn(longValue(Long.MaxValue / 2),longValue(Long.MaxValue / 2), longValue(Long.MaxValue / 2))

    result should equal(doubleValue(Long.MaxValue / 2))
  }

  test("onEmpty") {
    val result = aggregateOn()

    result should equal(NO_VALUE)
  }
}
