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
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class AvgFunctionTest extends CypherFunSuite with AggregateTest {
  def createAggregator(inner: Expression) = new AvgFunction(inner)

  test("singleOne") {
    val result = aggregateOn(1)

    result should equal(1.0)
  }

  test("allOnesAvgIsOne") {
    val result = aggregateOn(1, 1)

    result should equal(1.0)
  }

  test("twoAndEightAvgIs10") {
    val result = aggregateOn(2, 8)

    result should equal(5.0)
  }

  test("negativeOneIsStillOk") {
    val result = aggregateOn(-1)

    result should equal(-1.0)
  }

  test("ZeroIsAnOKAvg") {
    val result = aggregateOn(-10, 10)

    result should equal(0.0)
  }

  test("ADoubleInTheListTurnsTheAvgToDouble") {
    val result = aggregateOn(1, 1.0, 1)

    result should equal(1.0)
  }

  test("nullDoesntChangeThings") {
    val result = aggregateOn(3, null, 6)

    result should equal(4.5)
  }

  test("noOverflowOnLongListOfLargeNumbers") {
    val result = aggregateOn(Long.MaxValue / 2, Long.MaxValue / 2, Long.MaxValue / 2)

    result should equal(Long.MaxValue / 2)
  }

  test("onEmpty") {
    val result = aggregateOn()

    Option(result) should be (None)
  }
}
