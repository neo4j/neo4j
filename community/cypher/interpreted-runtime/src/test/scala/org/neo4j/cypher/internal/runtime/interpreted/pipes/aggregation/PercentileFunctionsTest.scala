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
package org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, Literal, NumericHelper, Variable}
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.doubleValue

trait PercentileTest {
  val state = QueryStateHelper.empty

  def createAggregator(inner: Expression, percentile: Expression): AggregationFunction

  def getPercentile(percentile: Double, values: List[Any]): AnyValue = {
    val func = createAggregator(Variable("x"), Literal(percentile))
    values.foreach(value => {
      func(ExecutionContext.from("x" -> ValueUtils.of(value)), QueryStateHelper.empty)
    })
    func.result(state)
  }
}

class PercentileDiscTest extends CypherFunSuite with PercentileTest {
  def createAggregator(inner: Expression, perc: Expression) = new PercentileDiscFunction(inner, perc)

  test("singleOne") {
    val values = List(1.0)
    getPercentile(0.0, values) should equal(doubleValue(1.0))
    getPercentile(0.50, values) should equal(doubleValue(1.0))
    getPercentile(0.99, values) should equal(doubleValue(1.0))
    getPercentile(1.00, values) should equal(doubleValue(1.0))
  }

  test("manyOnes") {
    val values = List(1.0, 1.0)
    getPercentile(0.0, values) should equal(doubleValue(1.0))
    getPercentile(0.50, values) should equal(doubleValue(1.0))
    getPercentile(0.99, values) should equal(doubleValue(1.0))
    getPercentile(1.00, values) should equal(doubleValue(1.0))
  }

  test("oneTwoThree") {
    val values = List(1.0, 2.0, 3.0)
    getPercentile(0.00, values) should equal(doubleValue(1.0))
    getPercentile(0.25, values) should equal(doubleValue(1.0))
    getPercentile(0.33, values) should equal(doubleValue(1.0))
    getPercentile(0.50, values) should equal(doubleValue(2.0))
    getPercentile(0.66, values) should equal(doubleValue(2.0))
    getPercentile(0.75, values) should equal(doubleValue(3.0))
    getPercentile(0.99, values) should equal(doubleValue(3.0))
    getPercentile(1.00, values) should equal(doubleValue(3.0))
  }

  test("oneTwoThreeFour") {
    val values = List(1.0, 2.0, 3.0, 4.0)
    getPercentile(0.00, values) should equal(doubleValue(1.0))
    getPercentile(0.25, values) should equal(doubleValue(1.0))
    getPercentile(0.33, values) should equal(doubleValue(2.0))
    getPercentile(0.50, values) should equal(doubleValue(2.0))
    getPercentile(0.66, values) should equal(doubleValue(3.0))
    getPercentile(0.75, values) should equal(doubleValue(3.0))
    getPercentile(0.99, values) should equal(doubleValue(4.0))
    getPercentile(1.00, values) should equal(doubleValue(4.0))
  }

  test("oneTwoThreeFourFive") {
    val values = List(1.0, 2.0, 3.0, 4.0, 5.0)
    getPercentile(0.00, values) should equal(doubleValue(1.0))
    getPercentile(0.25, values) should equal(doubleValue(2.0))
    getPercentile(0.33, values) should equal(doubleValue(2.0))
    getPercentile(0.50, values) should equal(doubleValue(3.0))
    getPercentile(0.66, values) should equal(doubleValue(4.0))
    getPercentile(0.75, values) should equal(doubleValue(4.0))
    getPercentile(0.99, values) should equal(doubleValue(5.0))
    getPercentile(1.00, values) should equal(doubleValue(5.0))
  }

  test("oneTwoThreeFourFiveSix") {
    val values = List(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)
    getPercentile(0.00, values) should equal(doubleValue(1.0))
    getPercentile(0.25, values) should equal(doubleValue(2.0))
    getPercentile(0.33, values) should equal(doubleValue(2.0))
    getPercentile(0.50, values) should equal(doubleValue(3.0))
    getPercentile(0.66, values) should equal(doubleValue(4.0))
    getPercentile(0.75, values) should equal(doubleValue(5.0))
    getPercentile(0.99, values) should equal(doubleValue(6.0))
    getPercentile(1.00, values) should equal(doubleValue(6.0))
  }

  test("oneTwoThreeFourFiveSixSeven") {
    val values = List(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0)
    getPercentile(0.00, values) should equal(doubleValue(1.0))
    getPercentile(0.25, values) should equal(doubleValue(2.0))
    getPercentile(0.33, values) should equal(doubleValue(3.0))
    getPercentile(0.50, values) should equal(doubleValue(4.0))
    getPercentile(0.66, values) should equal(doubleValue(5.0))
    getPercentile(0.75, values) should equal(doubleValue(6.0))
    getPercentile(0.99, values) should equal(doubleValue(7.0))
    getPercentile(1.00, values) should equal(doubleValue(7.0))
  }
}

class PercentileContTest extends CypherFunSuite with PercentileTest with NumericHelper {
  def createAggregator(inner: Expression, perc:Expression) = new PercentileContFunction(inner, perc)

  test("singleOne") {
    val values = List(1.0)
    asDouble(getPercentile(0.0, values)).doubleValue() should equal(1.0 +- .01)
    asDouble(getPercentile(0.50, values)).doubleValue() should equal(1.0 +- .01)
    asDouble(getPercentile(0.99, values)).doubleValue() should equal(1.0 +- .01)
    asDouble(getPercentile(1.00, values)).doubleValue() should equal(1.0 +- .01)
  }

  test("manyOnes") {
    val values = List(1.0, 1.0)
    asDouble(getPercentile(0.0, values)).doubleValue() should equal(1.0 +- .01)
    asDouble(getPercentile(0.50, values)).doubleValue() should equal(1.0 +- .01)
    asDouble(getPercentile(0.99, values)).doubleValue() should equal(1.0 +- .01)
    asDouble(getPercentile(1.00, values)).doubleValue() should equal(1.0 +- .01)
  }

  test("oneTwoThree") {
    val values = List(1.0, 2.0, 3.0)
    asDouble(getPercentile(0.0, values)).doubleValue() should equal(1.0 +- .01)
    asDouble(getPercentile(0.25, values)).doubleValue() should equal(1.5 +- .01)
    asDouble(getPercentile(0.33, values)).doubleValue() should equal(1.66 +- .01)
    asDouble(getPercentile(0.50, values)).doubleValue() should equal(2.0 +- .01)
    asDouble(getPercentile(0.66, values)).doubleValue() should equal(2.32 +- .01)
    asDouble(getPercentile(0.75, values)).doubleValue() should equal(2.5 +- .01)
    asDouble(getPercentile(0.99, values)).doubleValue() should equal(2.98 +- .01)
    asDouble(getPercentile(1.00, values)).doubleValue() should equal(3.0 +- .01)
  }

  test("oneTwoThreeFour") {
    val values = List(1.0, 2.0, 3.0, 4.0)
    asDouble(getPercentile(0.0, values)).doubleValue() should equal(1.0 +- .01)
    asDouble(getPercentile(0.25, values)).doubleValue() should equal(1.75 +- .01)
    asDouble(getPercentile(0.33, values)).doubleValue() should equal(1.99 +- .01)
    asDouble(getPercentile(0.50, values)).doubleValue() should equal(2.5 +- .01)
    asDouble(getPercentile(0.66, values)).doubleValue() should equal(2.98 +- .01)
    asDouble(getPercentile(0.75, values)).doubleValue() should equal(3.25 +- .01)
    asDouble(getPercentile(0.99, values)).doubleValue() should equal(3.97 +- .01)
    asDouble(getPercentile(1.00, values)).doubleValue() should equal(4.0 +- .01)
  }

  test("oneTwoThreeFourFive") {
    val values = List(1.0, 2.0, 3.0, 4.0, 5.0)
    asDouble(getPercentile(0.0, values)).doubleValue() should equal(1.0 +- .01)
    asDouble(getPercentile(0.25, values)).doubleValue() should equal(2.0 +- .01)
    asDouble(getPercentile(0.33, values)).doubleValue() should equal(2.32 +- .01)
    asDouble(getPercentile(0.50, values)).doubleValue() should equal(3.0 +- .01)
    asDouble(getPercentile(0.66, values)).doubleValue() should equal(3.64 +- .01)
    asDouble(getPercentile(0.75, values)).doubleValue() should equal(4.0 +- .01)
    asDouble(getPercentile(0.99, values)).doubleValue() should equal(4.96 +- .01)
    asDouble(getPercentile(1.00, values)).doubleValue() should equal(5.0 +- .01)
  }

  test("oneTwoThreeFourFiveSix") {
    val values = List(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)
    asDouble(getPercentile(0.0, values)).doubleValue() should equal(1.0 +- .01)
    asDouble(getPercentile(0.25, values)).doubleValue() should equal(2.25 +- .01)
    asDouble(getPercentile(0.33, values)).doubleValue() should equal(2.65 +- .01)
    asDouble(getPercentile(0.50, values)).doubleValue() should equal(3.5 +- .01)
    asDouble(getPercentile(0.66, values)).doubleValue() should equal(4.3 +- .01)
    asDouble(getPercentile(0.75, values)).doubleValue() should equal(4.75 +- .01)
    asDouble(getPercentile(0.99, values)).doubleValue() should equal(5.95 +- .01)
    asDouble(getPercentile(1.00, values)).doubleValue() should equal(6.0 +- .01)
  }

  test("oneTwoThreeFourFiveSixSeven") {
    val values = List(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0)
    asDouble(getPercentile(0.0, values)).doubleValue() should equal(1.0 +- .01)
    asDouble(getPercentile(0.25, values)).doubleValue() should equal(2.5 +- .01)
    asDouble(getPercentile(0.33, values)).doubleValue() should equal(2.98 +- .01)
    asDouble(getPercentile(0.50, values)).doubleValue() should equal(4.0 +- .01)
    asDouble(getPercentile(0.66, values)).doubleValue() should equal(4.96 +- .01)
    asDouble(getPercentile(0.75, values)).doubleValue() should equal(5.5 +- .01)
    asDouble(getPercentile(0.99, values)).doubleValue() should equal(6.94 +- .01)
    asDouble(getPercentile(1.00, values)).doubleValue() should equal(7.0 +- .01)
  }
}
