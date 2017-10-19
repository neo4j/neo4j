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
package org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, NumericHelper, Variable}
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.storable.DoubleValue

trait StdevTest {
  val state = QueryStateHelper.empty

  def createAggregator(inner: Expression): AggregationFunction

  def getStdev(values: List[Any]): Double = {
    val func = createAggregator(Variable("x"))
    values.foreach(value => {
      func(ExecutionContext.from("x" -> ValueUtils.of(value)), QueryStateHelper.empty)
    })
    func.result(state) match {
      case x: DoubleValue => x.doubleValue()
      case _ => -99.0
    }
  }
}

class StdevSampleTest extends CypherFunSuite with StdevTest {
  def createAggregator(inner: Expression) = new StdevFunction(inner, false)

  test("singleOne") {
    val values = List(1)
    getStdev(values) should equal(0.0 +- 0.000001)
  }

  test("manyOnes") {
    val values = List(1, 1)
    getStdev(values) should equal(0.0 +- 0.000001)
  }

  test("oneTwoThree") {
    val values = List(1, 2, 3)
    getStdev(values) should equal(1.0 +- 0.000001)
  }

  test("oneTwoThreeFour") {
    val values = List(1, 2, 3, 4)
    getStdev(values) should equal(1.29099444874 +- 0.000001)
  }

  test("oneTwoThreeFourFive") {
    val values = List(1, 2, 3, 4, 5)
    getStdev(values) should equal(1.58113883008 +- 0.000001)
  }

  test("oneTwoThreeFourFiveSix") {
    val values = List(1, 2, 3, 4, 5, 6)
    getStdev(values) should equal(1.87082869339 +- 0.000001)
  }

  test("oneTwoThreeFourFiveSixSeven") {
    val values = List(1, 2, 3, 4, 5, 6, 7)
    getStdev(values) should equal(2.16024689947 +- 0.000001)
  }
}

class StdevPopulationTest extends CypherFunSuite with StdevTest with NumericHelper {
  def createAggregator(inner: Expression) = new StdevFunction(inner, true)

  test("singleOne") {
    val values = List(1)
    getStdev(values) should equal(0.0 +- 0.000001)
  }

  test("manyOnes") {
    val values = List(1,1)
    getStdev(values) should equal(0.0 +- 0.000001)
  }

  test("oneTwoThree") {
    val values = List(1,2,3)
    getStdev(values) should equal(0.816496580928 +- 0.000001)
  }

  test("oneTwoThreeFour") {
    val values = List(1,2,3,4)
    getStdev(values) should equal(1.11803398875 +- 0.000001)
  }

  test("oneTwoThreeFourFive") {
    val values = List(1, 2, 3, 4, 5)
    getStdev(values) should equal(1.41421356237 +- 0.000001)
  }

  test("oneTwoThreeFourFiveSix") {
    val values = List(1, 2, 3, 4, 5, 6)
    getStdev(values) should equal(1.70782512766 +- 0.000001)
  }

  test("oneTwoThreeFourFiveSixSeven") {
    val values = List(1, 2, 3, 4, 5, 6, 7)
    getStdev(values) should equal(2.0 +- 0.000001)
  }
}
