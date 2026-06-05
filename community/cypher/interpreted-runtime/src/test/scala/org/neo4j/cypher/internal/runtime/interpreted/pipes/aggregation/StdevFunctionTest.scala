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

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.InterpretedRuntimeTestSuite
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Variable
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.functionArgumentGqlException
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.DoubleValue
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.Values

trait StdevTest {
  val state = QueryStateHelper.empty

  def createAggregator(inner: Expression): AggregationFunction

  def getStdevResult(values: List[Any]): AnyValue = {
    val func = createAggregator(Variable("x"))
    values.foreach(value => {
      func(CypherRow.from("x" -> ValueUtils.of(value)), QueryStateHelper.empty)
    })
    func.result(state)
  }

  def getStdev(values: List[Any]): Double = {
    getStdevResult(values) match {
      case x: DoubleValue => x.doubleValue()
      case _              => -99.0
    }
  }
}

class StdevSampleTest extends InterpretedRuntimeTestSuite with StdevTest {
  def createAggregator(inner: Expression) = new StdevFunction(inner, false)

  test("empty") {
    getStdevResult(List.empty) should equal(Values.NO_VALUE)
  }

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

  test("stdev cannot handle character value") {
    val values = List('a')
    val exception = intercept[CypherTypeException](getStdev(values))
    exception should be(functionArgumentGqlException(
      "STDEV(x) can only handle numerical values or null, but received: org.neo4j.values.storable.CharValue",
      "STDEV(x)",
      "Expected the value \"a\" to be of type INTEGER or FLOAT, but was of type STRING NOT NULL."
    ))
  }

  test("stdev cannot handle string value") {
    val values = List("abc")
    val exception = intercept[CypherTypeException](getStdev(values))
    exception should be(functionArgumentGqlException(
      "STDEV(x) can only handle numerical values or null, but received: org.neo4j.values.storable.UTF8StringValue",
      "STDEV(x)",
      "Expected the value \"abc\" to be of type INTEGER or FLOAT, but was of type STRING NOT NULL."
    ))
  }

  test("stdev cannot handle duration values") {
    val values = List(DurationValue.duration(0, 0, 10, 0))
    val exception = intercept[CypherTypeException](getStdev(values))
    exception should be(functionArgumentGqlException(
      "STDEV(x) can only handle numerical values or null, but received: org.neo4j.values.storable.DurationValue",
      "STDEV(x)",
      "Expected the value PT10S to be of type INTEGER or FLOAT, but was of type DURATION NOT NULL."
    ))
  }
}

class StdevPopulationTest extends InterpretedRuntimeTestSuite with StdevTest {
  def createAggregator(inner: Expression) = new StdevFunction(inner, true)

  test("empty") {
    getStdevResult(List.empty) should equal(Values.NO_VALUE)
  }

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
    getStdev(values) should equal(0.816496580928 +- 0.000001)
  }

  test("oneTwoThreeFour") {
    val values = List(1, 2, 3, 4)
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

  test("stdevp cannot handle character value") {
    val values = List('a')
    val exception = intercept[CypherTypeException](getStdev(values))
    exception should be(functionArgumentGqlException(
      "STDEVP(x) can only handle numerical values or null, but received: org.neo4j.values.storable.CharValue",
      "STDEVP(x)",
      "Expected the value \"a\" to be of type INTEGER or FLOAT, but was of type STRING NOT NULL."
    ))
  }

  test("stdevp cannot handle string value") {
    val values = List("abc")
    val exception = intercept[CypherTypeException](getStdev(values))
    exception should be(functionArgumentGqlException(
      "STDEVP(x) can only handle numerical values or null, but received: org.neo4j.values.storable.UTF8StringValue",
      "STDEVP(x)",
      "Expected the value \"abc\" to be of type INTEGER or FLOAT, but was of type STRING NOT NULL."
    ))
  }

  test("stdevp cannot handle duration values") {
    val values = List(DurationValue.duration(0, 0, 10, 0))
    val exception = intercept[CypherTypeException](getStdev(values))
    exception should be(functionArgumentGqlException(
      "STDEVP(x) can only handle numerical values or null, but received: org.neo4j.values.storable.DurationValue",
      "STDEVP(x)",
      "Expected the value PT10S to be of type INTEGER or FLOAT, but was of type DURATION NOT NULL."
    ))
  }
}
