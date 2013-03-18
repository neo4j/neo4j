/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.pipes.aggregation

import org.junit.Test
import org.junit.Assert._
import org.neo4j.cypher.internal.commands.expressions.{Expression, Identifier, Literal, NumericHelper}
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.pipes.{QueryStateHelper, QueryState}

abstract class PercentileTest {
  def createAggregator(inner: Expression, percentile: Expression): AggregationFunction

  def getPercentile(percentile: Double, values: List[Any]): Any = {
    val func = createAggregator(Identifier("x"), Literal(percentile))
    values.foreach(value => {
      func(ExecutionContext.from("x" -> value))(QueryStateHelper.empty)
    })
    func.result
  }
}

class PercentileDiscTest extends PercentileTest {
  def createAggregator(inner: Expression, perc:Expression) = new PercentileDiscFunction(inner, perc)

  @Test def singleOne() {
    val values = List(1.0)
    assertEquals(1.0, getPercentile(0.0, values))
    assertEquals(1.0, getPercentile(0.50, values))
    assertEquals(1.0, getPercentile(0.99, values))
    assertEquals(1.0, getPercentile(1.00, values))
  }

  @Test def manyOnes() {
    val values = List(1.0, 1.0)
    assertEquals(1.0, getPercentile(0.0, values))
    assertEquals(1.0, getPercentile(0.50, values))
    assertEquals(1.0, getPercentile(0.99, values))
    assertEquals(1.0, getPercentile(1.00, values))
  }

  @Test def oneTwoThree() {
    val values = List(1.0, 2.0, 3.0)
    assertEquals("0.00", 1.0, getPercentile(0.0, values))
    assertEquals("0.25", 1.0, getPercentile(0.25, values))
    assertEquals("0.33", 1.0, getPercentile(0.33, values))
    assertEquals("0.50", 2.0, getPercentile(0.50, values))
    assertEquals("0.66", 2.0, getPercentile(0.66, values))
    assertEquals("0.75", 3.0, getPercentile(0.75, values))
    assertEquals("0.99", 3.0, getPercentile(0.99, values))
    assertEquals("1.00", 3.0, getPercentile(1.00, values))
  }

  @Test def oneTwoThreeFour() {
    val values = List(1.0, 2.0, 3.0, 4.0)
    assertEquals("0.00", 1.0, getPercentile(0.0, values))
    assertEquals("0.25", 1.0, getPercentile(0.25, values)) 
    assertEquals("0.33", 2.0, getPercentile(0.33, values))
    assertEquals("0.50", 2.0, getPercentile(0.50, values))
    assertEquals("0.66", 3.0, getPercentile(0.66, values))
    assertEquals("0.75", 3.0, getPercentile(0.75, values)) 
    assertEquals("0.99", 4.0, getPercentile(0.99, values))
    assertEquals("1.00", 4.0, getPercentile(1.00, values))
  }

  @Test def oneTwoThreeFourFive() {
    val values = List(1.0, 2.0, 3.0, 4.0, 5.0)
    assertEquals("0.0", 1.0, getPercentile(0.0, values))
    assertEquals("0.25", 2.0, getPercentile(0.25, values))
    assertEquals("0.33", 2.0, getPercentile(0.33, values))
    assertEquals("0.50", 3.0, getPercentile(0.50, values))
    assertEquals("0.66", 4.0, getPercentile(0.66, values))
    assertEquals("0.75", 4.0, getPercentile(0.75, values))
    assertEquals("0.99", 5.0, getPercentile(0.99, values))
    assertEquals("1.00", 5.0, getPercentile(1.00, values))
  }

  @Test def oneTwoThreeFourFiveSix() {
    val values = List(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)
    assertEquals("0.0", 1.0, getPercentile(0.0, values))
    assertEquals("0.25", 2.0, getPercentile(0.25, values))
    assertEquals("0.33", 2.0, getPercentile(0.33, values))
    assertEquals("0.50", 3.0, getPercentile(0.50, values))
    assertEquals("0.66", 4.0, getPercentile(0.66, values))
    assertEquals("0.75", 5.0, getPercentile(0.75, values))
    assertEquals("0.99", 6.0, getPercentile(0.99, values))
    assertEquals("1.00", 6.0, getPercentile(1.00, values))
  }

  @Test def oneTwoThreeFourFiveSixSeven() {
    val values = List(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0)
    assertEquals("0.0", 1.0, getPercentile(0.0, values))
    assertEquals("0.25", 2.0, getPercentile(0.25, values))
    assertEquals("0.33", 3.0, getPercentile(0.33, values))
    assertEquals("0.50", 4.0, getPercentile(0.50, values))
    assertEquals("0.66", 5.0, getPercentile(0.66, values))
    assertEquals("0.75", 6.0, getPercentile(0.75, values))
    assertEquals("0.99", 7.0, getPercentile(0.99, values))
    assertEquals("1.00", 7.0, getPercentile(1.00, values))
  }
}

class PercentileContTest extends PercentileTest with NumericHelper{
  def createAggregator(inner: Expression, perc:Expression) = new PercentileContFunction(inner, perc)

  @Test def singleOne() {
    val values = List(1.0)
    assertEquals("0.0", 1.0, asDouble(getPercentile(0.0, values)), .01)
    assertEquals("0.50", 1.0, asDouble(getPercentile(0.50, values)), .01)
    assertEquals("0.99", 1.0, asDouble(getPercentile(0.99, values)), .01)
    assertEquals("1.00", 1.0, asDouble(getPercentile(1.00, values)), .01)
  }

  @Test def manyOnes() {
    val values = List(1.0, 1.0)
    assertEquals("0.0", 1.0, asDouble(getPercentile(0.0, values)), .01)
    assertEquals("0.5", 1.0, asDouble(getPercentile(0.50, values)), .01)
    assertEquals("0.99", 1.0, asDouble(getPercentile(0.99, values)), .01)
    assertEquals("1.00", 1.0, asDouble(getPercentile(1.00, values)), .01)
  }

  @Test def oneTwoThree() {
    val values = List(1.0, 2.0, 3.0)
    assertEquals("0.0", 1.0, asDouble(getPercentile(0.0, values)), .01)
    assertEquals("0.25", 1.5, asDouble(getPercentile(0.25, values)), .01)
    assertEquals("0.33", 1.66, asDouble(getPercentile(0.33, values)), .01)
    assertEquals("0.50", 2.0, asDouble(getPercentile(0.50, values)), .01)
    assertEquals("0.66", 2.32, asDouble(getPercentile(0.66, values)), .01)
    assertEquals("0.75", 2.5, asDouble(getPercentile(0.75, values)), .01)
    assertEquals("0.99", 2.98, asDouble(getPercentile(0.99, values)), .01)
    assertEquals("1.00", 3.0, asDouble(getPercentile(1.00, values)), .01)
  }

  @Test def oneTwoThreeFour() {
    val values = List(1.0, 2.0, 3.0, 4.0)
    assertEquals("0.0", 1.0, asDouble(getPercentile(0.0, values)), .01)
    assertEquals("0.25", 1.75, asDouble(getPercentile(0.25, values)), .01)
    assertEquals("0.33", 1.99, asDouble(getPercentile(0.33, values)), .01)
    assertEquals("0.50", 2.5, asDouble(getPercentile(0.50, values)), .01)
    assertEquals("0.66", 2.98, asDouble(getPercentile(0.66, values)), .01)
    assertEquals("0.75", 3.25, asDouble(getPercentile(0.75, values)), .01)
    assertEquals("0.99", 3.97, asDouble(getPercentile(0.99, values)), .01)
    assertEquals("1.00", 4.0, asDouble(getPercentile(1.00, values)), .01)
  }

  @Test def oneTwoThreeFourFive() {
    val values = List(1.0, 2.0, 3.0, 4.0, 5.0)
    assertEquals("0.0", 1.0, asDouble(getPercentile(0.0, values)), .01)
    assertEquals("0.25", 2.0, asDouble(getPercentile(0.25, values)), .01)
    assertEquals("0.33", 2.32, asDouble(getPercentile(0.33, values)), .01)
    assertEquals("0.50", 3.0, asDouble(getPercentile(0.50, values)), .01)
    assertEquals("0.66", 3.64, asDouble(getPercentile(0.66, values)), .01)
    assertEquals("0.75", 4.0, asDouble(getPercentile(0.75, values)), .01)
    assertEquals("0.99", 4.96, asDouble(getPercentile(0.99, values)), .01)
    assertEquals("1.00", 5.0, asDouble(getPercentile(1.00, values)), .01)
  }

  @Test def oneTwoThreeFourFiveSix() {
    val values = List(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)
    assertEquals("0.0", 1.0, asDouble(getPercentile(0.0, values)), .01)
    assertEquals("0.25", 2.25, asDouble(getPercentile(0.25, values)), .01)
    assertEquals("0.33", 2.65, asDouble(getPercentile(0.33, values)), .01)
    assertEquals("0.50", 3.5, asDouble(getPercentile(0.50, values)), .01)
    assertEquals("0.66", 4.3, asDouble(getPercentile(0.66, values)), .01)
    assertEquals("0.75", 4.75, asDouble(getPercentile(0.75, values)), .01)
    assertEquals("0.99", 5.95, asDouble(getPercentile(0.99, values)), .01)
    assertEquals("1.00", 6.0, asDouble(getPercentile(1.00, values)), .01)
  }

  @Test def oneTwoThreeFourFiveSixSeven() {
    val values = List(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0)
    assertEquals("0.0", 1.0, asDouble(getPercentile(0.0, values)), .01)
    assertEquals("0.25", 2.5, asDouble(getPercentile(0.25, values)), .01)
    assertEquals("0.33", 2.98, asDouble(getPercentile(0.33, values)), .01)
    assertEquals("0.50", 4.0, asDouble(getPercentile(0.50, values)), .01)
    assertEquals("0.66", 4.96, asDouble(getPercentile(0.66, values)), .01)
    assertEquals("0.75", 5.5, asDouble(getPercentile(0.75, values)), .01)
    assertEquals("0.99", 6.94, asDouble(getPercentile(0.99, values)), .01)
    assertEquals("1.00", 7.0, asDouble(getPercentile(1.00, values)), .01)
  }
}
