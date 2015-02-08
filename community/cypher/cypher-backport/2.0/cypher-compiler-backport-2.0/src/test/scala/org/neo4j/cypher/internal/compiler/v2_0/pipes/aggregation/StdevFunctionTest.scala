/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.pipes.aggregation

import org.neo4j.cypher.internal.compiler.v2_0._
import commands.expressions.{Expression, Identifier, NumericHelper}
import pipes.QueryStateHelper
import org.junit.Test
import org.junit.Assert._

abstract class StdevTest {
  def createAggregator(inner: Expression): AggregationFunction

  def getStdev(values: List[Any]): Double = {
    val func = createAggregator(Identifier("x"))
    values.foreach(value => {
      func(ExecutionContext.from("x" -> value))(QueryStateHelper.empty)
    })
    func.result match {
      case x:Double => x
      case _ => -99.0
    }
  }
}

class StdevSampleTest extends StdevTest {
  def createAggregator(inner: Expression) = new StdevFunction(inner, false)

  @Test def singleOne() {
    val values = List(1)
    assertEquals(0.0, getStdev(values), 0.000001)
  }

  @Test def manyOnes() {
    val values = List(1, 1)
    assertEquals(0.0, getStdev(values), 0.000001)
  }

  @Test def oneTwoThree() {
    val values = List(1, 2, 3)
    assertEquals(1.0, getStdev(values), 0.000001)
  }

  @Test def oneTwoThreeFour() {
    val values = List(1, 2, 3, 4)
    assertEquals(1.29099444874, getStdev(values), 0.000001)
  }

  @Test def oneTwoThreeFourFive() {
    val values = List(1, 2, 3, 4, 5)
    assertEquals(1.58113883008, getStdev(values), 0.000001)
  }

  @Test def oneTwoThreeFourFiveSix() {
    val values = List(1, 2, 3, 4, 5, 6)
    assertEquals(1.87082869339, getStdev(values), 0.000001)
  }

  @Test def oneTwoThreeFourFiveSixSeven() {
    val values = List(1, 2, 3, 4, 5, 6, 7)
    assertEquals(2.16024689947, getStdev(values), 0.000001)
  }
}

class StdevPopulationTest extends StdevTest with NumericHelper{
  def createAggregator(inner: Expression) = new StdevFunction(inner, true)

  @Test def singleOne() {
    val values = List(1)
    assertEquals(0.0, getStdev(values), 0.000001)
  }

  @Test def manyOnes() {
    val values = List(1,1)
    assertEquals(0.0, getStdev(values), 0.000001)
  }

  @Test def oneTwoThree() {
    val values = List(1,2,3)
    assertEquals(0.816496580928, getStdev(values), 0.000001)
  }

  @Test def oneTwoThreeFour() {
    val values = List(1,2,3,4)
    assertEquals(1.11803398875, getStdev(values), 0.000001)
  }

  @Test def oneTwoThreeFourFive() {
    val values = List(1, 2, 3, 4, 5)
    assertEquals(1.41421356237, getStdev(values), 0.000001)
  }

  @Test def oneTwoThreeFourFiveSix() {
    val values = List(1, 2, 3, 4, 5, 6)
    assertEquals(1.70782512766, getStdev(values), 0.000001)
  }

  @Test def oneTwoThreeFourFiveSixSeven() {
    val values = List(1, 2, 3, 4, 5, 6, 7)
    assertEquals(2.0, getStdev(values), 0.000001)
  }
}
