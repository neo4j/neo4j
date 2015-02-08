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

import org.junit.Test
import org.junit.Assert._
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Expression

class AvgFunctionTest extends AggregateTest {
  def createAggregator(inner: Expression) = new AvgFunction(inner)

  @Test def singleOne() {
    val result = aggregateOn(1)

    assertEquals(1.0, result)
  }

  @Test def allOnesAvgIsOne() {
    val result = aggregateOn(1, 1)

    assertEquals(1.0, result)
  }

  @Test def twoAndEightAvgIs10() {
    val result = aggregateOn(2, 8)

    assertEquals(5.0, result)
  }

  @Test def negativeOneIsStillOk() {
    val result = aggregateOn(-1)

    assertEquals(-1.0, result)
  }

  @Test def ZeroIsAnOKAvg() {
    val result = aggregateOn(-10, 10)

    assertEquals(0.0, result)
  }

  @Test def ADoubleInTheListTurnsTheAvgToDouble() {
    val result = aggregateOn(1, 1.0, 1)

    assertEquals(1.0, result)
  }

  @Test def nullDoesntChangeThings() {
    val result = aggregateOn(3, null, 6)

    assertEquals(4.5, result)
  }
}