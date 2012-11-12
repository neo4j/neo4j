/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import org.neo4j.cypher.commands.Entity

class AvgFunctionTest {
  @Test def singleOne() {
    val result = avgOn(1)

    assertEquals(1.0, result)
  }

  @Test def allOnesAvgIsOne() {
    val result = avgOn(1, 1)

    assertEquals(1.0, result)
  }

  @Test def twoAndEightAvgIs10() {
    val result = avgOn(2, 8)

    assertEquals(5.0, result)
  }

  @Test def negativeOneIsStillOk() {
    val result = avgOn(-1)

    assertEquals(-1.0, result)
  }

  @Test def ZeroIsAnOKAvg() {
    val result = avgOn(-10, 10)

    assertEquals(0.0, result)
  }

  @Test def ADoubleInTheListTurnsTheAvgToDouble() {
    val result = avgOn(1, 1.0, 1)

    assertEquals(1.0, result)
  }

  @Test def nullDoesntChangeThings() {
    val result = avgOn(3, null, 6)

    assertEquals(4.5, result)
  }

  def avgOn(values: Any*): Any = {
    val func = new AvgFunction(Entity("x"))

    values.foreach(value => {
      func(Map("x" -> value))
    })

    func.result
  }
}