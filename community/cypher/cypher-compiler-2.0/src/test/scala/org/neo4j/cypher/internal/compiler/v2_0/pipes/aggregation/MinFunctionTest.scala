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

import org.junit.Assert._
import org.junit.Test
import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Expression

class MinFunctionTest extends AggregateTest {
  @Test def singleValueReturnsThatNumber() {
    val result = aggregateOn(1)

    assertEquals(1, result)
    assertTrue(result.isInstanceOf[Int])
  }

  @Test def singleValueOfDecimalReturnsDecimal() {
    val result = aggregateOn(1.0d)

    assertEquals(1.0, result)
  }

  @Test def mixesOfTypesIsOK() {
    val result = aggregateOn(1, 2.0d)

    assertEquals(1, result)
  }

  @Test def longListOfMixedStuff() {
    val result = aggregateOn(100, 230.0d, 56, 237, 23)

    assertEquals(23, result)
  }

  @Test def nullDoesNotChangeTheSum() {
    val result = aggregateOn(1, null, 10)

    assertEquals(1, result)
  }

  @Test(expected = classOf[SyntaxException]) def noNumberValuesThrowAnException() {
    aggregateOn(1, "wut")
  }

  def createAggregator(inner: Expression) = new MinFunction(inner)
}
