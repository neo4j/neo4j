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
package org.neo4j.cypher.pipes.aggregation

/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import org.junit.Assert._
import org.junit.Test
import org.neo4j.cypher.SyntaxException
import org.scalatest.junit.JUnitSuite
import org.neo4j.cypher.commands.EntityValue

class MinFunctionTest extends JUnitSuite {
  @Test def singleValueReturnsThatNumber() {
    val result = minOn(1)

    assertEquals(1, result)
    assertTrue(result.isInstanceOf[Int])
  }

  @Test def singleValueOfDecimalReturnsDecimal() {
    val result = minOn(1.0d)

    assertEquals(1.0, result)
  }

  @Test def mixesOfTypesIsOK() {
    val result = minOn(1, 2.0d)

    assertEquals(1, result)
  }

  @Test def longListOfMixedStuff() {
    val result = minOn(100, 230.0d, 56, 237, 23)

    assertEquals(23, result)
  }

  @Test def nullDoesNotChangeTheSum() {
    val result = minOn(1, null, 10)

    assertEquals(1, result)
  }

  @Test(expected = classOf[SyntaxException]) def noNumberValuesThrowAnException() {
    minOn(1, "wut")
  }

  def minOn(values: Any*): Any = {
    val func = new MinFunction(EntityValue("x"))

    values.foreach(value => {
      func(Map("x" -> value))
    })

    func.result
  }
}