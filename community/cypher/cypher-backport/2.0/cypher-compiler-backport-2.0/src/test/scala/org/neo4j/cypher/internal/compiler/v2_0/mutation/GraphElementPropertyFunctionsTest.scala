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
package org.neo4j.cypher.internal.compiler.v2_0.mutation

import org.junit.Test
import org.scalatest.Assertions
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import java.util

@RunWith(value = classOf[Parameterized])
class GraphElementPropertyFunctionsTest(given: List[_], expected: Array[_]) extends GraphElementPropertyFunctions with Assertions {

  @Test def test_it() {
    assert(makeValueNeoSafe(given) === expected)
  }
}

object GraphElementPropertyFunctionsTest {

  val byte: Byte = 1
  val short: Short = 1
  val int: Int = 1
  val long: Long = 1
  val float: Float = 1
  val double: Double = 1

  @Parameters(name = "{0}")
  def parameters: util.Collection[Array[AnyRef]] = {
    val list = new util.ArrayList[Array[AnyRef]]()
    def add(expected: Array[_],
            values: Any*) {
      list.add(Array(values.toList, expected))
    }

    add(Array[Byte](1, 1), byte, byte)
    add(Array[Short](1, 1), byte, short)
    add(Array[Int](1, 1), byte, int)
    add(Array[Long](1, 1), byte, long)
    add(Array[Float](1, 1), byte, float)
    add(Array[Double](1, 1), byte, double)

    add(Array[Short](1, 1), short, byte)
    add(Array[Short](1, 1), short, short)
    add(Array[Int](1, 1), short, int)
    add(Array[Long](1, 1), short, long)
    add(Array[Float](1, 1), short, float)
    add(Array[Double](1, 1), short, double)

    add(Array[Int](1, 1), int, byte)
    add(Array[Int](1, 1), int, short)
    add(Array[Int](1, 1), int, int)
    add(Array[Long](1, 1), int, long)
    add(Array[Float](1, 1), int, float)
    add(Array[Double](1, 1), int, double)

    add(Array[Long](1, 1), long, byte)
    add(Array[Long](1, 1), long, short)
    add(Array[Long](1, 1), long, int)
    add(Array[Long](1, 1), long, long)
    add(Array[Float](1, 1), long, float)
    add(Array[Double](1, 1), long, double)

    add(Array[Float](1, 1), float, byte)
    add(Array[Float](1, 1), float, short)
    add(Array[Float](1, 1), float, int)
    add(Array[Float](1, 1), float, long)
    add(Array[Float](1, 1), float, float)
    add(Array[Double](1, 1), float, double)

    add(Array[Double](1, 1), double, byte)
    add(Array[Double](1, 1), double, short)
    add(Array[Double](1, 1), double, int)
    add(Array[Double](1, 1), double, long)
    add(Array[Double](1, 1), double, float)
    add(Array[Double](1, 1), float, double)

    list
  }
}