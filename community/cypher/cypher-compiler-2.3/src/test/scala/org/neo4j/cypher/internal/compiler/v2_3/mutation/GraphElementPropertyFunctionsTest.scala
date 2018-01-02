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
package org.neo4j.cypher.internal.compiler.v2_3.mutation

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class GraphElementPropertyFunctionsTest extends CypherFunSuite with GraphElementPropertyFunctions {

  val byte: Byte = 1
  val short: Short = 1
  val int: Int = 1
  val long: Long = 1
  val float: Float = 1
  val double: Double = 1

  test("checks") {
    Array[Byte](1, 1) ==> (byte, byte)
    Array[Short](1, 1) ==> (byte, short)
    Array[Int](1, 1) ==> (byte, int)
    Array[Long](1, 1) ==> (byte, long)
    Array[Float](1, 1) ==> (byte, float)
    Array[Double](1, 1) ==> (byte, double)

    Array[Short](1, 1) ==> (short, byte)
    Array[Short](1, 1) ==> (short, short)
    Array[Int](1, 1) ==> (short, int)
    Array[Long](1, 1) ==> (short, long)
    Array[Float](1, 1) ==> (short, float)
    Array[Double](1, 1) ==> (short, double)

    Array[Int](1, 1) ==> (int, byte)
    Array[Int](1, 1) ==> (int, short)
    Array[Int](1, 1) ==> (int, int)
    Array[Long](1, 1) ==> (int, long)
    Array[Float](1, 1) ==> (int, float)
    Array[Double](1, 1) ==> (int, double)

    Array[Long](1, 1) ==> (long, byte)
    Array[Long](1, 1) ==> (long, short)
    Array[Long](1, 1) ==> (long, int)
    Array[Long](1, 1) ==> (long, long)
    Array[Float](1, 1) ==> (long, float)
    Array[Double](1, 1) ==> (long, double)

    Array[Float](1, 1) ==> (float, byte)
    Array[Float](1, 1) ==> (float, short)
    Array[Float](1, 1) ==> (float, int)
    Array[Float](1, 1) ==> (float, long)
    Array[Float](1, 1) ==> (float, float)
    Array[Double](1, 1) ==> (float, double)

    Array[Double](1, 1) ==> (double, byte)
    Array[Double](1, 1) ==> (double, short)
    Array[Double](1, 1) ==> (double, int)
    Array[Double](1, 1) ==> (double, long)
    Array[Double](1, 1) ==> (double, float)
    Array[Double](1, 1) ==> (float, double)
  }

  implicit class CheckValeNeoSafe(expected: Array[_]) {

    def ==>(vals: Any*) =
      makeValueNeoSafe(vals) should equal(expected)
  }
}
