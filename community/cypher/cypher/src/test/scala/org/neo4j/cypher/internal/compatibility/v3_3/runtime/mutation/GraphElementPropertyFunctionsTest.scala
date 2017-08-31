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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.mutation

import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.javacompat.ValueUtils
import org.neo4j.values.storable.ArrayValue
import org.neo4j.values.storable.Values._

class GraphElementPropertyFunctionsTest extends CypherFunSuite with GraphElementPropertyFunctions {

  val byte: Byte = 1
  val short: Short = 1
  val int: Int = 1
  val long: Long = 1
  val float: Float = 1
  val double: Double = 1

  test("checks") {
    byteArray(Array[Byte](1, 1)) ==> (byte, byte)
    shortArray(Array[Short](1, 1)) ==> (byte, short)
    intArray(Array[Int](1, 1)) ==> (byte, int)
    longArray(Array[Long](1, 1)) ==> (byte, long)
    floatArray(Array[Float](1, 1)) ==> (byte, float)
    doubleArray(Array[Double](1, 1)) ==> (byte, double)

    shortArray(Array[Short](1, 1)) ==> (short, byte)
    shortArray(Array[Short](1, 1)) ==> (short, short)
    intArray(Array[Int](1, 1)) ==> (short, int)
    longArray(Array[Long](1, 1)) ==> (short, long)
    floatArray(Array[Float](1, 1)) ==> (short, float)
    doubleArray(Array[Double](1, 1)) ==> (short, double)

    intArray(Array[Int](1, 1)) ==> (int, byte)
    intArray(Array[Int](1, 1)) ==> (int, short)
    intArray(Array[Int](1, 1)) ==> (int, int)
    longArray(Array[Long](1, 1)) ==> (int, long)
    floatArray(Array[Float](1, 1)) ==> (int, float)
    doubleArray(Array[Double](1, 1)) ==> (int, double)

    longArray(Array[Long](1, 1)) ==> (long, byte)
    longArray(Array[Long](1, 1)) ==> (long, short)
    longArray(Array[Long](1, 1)) ==> (long, int)
    longArray(Array[Long](1, 1)) ==> (long, long)
    floatArray(Array[Float](1, 1)) ==> (long, float)
    doubleArray(Array[Double](1, 1)) ==> (long, double)

    floatArray(Array[Float](1, 1)) ==> (float, byte)
    floatArray(Array[Float](1, 1)) ==> (float, short)
    floatArray(Array[Float](1, 1)) ==> (float, int)
    floatArray(Array[Float](1, 1)) ==> (float, long)
    floatArray(Array[Float](1, 1)) ==> (float, float)
    doubleArray(Array[Double](1, 1)) ==> (float, double)

    doubleArray(Array[Double](1, 1)) ==> (double, byte)
    doubleArray(Array[Double](1, 1)) ==> (double, short)
    doubleArray(Array[Double](1, 1)) ==> (double, int)
    doubleArray(Array[Double](1, 1)) ==> (double, long)
    doubleArray(Array[Double](1, 1)) ==> (double, float)
    doubleArray(Array[Double](1, 1)) ==> (float, double)
  }

  implicit class CheckValeNeoSafe(expected: ArrayValue) {
    import scala.collection.JavaConverters._
    def ==>(vals: Any*): Unit =
      makeValueNeoSafe(ValueUtils.asListValue(vals.asJava)) should equal(expected)
  }
}
