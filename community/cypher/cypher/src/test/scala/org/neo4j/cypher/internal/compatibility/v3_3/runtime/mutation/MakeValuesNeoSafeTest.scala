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

import org.neo4j.cypher.internal.frontend.v3_3.CypherTypeException
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite

import scala.Array._

class MakeValuesNeoSafeTest extends CypherFunSuite {

  test("string collection turns into string array") {
    makeValueNeoSafe(Seq("a", "b")) should equal(Array("a", "b"))
  }

  test("empty collection in is empty array") {
    makeValueNeoSafe(Seq()) should equal(Array())
  }

  test("retains type of primitive arrays") {
    Seq(emptyLongArray, emptyShortArray, emptyByteArray, emptyIntArray,
      emptyDoubleArray, emptyFloatArray, emptyBooleanArray).foreach { array =>

      makeValueNeoSafe(array) should equal(array)
      makeValueNeoSafe(array).getClass
        .getComponentType should equal(array.getClass.getComponentType)
    }
  }

  test("string arrays work") {
    val array = Array[String]()

    makeValueNeoSafe(array) should equal(array)
    makeValueNeoSafe(array).getClass
      .getComponentType should equal(array.getClass.getComponentType)
  }

  test("mixed types are not ok") {
    intercept[CypherTypeException](makeValueNeoSafe(Seq("a", 12, false)))
  }
}
