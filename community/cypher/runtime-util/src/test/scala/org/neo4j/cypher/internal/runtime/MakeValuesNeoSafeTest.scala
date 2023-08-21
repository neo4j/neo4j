/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.storable.Values.booleanArray
import org.neo4j.values.storable.Values.booleanValue
import org.neo4j.values.storable.Values.byteArray
import org.neo4j.values.storable.Values.doubleArray
import org.neo4j.values.storable.Values.floatArray
import org.neo4j.values.storable.Values.intArray
import org.neo4j.values.storable.Values.intValue
import org.neo4j.values.storable.Values.longArray
import org.neo4j.values.storable.Values.shortArray
import org.neo4j.values.storable.Values.stringArray
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.VirtualValues.list

import scala.Array.emptyBooleanArray
import scala.Array.emptyByteArray
import scala.Array.emptyDoubleArray
import scala.Array.emptyFloatArray
import scala.Array.emptyIntArray
import scala.Array.emptyLongArray
import scala.Array.emptyShortArray

class MakeValuesNeoSafeTest extends CypherFunSuite {

  test("string collection turns into string array") {
    makeValueNeoSafe(list(stringValue("a"), stringValue("b"))) should equal(stringArray("a", "b"))
  }

  test("empty collection in is empty array") {
    makeValueNeoSafe(list()) should equal(stringArray())
  }

  test("retains type of primitive arrays") {
    Seq(
      longArray(emptyLongArray),
      shortArray(emptyShortArray),
      byteArray(emptyByteArray),
      intArray(emptyIntArray),
      doubleArray(emptyDoubleArray),
      floatArray(emptyFloatArray),
      booleanArray(emptyBooleanArray)
    ).foreach { array =>
      makeValueNeoSafe(array) should equal(array)
    }
  }

  test("string arrays work") {
    makeValueNeoSafe(stringArray()) should equal(stringArray())
  }

  test("mixed types are not ok") {
    intercept[CypherTypeException](makeValueNeoSafe(list(stringValue("a"), intValue(12), booleanValue(false))))
  }
}
