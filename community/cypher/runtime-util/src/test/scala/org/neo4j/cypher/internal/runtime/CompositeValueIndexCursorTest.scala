/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.runtime.CompositeValueIndexCursor.ascending
import org.neo4j.cypher.internal.runtime.CompositeValueIndexCursor.descending
import org.neo4j.cypher.internal.runtime.CompositeValueIndexCursor.unordered
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.kernel.api.helpers.StubNodeValueIndexCursor
import org.neo4j.values.storable.Values

import scala.collection.mutable.ArrayBuffer

class CompositeValueIndexCursorTest extends CypherFunSuite {

  test("should create unordered cursor") {
    //given
    val cursor = unordered(
      Array(
        cursorFor(10, 11, 12),
        cursorFor(5),
        cursorFor(11, 15),
      ))

    //when
    val list = asList(cursor)

    //then
    list should equal(List(10, 11, 12, 5, 11, 15))
  }

  test("should create ascending cursor") {
    //given
    val cursor = ascending(
      Array(
        cursorFor(10, 11, 12),
        cursorFor(5),
        cursorFor(11, 15),
      ))

    //when
    val list = asList(cursor)

    //then
    list should equal(List(5, 10, 11, 11, 12, 15))
  }

  test("should create descending cursor") {
    //given
    val cursor = descending(
      Array(
        cursorFor(12, 11, 10),
        cursorFor(5),
        cursorFor(15, 11),
      ))

    //when
    val list = asList(cursor)

    //then
    list should equal(List(15, 12, 11, 11, 10, 5))
  }

  private def cursorFor(values: Any*): NodeValueIndexCursor = {
    val stub = new StubNodeValueIndexCursor()
      values.zipWithIndex.foreach {
        case (v, i) => stub.withNode(i, Values.of(v))
      }
     stub
  }

  private def asList(cursor: NodeValueIndexCursor): Seq[AnyRef] = {
    val values = ArrayBuffer.empty[AnyRef]
    while (cursor.next()) {
      values.append(cursor.propertyValue(0).asObject())
    }
    values
  }
}
