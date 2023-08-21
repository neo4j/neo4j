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

import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.CompositeValueIndexCursor.ascending
import org.neo4j.cypher.internal.runtime.CompositeValueIndexCursor.descending
import org.neo4j.cypher.internal.runtime.CompositeValueIndexCursor.unordered
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.kernel.api.helpers.StubNodeValueIndexCursor
import org.neo4j.values.storable.Values

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class CompositeValueIndexCursorTest extends CypherFunSuite {

  test("should create unordered cursor") {
    // given
    val cursor = unordered(
      Array(
        cursorFor(10, 11, 12),
        cursorFor(5),
        cursorFor(11, 15)
      )
    )

    // when
    val list = asList(cursor)

    // then
    list should equal(List(10, 11, 12, 5, 11, 15))
  }

  test("randomized unordered cursor") {
    // given
    val (cursors, totalSize) = randomCursors()
    val cursor = unordered(cursors)

    // when
    val list = asList(cursor)

    // then
    list should have size totalSize
  }

  test("should create ascending cursor") {
    // given
    val cursor = ascending(
      Array(
        cursorFor(10, 11, 12),
        cursorFor(5),
        cursorFor(11, 15)
      )
    )

    // when
    val list = asList(cursor)

    // then
    list should equal(List(5, 10, 11, 11, 12, 15))
  }

  test("randomized ascending cursor") {
    // given
    val (cursors, totalSize) = randomCursors(IndexOrderAscending)
    val cursor = ascending(cursors)

    // when
    val list = asList(cursor)

    // then
    list should have size totalSize
    list shouldBe sorted
  }

  test("should create descending cursor") {
    // given
    val cursor = descending(
      Array(
        cursorFor(12, 11, 10),
        cursorFor(5),
        cursorFor(15, 11)
      )
    )

    // when
    val list = asList(cursor)

    // then
    list should equal(List(15, 12, 11, 11, 10, 5))
  }

  test("randomized descending cursor") {
    // given
    val (cursors, totalSize) = randomCursors(IndexOrderDescending)
    val cursor = descending(cursors)

    // when
    val list = asList(cursor)

    // then
    list should have size totalSize
    list.reverse shouldBe sorted
  }

  private def cursorFor(values: Int*): NodeValueIndexCursor = {
    val stub = new StubNodeValueIndexCursor()
    values.zipWithIndex.foreach {
      case (v, i) => stub.withNode(i, Values.intValue(v))
    }
    stub
  }

  private def asList(cursor: NodeValueIndexCursor): List[Int] = {
    val values = ArrayBuffer.empty[Int]
    while (cursor.next()) {
      values.append(cursor.propertyValue(0).asObject().asInstanceOf[Int])
    }
    values
  }.toList

  private def randomCursors(indexOrder: IndexOrder = IndexOrderNone) = {
    val randomArray = new Array[NodeValueIndexCursor](Random.nextInt(1000))
    var i = 0
    var totalSize = 0
    while (i < randomArray.length) {
      val randomInts = indexOrder match {
        case IndexOrderNone       => Seq.fill(Random.nextInt(100))(Random.nextInt)
        case IndexOrderAscending  => Seq.fill(Random.nextInt(100))(Random.nextInt).sorted
        case IndexOrderDescending => Seq.fill(Random.nextInt(100))(Random.nextInt).sorted(Ordering.Int.reverse)
      }
      randomArray(i) = cursorFor(randomInts.toSeq: _*)
      totalSize += randomInts.size
      i += 1
    }
    (randomArray, totalSize)
  }
}
