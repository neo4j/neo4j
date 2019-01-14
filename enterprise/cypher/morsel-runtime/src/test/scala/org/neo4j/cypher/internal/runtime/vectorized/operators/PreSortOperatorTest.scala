/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{LongSlot, RefSlot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.slotted.pipes.Ascending
import org.neo4j.cypher.internal.runtime.vectorized.{Iteration, Morsel}
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.intValue

import scala.collection.mutable

class PreSortOperatorTest extends CypherFunSuite {

  test("sort a morsel with a single long column") {
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val info = new SlotConfiguration(mutable.Map("apa" -> slot), 1, 0)
    val sortOperator = new PreSortOperator(columnOrdering, info)

    val longs = Array[Long](9, 8, 7, 6, 5, 4, 3, 2, 1)
    val data = new Morsel(longs, Array[AnyValue](), longs.length)

    sortOperator.operate(new Iteration(None), data, null, null)

    data.longs should equal(Array[Long](1, 2, 3, 4, 5, 6, 7, 8, 9))
  }

  test("sort a morsel with a one long slot and one ref slot, order by ref") {
    val slot1 = LongSlot(0, nullable = false, CTNode)
    val slot2 = RefSlot(0, nullable = false, CTNumber)
    val columnOrdering = Seq(Ascending(slot2))
    val info = new SlotConfiguration(mutable.Map("apa1" -> slot1, "apa2" -> slot2), 1, 1)
    val sortOperator = new PreSortOperator(columnOrdering, info)

    val longs = Array[Long](
      6, 5, 4,
      9, 8, 7,
      3, 2, 1)
    val refs = Array[AnyValue](
      intValue(6), intValue(5), intValue(4),
      intValue(9), intValue(8), intValue(7),
      intValue(3), intValue(2), intValue(1))
    val data = new Morsel(longs, refs, longs.length)

    sortOperator.operate(new Iteration(None), data, null, null)

    data.longs should equal(Array[Long](
      1, 2, 3,
      4, 5, 6,
      7, 8, 9))
    data.refs should equal(Array[AnyValue](
      intValue(1), intValue(2), intValue(3),
      intValue(4), intValue(5), intValue(6),
      intValue(7), intValue(8), intValue(9))
    )

  }

  test("sort a morsel with a two long columns by one") {
    val slot1 = LongSlot(0, nullable = false, CTNode)
    val slot2 = LongSlot(1, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot1))
    val info = new SlotConfiguration(mutable.Map("apa1" -> slot1, "apa2" -> slot2), 2, 0)
    val sortOperator = new PreSortOperator(columnOrdering, info)

    val longs = Array[Long](
      9, 0,
      8, 1,
      7, 2,
      6, 3,
      5, 4,
      4, 5,
      3, 6,
      2, 7,
      1, 8)
    val rows = longs.length / 2 // Since we have two columns per row
    val data = new Morsel(longs, Array[AnyValue](), rows)

    sortOperator.operate(new Iteration(None), data, null, null)

    data.longs should equal(Array[Long](
      1, 8,
      2, 7,
      3, 6,
      4, 5,
      5, 4,
      6, 3,
      7, 2,
      8, 1,
      9, 0)
    )
  }

  test("sort a morsel with no valid data") {
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val info = new SlotConfiguration(mutable.Map("apa" -> slot), 1, 0)
    val sortOperator = new PreSortOperator(columnOrdering, info)

    val longs = new Array[Long](10)
    val data = new Morsel(longs, Array[AnyValue](), 0)

    sortOperator.operate(new Iteration(None), data, null, null)

    data.longs should equal(Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
  }

  test("sort a morsel with empty array") {
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val info = new SlotConfiguration(mutable.Map("apa" -> slot), 1, 0)
    val sortOperator = new PreSortOperator(columnOrdering, info)

    val data = new Morsel(Array.empty, Array[AnyValue](), 0)

    sortOperator.operate(new Iteration(None), data, null, null)

    data.longs shouldBe empty
  }
}
