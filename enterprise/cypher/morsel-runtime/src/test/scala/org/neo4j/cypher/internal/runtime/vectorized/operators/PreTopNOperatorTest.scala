/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.{LongSlot, RefSlot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.slotted.pipes.Ascending
import org.neo4j.cypher.internal.runtime.vectorized.{Iteration, Morsel, QueryState}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.intValue
import org.neo4j.values.virtual.VirtualValues
import org.opencypher.v9_0.util.symbols._
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

import scala.collection.mutable

class PreTopNOperatorTest extends CypherFunSuite {

  test("top on a morsel with a single long column") {
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val info = new SlotConfiguration(mutable.Map("apa" -> slot), 1, 0)
    val topOperator = new PreTopNOperator(columnOrdering, info, Literal(3))

    val longs = Array[Long](9, 8, 7, 6, 5, 4, 3, 2, 1)
    val data = new Morsel(longs, Array[AnyValue](), longs.length)

    topOperator.operate(new Iteration(None), data, null, QueryState(VirtualValues.EMPTY_MAP, null))

    data.longs.take(3) should equal(Array[Long](1, 2, 3))
    data.validRows shouldBe 3
  }

  test("top with n > morselSize on a morsel with a single long column") {
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val info = new SlotConfiguration(mutable.Map("apa" -> slot), 1, 0)
    val topOperator = new PreTopNOperator(columnOrdering, info, Literal(20))

    val longs = Array[Long](9, 8, 7, 6, 5, 4, 3, 2, 1)
    val data = new Morsel(longs, Array[AnyValue](), longs.length)

    topOperator.operate(new Iteration(None), data, null, QueryState(VirtualValues.EMPTY_MAP, null))

    data.longs should equal(Array[Long](1, 2, 3, 4, 5, 6, 7, 8, 9))
    data.validRows shouldBe longs.length
  }

  test("top on a morsel with a one long slot and one ref slot, order by ref") {
    val slot1 = LongSlot(0, nullable = false, CTNode)
    val slot2 = RefSlot(0, nullable = false, CTNumber)
    val columnOrdering = Seq(Ascending(slot2))
    val info = new SlotConfiguration(mutable.Map("apa1" -> slot1, "apa2" -> slot2), 1, 1)
    val topOperator = new PreTopNOperator(columnOrdering, info, Literal(3))

    val longs = Array[Long](
      6, 5, 4,
      9, 8, 7,
      3, 2, 1)
    val refs = Array[AnyValue](
      intValue(6), intValue(5), intValue(4),
      intValue(9), intValue(8), intValue(7),
      intValue(3), intValue(2), intValue(1))
    val data = new Morsel(longs, refs, longs.length)

    topOperator.operate(new Iteration(None), data, null, QueryState(VirtualValues.EMPTY_MAP, null))

    data.longs.take(3) should equal(Array[Long](
      1, 2, 3))
    data.refs.take(3) should equal(Array[AnyValue](
      intValue(1), intValue(2), intValue(3))
    )
    data.validRows shouldBe 3
  }

  test("top on a morsel with no valid data") {
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val info = new SlotConfiguration(mutable.Map("apa" -> slot), 1, 0)
    val topOperator = new PreTopNOperator(columnOrdering, info, Literal(3))

    val longs = new Array[Long](10)
    val data = new Morsel(longs, Array[AnyValue](), 0)

    topOperator.operate(new Iteration(None), data, null, QueryState(VirtualValues.EMPTY_MAP, null))

    data.longs should equal(Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    data.validRows shouldBe 0
  }

  test("top on a morsel with empty array") {
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val info = new SlotConfiguration(mutable.Map("apa" -> slot), 1, 0)
    val topOperator = new PreTopNOperator(columnOrdering, info, Literal(3))

    val data = new Morsel(Array.empty, Array[AnyValue](), 0)

    topOperator.operate(new Iteration(None), data, null, QueryState(VirtualValues.EMPTY_MAP, null))

    data.longs shouldBe empty
    data.validRows shouldBe 0
  }
}
