/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{LongSlot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.slotted.pipes.Ascending
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.util.v3_4.symbols.CTNode
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.AnyValue

import scala.collection.mutable

class MergeSortOperatorTest extends CypherFunSuite {

  test("sort a single morsel") {
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val info = new SlotConfiguration(mutable.Map("apa" -> slot), 1, 0)

    val longs = Array[Long](1, 2, 3, 4, 5, 6, 7, 8, 9)
    val in = new Morsel(longs, Array[AnyValue](), longs.length)
    val out = new Morsel(new Array[Long](longs.length), Array[AnyValue](), longs.length)

    val operator = new MergeSortOperator(columnOrdering, info)
    val continuation = operator.operate(StartLoopWithEagerData(Array(in), new Iteration(None)), out, null, null )

    continuation shouldBe an[EndOfLoop]
    out.longs should equal(longs)
  }

  test("sort two morsels") {
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val info = new SlotConfiguration(mutable.Map("apa" -> slot), 1, 0)

    val long1 = Array[Long](1, 2, 3, 4, 5, 6, 7, 8, 9)
    val long2 = Array[Long](5, 7, 9, 14, 86, 92)
    val in1 = new Morsel(long1, Array[AnyValue](), long1.length)
    val in2 = new Morsel(long2, Array[AnyValue](), long2.length)
    val out = new Morsel(new Array[Long](5), Array[AnyValue](), 5)

    val operator = new MergeSortOperator(columnOrdering, info)

    val continuation1 = operator.operate(StartLoopWithEagerData(Array(in1, in2), new Iteration(None)), out, null, null )
    continuation1 shouldBe a[ContinueWithSource[_]]
    out.longs should equal(Array(1, 2, 3, 4, 5))

    val continuation2 = operator.operate(ContinueLoopWith(continuation1), out, null, null )
    continuation2 shouldBe a[ContinueWithSource[_]]
    out.longs should equal(Array(5, 6, 7, 7, 8))

    val continuation3 = operator.operate(ContinueLoopWith(continuation2), out, null, null )
    continuation3 shouldBe an[EndOfLoop]
    out.longs should equal(Array(9, 9, 14,86,92))
  }

  test("sort two morsels with one empty array") {
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val info = new SlotConfiguration(mutable.Map("apa" -> slot), 1, 0)

    val long1 = Array[Long](1, 2, 3, 4, 5, 6, 7, 8, 9)
    val long2 = Array.empty[Long]
    val in1 = new Morsel(long1, Array[AnyValue](), long1.length)
    val in2 = new Morsel(long2, Array[AnyValue](), long2.length)
    val out = new Morsel(new Array[Long](9), Array[AnyValue](), 9)

    val operator = new MergeSortOperator(columnOrdering, info)
    val continuation = operator.operate(StartLoopWithEagerData(Array(in1, in2), new Iteration(None)), out, null, null )

    continuation shouldBe an[EndOfLoop]
    out.longs should equal(long1)
  }

  test("sort with too many output slots") {
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val info = new SlotConfiguration(mutable.Map("apa" -> slot), 1, 0)

    val longs = Array[Long](1, 2, 3, 4, 5, 6, 7, 8, 9)
    val in = new Morsel(longs, Array[AnyValue](), longs.length)
    val out = new Morsel(new Array[Long](longs.length + 5), Array[AnyValue](), longs.length + 5)

    val operator = new MergeSortOperator(columnOrdering, info)
    val continuation = operator.operate(StartLoopWithEagerData(Array(in), new Iteration(None)), out, null, null )

    continuation shouldBe an[EndOfLoop]
    out.longs should equal(Array[Long](1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 0, 0, 0))
    out.validRows should equal(longs.length)
  }

}
