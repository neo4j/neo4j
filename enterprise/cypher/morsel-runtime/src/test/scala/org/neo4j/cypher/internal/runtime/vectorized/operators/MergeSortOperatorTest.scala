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

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.LongSlot
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.slotted.pipes.Ascending
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualValues
import org.opencypher.v9_0.util.symbols.CTNode
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class MergeSortOperatorTest extends CypherFunSuite {

  test("sort a single morsel") {
    val numberOfLongs = 1
    val numberOfReferences = 0
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))

    val longs = Array[Long](1, 2, 3, 4, 5, 6, 7, 8, 9)
    val in = new Morsel(longs, Array[AnyValue](), longs.length)
    val out = new Morsel(new Array[Long](longs.length), Array[AnyValue](), longs.length)

    val operator = new MergeSortOperator(columnOrdering)
    val continuation = operator.init(null, null, Array(MorselExecutionContext(in, numberOfLongs, numberOfReferences)))
      .operate(MorselExecutionContext(out, numberOfLongs, numberOfReferences), null, null )

    continuation shouldBe an[EndOfLoop]
    out.longs should equal(longs)
  }

  test("top on a single morsel") {
    val numberOfLongs = 1
    val numberOfReferences = 0
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))

    val longs = Array[Long](1, 2, 3, 4, 5, 6, 7, 8, 9)
    val in = new Morsel(longs, Array[AnyValue](), longs.length)
    val out = new Morsel(new Array[Long](longs.length), Array[AnyValue](), longs.length)

    val operator = new MergeSortOperator(columnOrdering, Some(Literal(3)))
    val continuation = operator.init(null, null, Array(MorselExecutionContext(in, numberOfLongs, numberOfReferences)))
      .operate(MorselExecutionContext(out, numberOfLongs, numberOfReferences), null, QueryState(VirtualValues.EMPTY_MAP, null) )

    continuation shouldBe an[EndOfLoop]
    out.longs.take(3) should equal(Array[Long](1, 2, 3))
    out.validRows shouldBe 3
  }

  test("sort two morsels") {
    val numberOfLongs = 1
    val numberOfReferences = 0
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))

    val long1 = Array[Long](1, 2, 3, 4, 6, 7, 8, 9)
    val long2 = Array[Long](5, 7, 9, 14, 86, 92)
    val in1 = new Morsel(long1, Array[AnyValue](), long1.length)
    val in2 = new Morsel(long2, Array[AnyValue](), long2.length)
    val out = new Morsel(new Array[Long](5), Array[AnyValue](), 5)

    val operator = new MergeSortOperator(columnOrdering)

    val task = operator.init(null, null, Array(MorselExecutionContext(in1, numberOfLongs, numberOfReferences), MorselExecutionContext(in2, numberOfLongs, numberOfReferences)))
    task.operate(MorselExecutionContext(out, numberOfLongs, numberOfReferences), null, null )
    task.canContinue should be(true)
    out.longs should equal(Array(1, 2, 3, 4, 5))
    out.validRows shouldBe 5

    task.operate(MorselExecutionContext(out, numberOfLongs, numberOfReferences), null, null )
    task.canContinue should be(true)
    out.longs should equal(Array(6, 7, 7, 8, 9))
    out.validRows shouldBe 5

    task.operate(MorselExecutionContext(out, numberOfLongs, numberOfReferences), null, null )
    task.canContinue should be(false)
    out.longs.take(4) should equal(Array(9, 14, 86,92))
    out.validRows shouldBe 4
  }

  test("sort two morsels with additional column") {
    val numberOfLongs = 2
    val numberOfReferences = 0

    val slot1 = LongSlot(0, nullable = false, CTNode)
    val slot2 = LongSlot(1, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot1))

    val long1 = Array[Long](
      1, 101,
      3, 103,
      5, 105,
      7, 107,
      9, 109
    )
    val long2 = Array[Long](
      2, 102,
      4, 104,
      6, 106,
      8, 108,
      10, 110
    )
    val in1 = new Morsel(long1, Array[AnyValue](), long1.length / numberOfLongs)
    val in2 = new Morsel(long2, Array[AnyValue](), long2.length / numberOfLongs)

    val outputRowsPerMorsel = 4
    val out = new Morsel(new Array[Long](numberOfLongs * outputRowsPerMorsel), Array[AnyValue](), outputRowsPerMorsel)

    val operator = new MergeSortOperator(columnOrdering)

    val task = operator.init(null, null, Array(MorselExecutionContext(in1, numberOfLongs, numberOfReferences), MorselExecutionContext(in2, numberOfLongs, numberOfReferences)))
    task.operate(MorselExecutionContext(out, numberOfLongs, numberOfReferences), null, null )
    task.canContinue should be(true)
    out.longs should equal(Array(1, 101, 2, 102, 3, 103, 4, 104))
    out.validRows shouldBe outputRowsPerMorsel

    task.operate(MorselExecutionContext(out, numberOfLongs, numberOfReferences), null, null )
    task.canContinue should be(true)
    out.longs should equal(Array(5, 105, 6, 106, 7, 107, 8, 108))
    out.validRows shouldBe outputRowsPerMorsel

    task.operate(MorselExecutionContext(out, numberOfLongs, numberOfReferences), null, null )
    task.canContinue should be(false)
    out.longs.take(4) should equal(Array(9, 109, 10, 110))
    out.validRows shouldBe 2
  }

  test("sort two morsels by two columns") {
    val numberOfLongs = 2
    val numberOfReferences = 0

    val slot1 = LongSlot(0, nullable = false, CTNode)
    val slot2 = LongSlot(1, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot1), Ascending(slot2))

    val long1 = Array[Long](
      1, 101,
      1, 102,
      2, 202,
      5, 501,
      7, 701
    )
    val long2 = Array[Long](
      1, 103,
      2, 201,
      3, 301,
      5, 502,
      5, 503
    )
    val in1 = new Morsel(long1, Array[AnyValue](), long1.length / numberOfLongs)
    val in2 = new Morsel(long2, Array[AnyValue](), long2.length / numberOfLongs)

    val outputRowsPerMorsel = 4
    val out = new Morsel(new Array[Long](numberOfLongs * outputRowsPerMorsel), Array[AnyValue](), outputRowsPerMorsel)

    val operator = new MergeSortOperator(columnOrdering)

    val task = operator.init(null, null, Array(MorselExecutionContext(in1, numberOfLongs, numberOfReferences), MorselExecutionContext(in2, numberOfLongs, numberOfReferences)))
    task.operate(MorselExecutionContext(out, numberOfLongs, numberOfReferences), null, null )
    task.canContinue should be(true)
    out.longs should equal(Array(1, 101, 1, 102, 1, 103, 2, 201))
    out.validRows shouldBe outputRowsPerMorsel

    task.operate(MorselExecutionContext(out, numberOfLongs, numberOfReferences), null, null )
    task.canContinue should be(true)
    out.longs should equal(Array(2, 202, 3, 301, 5, 501, 5, 502))
    out.validRows shouldBe outputRowsPerMorsel

    task.operate(MorselExecutionContext(out, numberOfLongs, numberOfReferences), null, null )
    task.canContinue should be(true)
    out.longs.take(4) should equal(Array(5, 503, 7, 701))
    out.validRows shouldBe 2
  }

  test("top on two morsels") {
    val numberOfLongs = 1
    val numberOfReferences = 0
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))

    val long1 = Array[Long](1, 2, 3, 4, 6, 7, 8, 9)
    val long2 = Array[Long](5, 7, 9, 14, 86, 92)
    val in1 = new Morsel(long1, Array[AnyValue](), long1.length)
    val in2 = new Morsel(long2, Array[AnyValue](), long2.length)
    val out = new Morsel(new Array[Long](5), Array[AnyValue](), 5)

    val operator = new MergeSortOperator(columnOrdering, Some(Literal(9)))

    val task = operator.init(null, null, Array(MorselExecutionContext(in1, numberOfLongs, numberOfReferences), MorselExecutionContext(in2, numberOfLongs, numberOfReferences)))
    task.operate(MorselExecutionContext(out, numberOfLongs, numberOfReferences), null, QueryState(VirtualValues.EMPTY_MAP, null) )
    task.canContinue should be(true)
    out.longs should equal(Array(1, 2, 3, 4, 5))
    out.validRows shouldBe 5

    task.operate(MorselExecutionContext(out, numberOfLongs, numberOfReferences), null, QueryState(VirtualValues.EMPTY_MAP, null) )
    task.canContinue should be(false)
    out.longs.take(4) should equal(Array(6, 7, 7, 8))
    out.validRows shouldBe 4
  }

  test("sort two morsels with one empty array") {
    val numberOfLongs = 1
    val numberOfReferences = 0
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))

    val long1 = Array[Long](1, 2, 3, 4, 5, 6, 7, 8, 9)
    val long2 = Array.empty[Long]
    val in1 = new Morsel(long1, Array[AnyValue](), long1.length)
    val in2 = new Morsel(long2, Array[AnyValue](), long2.length)
    val out = new Morsel(new Array[Long](9), Array[AnyValue](), 9)

    val operator = new MergeSortOperator(columnOrdering)
    val task = operator.init(null, null, Array(MorselExecutionContext(in1, numberOfLongs, numberOfReferences), MorselExecutionContext(in2, numberOfLongs, numberOfReferences)))
    task.operate(MorselExecutionContext(out, numberOfLongs, numberOfReferences), null, null )
    task.canContinue should be(false)
    out.longs should equal(long1)
  }

  test("sort with too many output slots") {
    val numberOfLongs = 1
    val numberOfReferences = 0
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))

    val longs = Array[Long](1, 2, 3, 4, 5, 6, 7, 8, 9)
    val in = new Morsel(longs, Array[AnyValue](), longs.length)
    val out = new Morsel(new Array[Long](longs.length + 5), Array[AnyValue](), longs.length + 5)

    val operator = new MergeSortOperator(columnOrdering)
    val task = operator.init(null, null, Array(MorselExecutionContext(in, numberOfLongs, numberOfReferences)))
    task.operate(MorselExecutionContext(out, numberOfLongs, numberOfReferences), null, null )
    task.canContinue should be(false)
    out.longs should equal(Array[Long](1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 0, 0, 0))
    out.validRows should equal(longs.length)
  }

}
