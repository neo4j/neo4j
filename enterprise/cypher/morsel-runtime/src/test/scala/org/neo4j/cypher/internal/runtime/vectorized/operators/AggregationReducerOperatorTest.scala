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
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.vectorized.{Iteration, Morsel, QueryState, StartLoopWithEagerData}
import org.neo4j.cypher.internal.util.v3_4.symbols.{CTAny, CTNode}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.VirtualValues

import scala.collection.mutable

class AggregationReducerOperatorTest extends CypherFunSuite {

  test("single grouping key single morsel aggregation") {
    // Given
    val groupSlot = RefSlot(0, nullable = false, CTAny)
    val slots = new SlotConfiguration(mutable.Map("aggregate" -> RefSlot(1, nullable = false, CTAny),
                                                  "group" -> groupSlot), 0, 2)
    val aggregation = new AggregationReduceOperator(slots,
                                                    Array(AggregationOffsets(1, 1, DummyEvenNodeIdAggregation(0))),
                                                    Array(GroupingOffsets(groupSlot, groupSlot, new DummyExpression())))
    val in = 1 to 5 map ( i => {
      val refs = new Array[AnyValue](10)
      refs(0) = Values.stringValue("k1")
      refs(1) = Values.longArray(Array(2*i))
      refs(2) = Values.stringValue("k2")
      refs(3) = Values.longArray(Array(20*i))
      new Morsel(Array.empty, refs, 2)
    })

    val out = new Morsel(Array.empty, new Array[AnyValue](20), 2)
    // When
    aggregation.operate(
      StartLoopWithEagerData(in.toArray, new Iteration(None)), out, null, QueryState(VirtualValues.EMPTY_MAP, null))

    // Then
    out.refs(0) should equal(stringValue("k1"))
    out.refs(1) should equal(Values.longArray(Array(2,4,6,8,10)))
    out.refs(2) should equal(stringValue("k2"))
    out.refs(3) should equal(Values.longArray(Array(20,40,60,80,100)))
  }

  test("two grouping keys") {
    // Given
    val groupSlot1 = RefSlot(0, nullable = false, CTAny)
    val groupSlot2 = RefSlot(1, nullable = false, CTAny)
    val slots = new SlotConfiguration(mutable.Map("node" -> LongSlot(0, nullable = false, CTNode),
                                                  "aggregate" -> RefSlot(2, nullable = false, CTAny),
                                                  "group1" -> groupSlot1, "group2" -> groupSlot2), 1, 3)
    val aggregation = new AggregationReduceOperator(slots,
                                                    Array(AggregationOffsets(2, 2, DummyEvenNodeIdAggregation(0))),
                                                    Array(GroupingOffsets(groupSlot1, groupSlot1,
                                                                          new DummyExpression()),
                                                          GroupingOffsets(groupSlot2, groupSlot2,
                                                                          new DummyExpression())

                                                          ))
    val in = 1 to 5 map ( i => {
      val refs = new Array[AnyValue](15)
      refs(0) = Values.stringValue("k11")
      refs(1) = Values.stringValue("k12")
      refs(2) = Values.longArray(Array(2*i))
      refs(3) = Values.stringValue("k21")
      refs(4) = Values.stringValue("k22")
      refs(5) = Values.longArray(Array(20*i))
      new Morsel(Array.empty, refs, 2)
    })

    val out = new Morsel(Array.empty, new Array[AnyValue](20), 2)
    // When
    aggregation.operate(
      StartLoopWithEagerData(in.toArray, new Iteration(None)), out, null, QueryState(VirtualValues.EMPTY_MAP, null))

    // Then
    out.refs(0) should equal(stringValue("k11"))
    out.refs(1) should equal(stringValue("k12"))
    out.refs(2) should equal(Values.longArray(Array(2,4,6,8,10)))
    out.refs(3) should equal(stringValue("k21"))
    out.refs(4) should equal(stringValue("k22"))
    out.refs(5) should equal(Values.longArray(Array(20,40,60,80,100)))
  }

  test("three grouping keys") {
    // Given
    val groupSlot1 = RefSlot(0, nullable = false, CTAny)
    val groupSlot2 = RefSlot(1, nullable = false, CTAny)
    val groupSlot3 = RefSlot(2, nullable = false, CTAny)
    val slots = new SlotConfiguration(mutable.Map("node" -> LongSlot(0, nullable = false, CTNode),
                                                  "aggregate" -> RefSlot(3, nullable = false, CTAny),
                                                  "group1" -> groupSlot1, "group2" -> groupSlot2, "group3" -> groupSlot3), 1, 4)
    val aggregation = new AggregationReduceOperator(slots,
                                                    Array(AggregationOffsets(3, 3, DummyEvenNodeIdAggregation(0))),
                                                    Array(GroupingOffsets(groupSlot1, groupSlot1, new DummyExpression()),
                                                          GroupingOffsets(groupSlot2, groupSlot2, new DummyExpression()),
                                                          GroupingOffsets(groupSlot3, groupSlot3, new DummyExpression())
                                                    ))
    val in = 1 to 5 map ( i => {
      val refs = new Array[AnyValue](20)
      refs(0) = Values.stringValue("k11")
      refs(1) = Values.stringValue("k12")
      refs(2) = Values.stringValue("k13")
      refs(3) = Values.longArray(Array(2*i))
      refs(4) = Values.stringValue("k21")
      refs(5) = Values.stringValue("k22")
      refs(6) = Values.stringValue("k23")
      refs(7) = Values.longArray(Array(20*i))
      new Morsel(Array.empty, refs, 2)
    })

    val out = new Morsel(Array.empty, new Array[AnyValue](20), 2)
    // When
    aggregation.operate(
      StartLoopWithEagerData(in.toArray, new Iteration(None)), out, null, QueryState(VirtualValues.EMPTY_MAP, null))

    // Then
    out.refs(0) should equal(stringValue("k21"))
    out.refs(1) should equal(stringValue("k22"))
    out.refs(2) should equal(stringValue("k23"))
    out.refs(3) should equal(Values.longArray(Array(20,40,60,80,100)))
    out.refs(4) should equal(stringValue("k11"))
    out.refs(5) should equal(stringValue("k12"))
    out.refs(6) should equal(stringValue("k13"))
    out.refs(7) should equal(Values.longArray(Array(2,4,6,8,10)))

  }

  test("more than three grouping keys") {
    // Given
    val groupSlot1 = RefSlot(0, nullable = false, CTAny)
    val groupSlot2 = RefSlot(1, nullable = false, CTAny)
    val groupSlot3 = RefSlot(2, nullable = false, CTAny)
    val groupSlot4 = RefSlot(3, nullable = false, CTAny)
    val groupSlot5 = RefSlot(4, nullable = false, CTAny)
    val slots = new SlotConfiguration(mutable.Map("node" -> LongSlot(0, nullable = false, CTNode),
                                                  "aggregate" -> RefSlot(5, nullable = false, CTAny),
                                                  "group1" -> groupSlot1, "group2" -> groupSlot2, "group3" -> groupSlot3,
                                                  "group4" -> groupSlot4,  "group5" -> groupSlot5), 1, 6)
    val aggregation = new AggregationReduceOperator(slots,
                                                    Array(AggregationOffsets(5, 5, DummyEvenNodeIdAggregation(0))),
                                                    Array(GroupingOffsets(groupSlot1, groupSlot1, new DummyExpression()),
                                                          GroupingOffsets(groupSlot2, groupSlot2, new DummyExpression()),
                                                          GroupingOffsets(groupSlot3, groupSlot3, new DummyExpression()),
                                                          GroupingOffsets(groupSlot4, groupSlot4, new DummyExpression()),
                                                          GroupingOffsets(groupSlot5, groupSlot5, new DummyExpression())
                                                    ))
    val in = 1 to 5 map ( i => {
      val refs = new Array[AnyValue](30)
      refs(0) = Values.stringValue("k11")
      refs(1) = Values.stringValue("k12")
      refs(2) = Values.stringValue("k13")
      refs(3) = Values.stringValue("k14")
      refs(4) = Values.stringValue("k15")
      refs(5) = Values.longArray(Array(2*i))
      refs(6) = Values.stringValue("k21")
      refs(7) = Values.stringValue("k22")
      refs(8) = Values.stringValue("k23")
      refs(9) = Values.stringValue("k24")
      refs(10) = Values.stringValue("k25")
      refs(11) = Values.longArray(Array(20*i))
      new Morsel(Array.empty, refs, 2)
    })

    val out = new Morsel(Array.empty, new Array[AnyValue](20), 2)
    // When
    aggregation.operate(
      StartLoopWithEagerData(in.toArray, new Iteration(None)), out, null, QueryState(VirtualValues.EMPTY_MAP, null))

    // Then
    out.refs(0) should equal(stringValue("k21"))
    out.refs(1) should equal(stringValue("k22"))
    out.refs(2) should equal(stringValue("k23"))
    out.refs(3) should equal(stringValue("k24"))
    out.refs(4) should equal(stringValue("k25"))
    out.refs(5) should equal(Values.longArray(Array(20,40,60,80,100)))
    out.refs(6) should equal(stringValue("k11"))
    out.refs(7) should equal(stringValue("k12"))
    out.refs(8) should equal(stringValue("k13"))
    out.refs(9) should equal(stringValue("k14"))
    out.refs(10) should equal(stringValue("k15"))
    out.refs(11) should equal(Values.longArray(Array(2,4,6,8,10)))
  }
}
