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

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{RefSlot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.vectorized.{Iteration, Morsel, QueryState, StartLoopWithEagerData}
import org.neo4j.cypher.internal.util.v3_4.symbols.CTAny
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

import scala.collection.mutable

class AggregationReducerOperatorNoGroupingTest extends CypherFunSuite {

  test("reduce from single morsel") {
    // Given
    val slots = new SlotConfiguration(mutable.Map("aggregate" -> RefSlot(0, nullable = false, CTAny)), 1, 1)
    val aggregation = new AggregationReduceOperatorNoGrouping(slots,
                                                              Array(AggregationOffsets(0, 0, DummyEvenNodeIdAggregation(0))))
    val refs = new Array[AnyValue](10)
    refs(0) = Values.longArray(Array(2,4,42))
    val in = new Morsel(Array.empty, refs, refs.length)
    val out = new Morsel(new Array[Long](10), new Array[AnyValue](10), refs.length)
    // When
    aggregation.operate(
      StartLoopWithEagerData(Array(in), new Iteration(None)), out, null, QueryState(VirtualValues.EMPTY_MAP, null))

    // Then
    out.refs(0) should equal(Values.longArray(Array(2,4,42)))
  }

  test("reduce values from multiple morsels") {
    // Given
    val slots = new SlotConfiguration(mutable.Map("aggregate" -> RefSlot(0, nullable = false, CTAny)), 1, 1)
    val aggregation = new AggregationReduceOperatorNoGrouping(slots,

                                                              Array(AggregationOffsets(0, 0, DummyEvenNodeIdAggregation(0))))
    val in = 1 to 10 map ( i => {
      val refs = new Array[AnyValue](10)
      refs(0) = Values.longArray(Array(2*i))
      new Morsel(Array.empty, refs, refs.length)
    })

    val out = new Morsel(new Array[Long](10), new Array[AnyValue](10), 10)
    // When
    aggregation.operate(
      StartLoopWithEagerData(in.toArray, new Iteration(None)), out, null, QueryState(VirtualValues.EMPTY_MAP, null))

    // Then
    out.refs(0) should equal(Values.longArray(Array(2,4,6,8,10,12,14,16,18,20)))
  }
}
