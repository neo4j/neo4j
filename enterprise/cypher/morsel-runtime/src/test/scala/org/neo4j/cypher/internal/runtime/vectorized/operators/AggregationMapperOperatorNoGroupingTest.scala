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

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{LongSlot, RefSlot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.vectorized.{Iteration, Morsel, QueryState}
import org.neo4j.cypher.internal.util.v3_4.symbols.{CTAny, CTNode}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

import scala.collection.mutable

class AggregationMapperOperatorNoGroupingTest extends CypherFunSuite {

  test("single aggregation on a single morsel") {
    // Given
    val slots = new SlotConfiguration(mutable.Map("node" -> LongSlot(0, nullable = false, CTNode),
                                                  "aggregate" -> RefSlot(0, nullable = false, CTAny)), 1, 1)
    val aggregation = new AggregationMapperOperatorNoGrouping(slots, Array(AggregationOffsets(0, 0, DummyEvenNodeIdAggregation(0))))
    val longs = Array[Long](0,1,2,3,4,5,6,7,8,9)
    val refs = new Array[AnyValue](10)
    val data = new Morsel(longs, refs, longs.length)

    // When
    aggregation.operate(new Iteration(None), data, null, QueryState(VirtualValues.EMPTY_MAP, null))

    // Then
    data.refs(0) should equal(Values.longArray(Array(0,2,4,6,8)))
  }

  test("multiple aggregations on a single morsel") {
    val slots = new SlotConfiguration(mutable.Map("n1" -> LongSlot(0, nullable = false, CTNode),
                                                  "n2" -> LongSlot(1, nullable = false, CTNode),
                                                  "aggregate" -> RefSlot(0, nullable = false, CTAny)), 2, 1)

    val aggregation = new AggregationMapperOperatorNoGrouping(slots, Array(
      AggregationOffsets(0, 0, DummyEvenNodeIdAggregation(0)),
      AggregationOffsets(1, 1, DummyEvenNodeIdAggregation(1))
    ))

    //this is interpreted as n1,n2,n1,n2...
    val longs = Array[Long](0,1,2,3,4,5,6,7,8,9)
    val refs = new Array[AnyValue](5)
    val data = new Morsel(longs, refs, 5)

    aggregation.operate(new Iteration(None), data, null, QueryState(VirtualValues.EMPTY_MAP, null))

    data.refs(0) should equal(Values.longArray(Array(0,2,4,6,8)))
    data.refs(1) should equal(Values.longArray(Array.empty))
  }
}
