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

import org.neo4j.cypher.internal.runtime.vectorized.{Iteration, Morsel, MorselExecutionContext, QueryState}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class AggregationMapperOperatorNoGroupingTest extends CypherFunSuite {

  test("single aggregation on a single morsel") {
    // Given
    val numberOfLongs = 1
    val numberOfReferences = 1
    val aggregation = new AggregationMapperOperatorNoGrouping(Array(AggregationOffsets(0, 0, DummyEvenNodeIdAggregation(0))))
    val longs = Array[Long](0,1,2,3,4,5,6,7,8,9)
    val refs = new Array[AnyValue](10)
    val data = new Morsel(longs, refs, longs.length)

    // When
    aggregation.operate(Iteration(None), MorselExecutionContext(data, numberOfLongs, numberOfReferences), null, QueryState(VirtualValues.EMPTY_MAP, null))

    // Then
    data.refs(0) should equal(Values.longArray(Array(0,2,4,6,8)))
  }

  test("multiple aggregations on a single morsel") {
    val numberOfLongs = 2
    val numberOfReferences = 1

    val aggregation = new AggregationMapperOperatorNoGrouping(Array(
      AggregationOffsets(0, 0, DummyEvenNodeIdAggregation(0)),
      AggregationOffsets(1, 1, DummyEvenNodeIdAggregation(1))
    ))

    //this is interpreted as n1,n2,n1,n2...
    val longs = Array[Long](0,1,2,3,4,5,6,7,8,9)
    val refs = new Array[AnyValue](5)
    val data = new Morsel(longs, refs, 5)

    aggregation.operate(Iteration(None), MorselExecutionContext(data, numberOfLongs, numberOfReferences), null, QueryState(VirtualValues.EMPTY_MAP, null))

    data.refs(0) should equal(Values.longArray(Array(0,2,4,6,8)))
    data.refs(1) should equal(Values.longArray(Array.empty))
  }
}
