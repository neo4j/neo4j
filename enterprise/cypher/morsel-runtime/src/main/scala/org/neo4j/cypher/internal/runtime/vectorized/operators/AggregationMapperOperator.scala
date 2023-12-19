/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.runtime.vectorized.expressions.{AggregationHelper, AggregationMapper}
import org.neo4j.values.AnyValue

import scala.collection.mutable.{Map => MutableMap}


/*
Responsible for aggregating the data coming from a single morsel. This is equivalent to the map
step of map-reduce. Each thread performs it its local aggregation on the data local to it. In
the subsequent reduce steps these local aggregations are merged into a single global aggregate.
 */
class AggregationMapperOperator(slots: SlotConfiguration, aggregations: Array[AggregationOffsets], groupings: Array[GroupingOffsets]) extends MiddleOperator {

  //These are assigned at compile time to save some time at runtime
  private val groupingFunction = AggregationHelper.groupingFunction(groupings)
  private val addGroupingValuesToResult = AggregationHelper.computeGroupingSetter(groupings)

  override def operate(iterationState: Iteration, data: Morsel, context: QueryContext, state: QueryState): Unit = {

    val result = MutableMap[AnyValue, Array[(Int,AggregationMapper)]]()

    val longCount = slots.numberOfLongs
    val refCount = slots.numberOfReferences
    val currentRow = new MorselExecutionContext(data, longCount, refCount, currentRow = 0)
    val queryState = new OldQueryState(context, resources = null, params = state.params)

    //loop over the entire morsel and apply the aggregation
    while (currentRow.currentRow < data.validRows) {
      val groupingValue: AnyValue = groupingFunction(currentRow, queryState)
      val functions = result
        .getOrElseUpdate(groupingValue, aggregations.map(a => a.incoming -> a.aggregation.createAggregationMapper))
      functions.foreach(f => f._2.map(currentRow, queryState))
      currentRow.currentRow += 1
    }

    //reuse and reset morsel context
    currentRow.currentRow = 0
    result.foreach {
      case (key, aggregator) =>
        addGroupingValuesToResult(currentRow, key)
        var i = 0
        while (i < aggregations.length) {
          val (offset, mapper) = aggregator(i)
          data.refs(currentRow.currentRow * refCount + offset) = mapper.result
          i += 1
        }
        currentRow.currentRow += 1
    }
    data.validRows = currentRow.currentRow
  }
}
