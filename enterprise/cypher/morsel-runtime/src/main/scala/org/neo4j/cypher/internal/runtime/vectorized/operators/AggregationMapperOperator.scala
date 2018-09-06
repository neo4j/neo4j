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

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.runtime.vectorized.expressions.{AggregationHelper, AggregationMapper}
import org.neo4j.values.AnyValue

import scala.collection.mutable


/*
Responsible for aggregating the data coming from a single morsel. This is equivalent to the map
step of map-reduce. Each thread performs it its local aggregation on the data local to it. In
the subsequent reduce steps these local aggregations are merged into a single global aggregate.
 */
class AggregationMapperOperator(aggregations: Array[AggregationOffsets],
                                groupings: Array[GroupingOffsets]) extends StatelessOperator {

  //These are assigned at compile time to save some time at runtime
  private val groupingFunction = AggregationHelper.groupingFunction(groupings)
  private val addGroupingValuesToResult = AggregationHelper.computeGroupingSetter(groupings)(_.mapperOutputSlot)

  override def operate(currentRow: MorselExecutionContext,
                       context: QueryContext,
                       state: QueryState): Unit = {

    val result = mutable.LinkedHashMap[AnyValue, Array[(Int,AggregationMapper)]]()

    val queryState = new OldQueryState(context, resources = null, params = state.params)

    //loop over the entire morsel and apply the aggregation
    while (currentRow.hasMoreRows) {
      val groupingValue: AnyValue = groupingFunction(currentRow, queryState)
      val functions = result
        .getOrElseUpdate(groupingValue, aggregations.map(a => a.mapperOutputSlot -> a.aggregation.createAggregationMapper))
      functions.foreach(f => f._2.map(currentRow, queryState))
      currentRow.moveToNextRow()
    }

    //reuse and reset morsel context
    currentRow.resetToFirstRow()
    result.foreach {
      case (key, aggregator) =>
        addGroupingValuesToResult(currentRow, key)
        var i = 0
        while (i < aggregations.length) {
          val (offset, mapper) = aggregator(i)
          currentRow.setRefAt(offset, mapper.result)
          i += 1
        }
        currentRow.moveToNextRow()
    }
    currentRow.finishedWriting()
  }
}
