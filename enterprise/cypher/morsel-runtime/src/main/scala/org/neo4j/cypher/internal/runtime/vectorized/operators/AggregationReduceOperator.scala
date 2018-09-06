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
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.runtime.vectorized.expressions.{AggregationHelper, AggregationReducer}
import org.neo4j.values.AnyValue

import scala.collection.mutable

/*
Responsible for reducing the output of AggregationMapperOperatorNoGrouping
 */
class AggregationReduceOperator(aggregations: Array[AggregationOffsets],
                                groupings: Array[GroupingOffsets]) extends ReduceOperator {

  //These are assigned at compile time to save some time at runtime
  private val addGroupingValuesToResult = AggregationHelper.computeGroupingSetter(groupings)(_.reducerOutputSlot)
  private val getGroupingKey = AggregationHelper.computeGroupingGetter(groupings)

  type GroupingKey = AnyValue

  override def init(queryContext: QueryContext,
                    state: QueryState,
                    inputMorsels: Seq[MorselExecutionContext]): ContinuableOperatorTask = {
    new OTask(inputMorsels.toArray)
  }

  class OTask(inputMorsels: Array[MorselExecutionContext]) extends ContinuableOperatorTask {
    private val incomingSlots = aggregations.map(_.mapperOutputSlot)
    private val outgoingSlots = aggregations.map(_.reducerOutputSlot)
    private var aggregates: Iterator[(GroupingKey, Array[AggregationReducer])] = _

    override def operate(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {
      if (null == aggregates) {
        aggregates = aggregateInputs(inputMorsels)
      }
      while (aggregates.hasNext && outputRow.hasMoreRows) {
        val (key, reducers) = aggregates.next()
        addGroupingValuesToResult(outputRow, key)
        var i = 0
        while (i < aggregations.length) {
          val reducer = reducers(i)
          outputRow.setRefAt(outgoingSlots(i), reducer.result)
          i += 1
        }
        outputRow.moveToNextRow()
      }
      outputRow.finishedWriting()
    }

    override def canContinue: Boolean = aggregates.hasNext

    private def aggregateInputs(inputMorsels: Array[MorselExecutionContext]) = {
      var morselPos = 0
      val result =  mutable.LinkedHashMap[GroupingKey, Array[AggregationReducer]]()
      while (morselPos < inputMorsels.length) {
        val currentIncomingRow = inputMorsels(morselPos)
        while (currentIncomingRow.hasMoreRows) {
          val key = getGroupingKey(currentIncomingRow)
          val reducersForKey = result.getOrElseUpdate(key, aggregations.map(_.aggregation.createAggregationReducer))
          var i = 0
          while (i < aggregations.length) {
            val reducer = reducersForKey(i)
            reducer.reduce(currentIncomingRow.getRefAt(incomingSlots(i)))
            i += 1
          }
          currentIncomingRow.moveToNextRow()
        }
        morselPos += 1
      }
      result.iterator
    }
  }
}
