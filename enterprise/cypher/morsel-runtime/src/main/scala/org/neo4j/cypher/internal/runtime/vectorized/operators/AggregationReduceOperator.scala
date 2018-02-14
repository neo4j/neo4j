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

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.runtime.vectorized.expressions.{AggregationHelper, AggregationReducer}
import org.neo4j.values.AnyValue

import scala.collection.mutable.{Map => MutableMap}


/*
Responsible for reducing the output of AggregationMapperOperatorNoGrouping
 */
class AggregationReduceOperator(slots: SlotConfiguration, aggregations: Array[AggregationOffsets], groupings: Array[GroupingOffsets]) extends Operator {

  //These are assigned at compile time to save some time at runtime
  private val addGroupingValuesToResult = AggregationHelper.computeGroupingSetter(groupings)
  private val getGroupingKey = AggregationHelper.computeGroupingGetter(groupings)

  override def operate(message: Message, output: Morsel, context: QueryContext, state: QueryState): Continuation = {
    var iterationState: Iteration = null
    var morselPos = 0
    var morsels: Array[Morsel] = null
    val longCount = slots.numberOfLongs
    val refCount = slots.numberOfReferences

    //This operator will never be "interrupted" since it will never overflow the morsel
    message match {
      case StartLoopWithEagerData(inputs, is) =>
        iterationState = is
        morsels = inputs
      case _ => throw new IllegalStateException()
    }

    //Go through the morsels and collect the output from the map step
    //and reduce the values
    val result = MutableMap[AnyValue, Array[(Int,Int,AggregationReducer)]]()

    while (morselPos < morsels.length) {
      val data = morsels(morselPos)
      val currentRow = new MorselExecutionContext(data, longCount, refCount, currentRow = 0)
      while (currentRow.currentRow < data.validRows) {
        val key = getGroupingKey(currentRow)
        val functions = result.getOrElseUpdate(key, aggregations
          .map(a => (a.incoming, a.outgoing, a.aggregation.createAggregationReducer)))
        var i = 0
        while (i < functions.length) {
          val (incoming, _, reducer) = functions(i)
          reducer.reduce(data.refs(currentRow.currentRow * refCount + incoming))
          i += 1
        }
        currentRow.currentRow += 1
      }
      morselPos += 1
    }

    //Write the reduced value to output
    //reuse and reset morsel context
    val currentRow = new MorselExecutionContext(output, longCount, refCount, currentRow = 0)
    result.foreach {
      case (key, aggregator) =>
        addGroupingValuesToResult(currentRow, key)
        var i = 0
        while (i < aggregations.length) {
          val (_, offset, reducer) = aggregator(i)
          output.refs(currentRow.currentRow * refCount + offset) = reducer.result
          i += 1
        }
        currentRow.currentRow += 1
    }
    output.validRows = currentRow.currentRow

    //we are done
    EndOfLoop(iterationState)
  }

  override def addDependency(pipeline: Pipeline): Dependency = Eager(pipeline)
}
