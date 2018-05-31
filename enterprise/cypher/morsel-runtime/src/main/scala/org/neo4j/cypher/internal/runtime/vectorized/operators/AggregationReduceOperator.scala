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

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
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
    val longCount = slots.numberOfLongs
    val refCount = slots.numberOfReferences
    var writePos = 0
    var iterator: Iterator[(AnyValue, Array[(Int, Int, AggregationReducer)])] = null

    message match {
      case StartLoopWithEagerData(inputs, is) =>
        iterationState = is
        iterator = getIterator(inputs, longCount, refCount)

      case ContinueLoopWith(ContinueWithSource(it: Iterator[_], is)) =>
        iterator = it.asInstanceOf[Iterator[(AnyValue, Array[(Int, Int, AggregationReducer)])]]
        iterationState = is
      case _ => throw new IllegalStateException()
    }

    val currentRow = new MorselExecutionContext(output, longCount, refCount, currentRow = 0)
    while (iterator.hasNext && writePos < output.validRows) {
      val (key, aggregator) = iterator.next()
      addGroupingValuesToResult(currentRow, key)
      var i = 0
      while (i < aggregations.length) {
        val (_, offset, reducer) = aggregator(i)
        output.refs(currentRow.currentRow * refCount + offset) = reducer.result
        i += 1
      }
      writePos +=1
      currentRow.currentRow += 1
    }
    output.validRows = writePos

    if (iterator.hasNext) ContinueWithSource(iterator, iterationState)
    else EndOfLoop(iterationState)
  }

  private def getIterator(morsels: Array[Morsel], longCount: Int, refCount: Int) = {
    var morselPos = 0
    val result = MutableMap[AnyValue, Array[(Int, Int, AggregationReducer)]]()
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
    result.iterator
  }

  override def addDependency(pipeline: Pipeline): Dependency = Eager(pipeline)
}
