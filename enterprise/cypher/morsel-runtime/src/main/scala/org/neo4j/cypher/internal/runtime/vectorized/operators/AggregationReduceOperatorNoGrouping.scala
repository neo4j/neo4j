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
import org.neo4j.cypher.internal.runtime.vectorized.expressions.AggregationReducer

case class ReducerOffsets(incoming: Int, outgoing: Int, aggregation: AggregationReducer)

/*
Responsible for reducing the output of AggregationMapperOperatorNoGrouping
 */
class AggregationReduceOperatorNoGrouping(slots: SlotConfiguration, aggregations: Array[ReducerOffsets]) extends Operator {

  override def operate(message: Message, output: Morsel, context: QueryContext, state: QueryState): Continuation = {
    var iterationState: Iteration = null
    var morselPos = 0
    var morsels: Array[Morsel] = null

    //This operator will never be "interrupted" since it will always write
    //a single value
    message match {
      case StartLoopWithEagerData(inputs, is) =>
        iterationState = is
        morsels = inputs
      case _ => throw new IllegalStateException()
    }

    //Go through the morels and collect the output from the map step
    //and reduce the values
    while (morselPos < morsels.length) {
      val data = morsels(morselPos)
      var i = 0
      while (i < aggregations.length) {
        val offsets = aggregations(i)
        offsets.aggregation.reduce(data.refs(offsets.incoming))
        i += 1
      }
      morselPos += 1
    }

    //Write the reduced value to output
    var writePos = 0
    while (writePos < aggregations.length) {
        val offsets = aggregations(writePos)
        output.refs(offsets.outgoing) = offsets.aggregation.result
      writePos += 1
    }
    output.validRows = writePos

    //we are done
    EndOfLoop(iterationState)
  }
  override def addDependency(pipeline: Pipeline): Dependency = Eager(pipeline)
}
