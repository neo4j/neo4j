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
import org.neo4j.cypher.internal.runtime.vectorized._

/*
Responsible for reducing the output of AggregationMapperOperatorNoGrouping
 */
class AggregationReduceOperatorNoGrouping(slots: SlotConfiguration, aggregations: Array[AggregationOffsets]) extends Operator {

  override def operate(message: Message, output: Morsel, context: QueryContext, state: QueryState): Continuation = {
    val reducers = aggregations.map(_.aggregation.createAggregationReducer)
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

    //Go through the morsels and collect the output from the map step
    //and reduce the values
    while (morselPos < morsels.length) {
      val data = morsels(morselPos)
      var i = 0
      while (i < aggregations.length) {
        reducers(i).reduce(data.refs(aggregations(i).incoming))
        i += 1
      }
      morselPos += 1
    }

    //Write the reduced value to output
    var i = 0
    while (i < aggregations.length) {
      output.refs(aggregations(i).outgoing) = reducers(i).result
      i += 1
    }

    //we are done, we have written a single row to the output
    output.validRows = 1
    EndOfLoop(iterationState)
  }
  override def addDependency(pipeline: Pipeline): Dependency = Eager(pipeline)
}
