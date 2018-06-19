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

/*
Responsible for reducing the output of AggregationMapperOperatorNoGrouping
 */
class AggregationReduceOperatorNoGrouping(aggregations: Array[AggregationOffsets]) extends ReduceOperator {

  override def init(queryContext: QueryContext,
                    state: QueryState,
                    inputMorsels: Seq[MorselExecutionContext]
                   ): ContinuableOperatorTask =
    new OTask(inputMorsels.toArray)

  class OTask(inputMorsels: Array[MorselExecutionContext]) extends ContinuableOperatorTask {

    override def operate(currentRow: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState): Unit = {

      val incomingSlots = aggregations.map(_.mapperOutputSlot)
      val reducers = aggregations.map(_.aggregation.createAggregationReducer)

      //Go through the morsels and collect the output from the map step
      //and reduce the values
      var i = 0
      while (i < inputMorsels.length) {
        val currentInputRow = inputMorsels(i)
        var j = 0
        while (j < aggregations.length) {
          reducers(j).reduce(currentInputRow.getRefAt(incomingSlots(j)))
          j += 1
        }
        i += 1
      }

      //Write the reduced value to output
      i = 0
      while (i < aggregations.length) {
        currentRow.setRefAt(aggregations(i).reducerOutputSlot, reducers(i).result)
        i += 1
      }
      currentRow.moveToNextRow()
      currentRow.finishedWriting()
    }

    // This operator will never continue since it will always write a single row
    override def canContinue: Boolean = false
  }
}
