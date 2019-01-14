/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AggregationExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.util.v3_4.attribution.Id

/*
This pipe can be used whenever we are aggregating and not grouping on anything
 */
case class EagerAggregationWithoutGroupingSlottedPipe(source: Pipe,
                                                      slots: SlotConfiguration,
                                                      aggregations: Map[Int, AggregationExpression])
                                                     (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {

  aggregations.values.foreach(_.registerOwningPipe(this))

  private val (aggregationOffsets: IndexedSeq[Int], aggregationFunctions: IndexedSeq[AggregationExpression]) = {
    val (a, b) = aggregations.unzip
    (a.toIndexedSeq, b.toIndexedSeq)
  }

  protected def internalCreateResults(input: Iterator[ExecutionContext],
                                      state: QueryState): Iterator[ExecutionContext] = {

    val aggregationAccumulators: Seq[AggregationFunction] = aggregationFunctions.map(_.createAggregationFunction)

    if (input.isEmpty)
      createEmptyResult(state)
    else {
      // Consume input
      input.foreach { ctx =>
        aggregationAccumulators.foreach(f => f.apply(ctx, state))
      }

      // Present result
      val context = SlottedExecutionContext(slots)
      (aggregationOffsets zip aggregationAccumulators).foreach {
        case (offset, value) => context.setRefAt(offset, value.result(state))
      }
      Iterator(context)
    }
  }

  // Used when we have no input and no grouping expressions. In this case, we'll return a single row
  def createEmptyResult(state: QueryState): Iterator[ExecutionContext] = {
    val context = SlottedExecutionContext(slots)
    val aggregationOffsetsAndFunctions = aggregationOffsets zip aggregations
      .map(_._2.createAggregationFunction.result(state))

    aggregationOffsetsAndFunctions.toMap.foreach {
      case (offset, zeroValue) => context.setRefAt(offset, zeroValue)
    }
    Iterator.single(context)
  }
}
