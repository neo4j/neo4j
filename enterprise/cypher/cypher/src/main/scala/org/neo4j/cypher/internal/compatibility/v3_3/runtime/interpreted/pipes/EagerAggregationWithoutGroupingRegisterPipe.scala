/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.AggregationExpression
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.PrimitiveExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.Id
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionContext, PipelineInformation}

/*
This pipe can be used whenever we are aggregating and not grouping on anything
 */
case class EagerAggregationWithoutGroupingRegisterPipe(source: Pipe,
                                                       pipelineInformation: PipelineInformation,
                                                       aggregations: Map[Int, AggregationExpression])(val id: Id = new Id)
  extends PipeWithSource(source) {

  aggregations.values.foreach(_.registerOwningPipe(this))

  private val (aggregationOffsets: IndexedSeq[Int], aggregationFunctions: IndexedSeq[AggregationExpression]) = {
    val (a, b) = aggregations.unzip
    (a.toIndexedSeq, b.toIndexedSeq)
  }

  protected def internalCreateResults(input: Iterator[ExecutionContext],
                                      state: QueryState): Iterator[ExecutionContext] = {

    implicit val s = state

    val aggregationAccumulators: Seq[AggregationFunction] = aggregationFunctions.map(_.createAggregationFunction)

    if (input.isEmpty)
      createEmptyResult(state)
    else {
      // Consume input
      input.foreach { ctx =>
        aggregationAccumulators.foreach(f => f.apply(ctx))
      }

      // Present result
      val context = PrimitiveExecutionContext(pipelineInformation)
      (aggregationOffsets zip aggregationAccumulators).foreach {
        case (offset, value) => context.setRefAt(offset, value.result)
      }
      Iterator(context)
    }
  }

  // Used when we have no input and no grouping expressions. In this case, we'll return a single row
  def createEmptyResult(state: QueryState): Iterator[ExecutionContext] = {
    val context = PrimitiveExecutionContext(pipelineInformation)
    val aggregationOffsetsAndFunctions = aggregationOffsets zip aggregations
      .map(_._2.createAggregationFunction.result(state))

    aggregationOffsetsAndFunctions.toMap.foreach {
      case (offset, zeroValue) => context.setRefAt(offset, zeroValue)
    }
    Iterator.single(context)
  }
}
