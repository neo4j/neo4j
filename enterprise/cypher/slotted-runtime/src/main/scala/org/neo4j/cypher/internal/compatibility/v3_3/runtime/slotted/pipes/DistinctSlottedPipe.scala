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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.Expression
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.Id
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.PrimitiveExecutionContext
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualValues

import scala.collection.mutable

case class DistinctSlottedPipe(source: Pipe,
                               pipelineInformation: PipelineInformation,
                               groupingExpressions: Map[Int, Expression])(val id: Id = new Id)
  extends PipeWithSource(source) {

  private val keyOffsets: Array[Int] = groupingExpressions.keys.toArray

  groupingExpressions.values.foreach(_.registerOwningPipe(this))

  protected def internalCreateResults(input: Iterator[ExecutionContext],
                                      state: QueryState): Iterator[ExecutionContext] = {
    // For each incoming row, run expression and put it into the correct slot in the context
    val result = input.map(incoming => {
      val outgoing = PrimitiveExecutionContext(pipelineInformation)
      groupingExpressions.foreach {
        case (offset, expression) => outgoing.setRefAt(offset, expression(incoming, state))
      }
      outgoing
    })

    /*
     * Filter out rows we have already seen
     */
    var seen = mutable.Set[AnyValue]()
    result.filter { ctx =>
      val values = VirtualValues.list(keyOffsets.map(ctx.getRefAt): _*)
      if (seen.contains(values)) {
        false
      } else {
        seen += values
        true
      }
    }
  }
}
