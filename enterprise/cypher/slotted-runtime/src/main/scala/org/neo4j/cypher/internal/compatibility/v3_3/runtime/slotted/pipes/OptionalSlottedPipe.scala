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

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.PrimitiveExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionContext, PipelineInformation}
import org.neo4j.cypher.internal.v3_3.logical.plans.LogicalPlanId

case class OptionalSlottedPipe(source: Pipe, nullableOffsets: Seq[Int],
                               pipelineInformation: PipelineInformation)
                              (val id: LogicalPlanId = LogicalPlanId.DEFAULT)
  extends PipeWithSource(source) with Pipe {

  private def notFoundExecutionContext(state: QueryState): ExecutionContext = {
    val context = PrimitiveExecutionContext(pipelineInformation)
    state.copyArgumentStateTo(context)
    // TODO: This can probably be done with java.util.Arrays.fill knowing the first offset
    nullableOffsets.foreach(offset => context.setLongAt(offset, -1))
    context
  }

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    if (input.isEmpty) {
      Iterator(notFoundExecutionContext(state))
    } else {
      input
    }
}
