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
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.helpers.NullChecker
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionContext, PipelineInformation}
import org.neo4j.cypher.internal.v3_3.logical.plans.LogicalPlanId
import org.neo4j.values.storable.Values

case class ConditionalApplySlottedPipe(lhs: Pipe, rhs: Pipe, longOffsets: Seq[Int], refOffsets: Seq[Int], negated: Boolean,
                                       pipelineInformation: PipelineInformation)
                                      (val id: LogicalPlanId = LogicalPlanId.DEFAULT)
  extends PipeWithSource(lhs) with Pipe {

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    input.flatMap {
      lhsContext =>

        if (condition(lhsContext)) {
          val rhsState = state.withInitialContext(lhsContext)
          rhs.createResults(rhsState)
        }
        else {
          val output = PrimitiveExecutionContext(pipelineInformation)
          output.copyFrom(lhsContext, pipelineInformation.initialNumberOfLongs, pipelineInformation.initialNumberOfReferences)
          Iterator.single(output)
        }
    }

  private def condition(context: ExecutionContext) = {
    val cond = longOffsets.exists(offset => !NullChecker.nodeIsNull(context.getLongAt(offset))) ||
      refOffsets.exists(context.getRefAt(_) != Values.NO_VALUE)
    if (negated) !cond else cond
  }

}
