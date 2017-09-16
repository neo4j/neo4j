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

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.Expression
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionContext, LongSlot, RefSlot, Slot}
import org.neo4j.cypher.internal.v3_3.logical.plans.LogicalPlanId

/*
Projection evaluates expressions and stores their values into new slots in the execution context.
It's an additive operation - nothing is lost in the execution context, the pipe simply adds new key-value pairs.
 */
case class ProjectionSlottedPipe(source: Pipe, introducedExpressions: Map[Slot, Expression])
                                (val id: LogicalPlanId = LogicalPlanId.DEFAULT) extends PipeWithSource(source) {

  introducedExpressions.values.foreach(_.registerOwningPipe(this))

  private val projectionFunctions = introducedExpressions map {
    case (LongSlot(offset, _, _, _), expression) =>
      // We just pass along Long slot expressions without evaluation
      (ctx: ExecutionContext, state: QueryState) =>

    case (RefSlot(offset, _, _, _), expression) =>
      (ctx: ExecutionContext, state: QueryState) =>
        val result = expression(ctx, state)
        ctx.setRefAt(offset, result)
  }
  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.map {
      ctx =>
        projectionFunctions.foreach(_(ctx, state))
        ctx
    }
  }
}
