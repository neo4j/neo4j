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

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.util.v3_4.attribution.Id

/*
Projection evaluates expressions and stores their values into new slots in the execution context.
It's an additive operation - nothing is lost in the execution context, the pipe simply adds new key-value pairs.
 */
case class ProjectionSlottedPipe(source: Pipe, introducedExpressions: Map[Int, Expression])
                                (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  introducedExpressions.values.foreach(_.registerOwningPipe(this))

  private val projectionFunctions: Iterable[(ExecutionContext, QueryState) => Unit] = introducedExpressions map {
    case (offset, expression) =>
      (ctx: ExecutionContext, state: QueryState) =>
        val result = expression(ctx, state)
        ctx.setRefAt(offset, result)
  }

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    if (projectionFunctions.isEmpty)
      input
    else {
      input.map {
        ctx =>
          projectionFunctions.foreach(_ (ctx, state))
          ctx
      }
    }
  }
}
