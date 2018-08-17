/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, QueryState}
import org.neo4j.cypher.internal.runtime.interpreted.{CommandProjection, ExecutionContext}

case class SlottedCommandProjection(introducedExpressions: Map[Int, Expression]) extends CommandProjection {

  override def isEmpty: Boolean = introducedExpressions.isEmpty

  override def registerOwningPipe(pipe: Pipe): Unit = introducedExpressions.values.foreach(_.registerOwningPipe(pipe))

  private val projectionFunctions: Iterable[(ExecutionContext, QueryState) => Unit] = introducedExpressions map {
    case (offset, expression) =>
      (ctx: ExecutionContext, state: QueryState) =>
        val result = expression(ctx, state)
        ctx.setRefAt(offset, result)
  }

  override def project(ctx: ExecutionContext, state: QueryState): Unit = projectionFunctions.foreach(_ (ctx, state))
}
