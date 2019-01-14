/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.InterpretedCommandProjection
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.{CommandProjection, ExecutionContext}
import org.neo4j.cypher.internal.v3_5.util.attribution.Id

case class ProjectionPipe(source: Pipe, projection: CommandProjection)
                         (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  projection.registerOwningPipe(this)

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    if (projection.isEmpty)
      input
    else {
      input.map {
        ctx =>
          projection.project(ctx, state)
          ctx
      }
    }
  }
}

object ProjectionPipe {
  def apply(source: Pipe, projections: Map[String, Expression]): ProjectionPipe =
    ProjectionPipe(source, InterpretedCommandProjection(projections))()
}
