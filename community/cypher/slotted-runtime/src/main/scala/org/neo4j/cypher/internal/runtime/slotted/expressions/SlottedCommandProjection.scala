/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.interpreted.CommandProjection
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedCommandProjection.projectFunctions

case class SlottedCommandProjection(introducedExpressions: Map[Int, Expression]) extends CommandProjection {

  override def isEmpty: Boolean = introducedExpressions.isEmpty

  private val projectionFunctions: Array[(ReadWriteRow, QueryState) => Unit] =
    projectFunctions(introducedExpressions).toArray

  override def project(ctx: ReadWriteRow, state: QueryState): Unit = {
    var i = 0
    val length = projectionFunctions.length
    while (i < length) {
      projectionFunctions(i).apply(ctx, state)
      i += 1
    }
  }
}

object SlottedCommandProjection {

  def projectFunctions(expressions: Map[Int, Expression]): Iterable[(ReadWriteRow, QueryState) => Unit] = {
    expressions map {
      case (offset, expression) =>
        (ctx: ReadWriteRow, state: QueryState) =>
          val result = expression(ctx, state)
          ctx.setRefAt(offset, result)
    }
  }

}
