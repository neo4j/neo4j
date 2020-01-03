/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DistinctPipe.GroupingCol
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualValues

import scala.collection.mutable

case class DistinctPipe(source: Pipe, groupingColumns: Array[GroupingCol])
                       (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  groupingColumns.map(_.expression).foreach(_.registerOwningPipe(this))

  private val keyNames = groupingColumns.map(_.key)

  protected def internalCreateResults(input: Iterator[ExecutionContext],
                                      state: QueryState): Iterator[ExecutionContext] = {
    /*
     * The filtering is done by extracting from the context the values of all return expressions, and keeping them
     * in a set.
     */
    val seen = mutable.Set[AnyValue]()

    input.filter { ctx =>
      var i = 0
      while (i < groupingColumns.length) {
        ctx.set(groupingColumns(i).key, groupingColumns(i).expression(ctx, state))
        i += 1
      }
      val groupingValue = VirtualValues.list(keyNames.map(ctx.getByName): _*)
      val added = seen.add(groupingValue)
      if (added) {
        state.memoryTracker.allocated(groupingValue)
      }
      added
    }
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case DistinctPipe(otherSource, otherGroupingColumns) =>
        otherSource == this.source && otherGroupingColumns.sameElements(this.groupingColumns)
      case _ => false
    }
  }
}

object DistinctPipe {
  case class GroupingCol(key: String, expression: Expression, ordered: Boolean = false)
}
