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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrefetchingIterator
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DistinctPipe.GroupingCol
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection.DistinctSet
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.ListValueBuilder

case class DistinctPipe(source: Pipe, groupingColumns: Array[GroupingCol])(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(source) {

  private val keyNames = groupingColumns.map(_.key)

  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    new PrefetchingIterator[CypherRow] {
      /*
       * The filtering is done by extracting from the context the values of all return expressions, and keeping them
       * in a set.
       */
      private var seen =
        DistinctSet.createDistinctSet[AnyValue](state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x))

      state.query.resources.trace(seen)

      override def produceNext(): Option[CypherRow] = {
        while (input.hasNext) {
          val next: CypherRow = input.next()

          var i = 0
          while (i < groupingColumns.length) {
            next.set(groupingColumns(i).key, groupingColumns(i).expression(next, state))
            i += 1
          }
          val builder = ListValueBuilder.newListBuilder(keyNames.length)
          keyNames.foreach(name => builder.add(next.getByName(name)))
          val groupingValue = builder.build()

          if (seen.add(groupingValue)) {
            return Some(next)
          }
        }
        seen.close()
        seen = null
        None
      }

      override protected[this] def closeMore(): Unit = if (seen != null) seen.close()
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
