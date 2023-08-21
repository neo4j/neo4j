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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrefetchingIterator
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.pipes.DistinctSlottedPrimitivePipe.buildGroupingValue
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection.DistinctSet
import org.neo4j.values.storable.LongArray
import org.neo4j.values.storable.Values

case class DistinctSlottedPrimitivePipe(
  source: Pipe,
  slots: SlotConfiguration,
  primitiveSlots: Array[Int],
  groupingExpression: GroupingExpression
)(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(source) {

  // ===========================================================================
  // Runtime code
  // ===========================================================================
  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    new PrefetchingIterator[CypherRow] {
      private var seen =
        DistinctSet.createDistinctSet[LongArray](state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x))

      state.query.resources.trace(seen)

      override def produceNext(): Option[CypherRow] = {
        while (input.hasNext) {
          val next: CypherRow = input.next()

          val groupingValue = buildGroupingValue(next, primitiveSlots)
          if (seen.add(groupingValue)) {
            // Found unseen key! Set it as the next element to yield, and exit
            val key = groupingExpression.computeGroupingKey(next, state)
            groupingExpression.project(next, key)
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
}

object DistinctSlottedPrimitivePipe {

  def buildGroupingValue(row: ReadableRow, slots: Array[Int]): LongArray = {
    val keys = new Array[Long](slots.length)
    var i = 0
    while (i < slots.length) {
      keys(i) = row.getLongAt(slots(i))
      i += 1
    }
    Values.longArray(keys)
  }
}
