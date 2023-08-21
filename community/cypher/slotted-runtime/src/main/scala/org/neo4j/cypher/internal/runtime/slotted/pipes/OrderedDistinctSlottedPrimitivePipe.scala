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
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.pipes.DistinctSlottedPrimitivePipe.buildGroupingValue
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection.DistinctSet
import org.neo4j.values.storable.LongArray

case class OrderedDistinctSlottedPrimitivePipe(
  source: Pipe,
  slots: SlotConfiguration,
  orderedPrimitiveSlots: Array[Int],
  unorderedPrimitiveSlots: Array[Int],
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
      private val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)
      private var seen: DistinctSet[LongArray] = DistinctSet.createDistinctSet[LongArray](memoryTracker)
      private var currentOrderedGroupingValue: LongArray = _

      state.query.resources.trace(seen)

      override def produceNext(): Option[CypherRow] = {
        while (input.hasNext) {
          val next: CypherRow = input.next()

          val unorderedGroupingValue = buildGroupingValue(next, unorderedPrimitiveSlots)
          val orderedGroupingValue = buildGroupingValue(next, orderedPrimitiveSlots)

          if (currentOrderedGroupingValue == null || currentOrderedGroupingValue != orderedGroupingValue) {
            currentOrderedGroupingValue = orderedGroupingValue
            seen.close()
            seen = DistinctSet.createDistinctSet[LongArray](memoryTracker)
          }

          if (seen.add(unorderedGroupingValue)) {
            // Found unseen key! Set it as the next element to yield, and exit
            groupingExpression.project(next, groupingExpression.computeGroupingKey(next, state))
            return Some(next)
          }
        }
        seen.close()
        seen = null
        None
      }

      override protected[this] def closeMore(): Unit = {
        if (seen != null) seen.close()
        input.close()
      }
    }
  }
}

/**
 * Specialization for the case that all groupingColumns are ordered
 */
case class AllOrderedDistinctSlottedPrimitivePipe(
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
      private var currentOrderedGroupingValue: LongArray = _

      override def produceNext(): Option[CypherRow] = {
        while (input.hasNext) {
          val next: CypherRow = input.next()

          val groupingValue = buildGroupingValue(next, primitiveSlots)

          if (currentOrderedGroupingValue == null || currentOrderedGroupingValue != groupingValue) {
            currentOrderedGroupingValue = groupingValue
            // Found unseen key! Set it as the next element to yield, and exit
            groupingExpression.project(next, groupingExpression.computeGroupingKey(next, state))
            return Some(next)
          }
        }
        None
      }

      override protected[this] def closeMore(): Unit = input.close()
    }
  }
}
