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
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection.DistinctSet
import org.neo4j.values.AnyValue

case class OrderedDistinctSlottedPipe(
  source: Pipe,
  slots: SlotConfiguration,
  orderedGroupingExpression: GroupingExpression,
  unorderedGroupingExpression: GroupingExpression
)(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(source) {

  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    new PrefetchingIterator[CypherRow] {
      private val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)
      private var seen: DistinctSet[AnyValue] = DistinctSet.createDistinctSet[AnyValue](memoryTracker)
      private var currentOrderedGroupingValue: AnyValue = _

      state.query.resources.trace(seen)

      override def produceNext(): Option[CypherRow] = {
        while (input.hasNext) {
          val next: CypherRow = input.next()

          val unorderedGroupingValue = unorderedGroupingExpression.computeGroupingKey(next, state)
          val orderedGroupingValue = orderedGroupingExpression.computeGroupingKey(next, state)

          if (currentOrderedGroupingValue == null || currentOrderedGroupingValue != orderedGroupingValue) {
            currentOrderedGroupingValue = orderedGroupingValue
            seen.close()
            seen = DistinctSet.createDistinctSet[AnyValue](memoryTracker)
          }

          if (seen.add(unorderedGroupingValue)) {
            // Found unseen key! Set it as the next element to yield, and exit
            orderedGroupingExpression.project(next, orderedGroupingValue)
            unorderedGroupingExpression.project(next, unorderedGroupingValue)
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

/**
 * Specialization for the case that all groupingColumns are ordered
 */
case class AllOrderedDistinctSlottedPipe(
  source: Pipe,
  slots: SlotConfiguration,
  groupingExpression: GroupingExpression
)(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(source) {

  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    new PrefetchingIterator[CypherRow] {
      private var currentOrderedGroupingValue: AnyValue = _

      override def produceNext(): Option[CypherRow] = {
        while (input.hasNext) {
          val next: CypherRow = input.next()

          val groupingValue = groupingExpression.computeGroupingKey(next, state)

          if (currentOrderedGroupingValue == null || currentOrderedGroupingValue != groupingValue) {
            currentOrderedGroupingValue = groupingValue
            // Found unseen key! Set it as the next element to yield, and exit
            groupingExpression.project(next, groupingValue)
            return Some(next)
          }
        }
        None
      }

      override protected[this] def closeMore(): Unit = input.close()
    }
  }
}
