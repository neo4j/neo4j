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
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.ValuePopulation.populate
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.memory.MemoryTracker

case class ProduceResultsPipe(source: Pipe, columns: Array[Expression])(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(source) {
  override def isRootPipe: Boolean = true

  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    // do not register this pipe as parent as it does not do anything except filtering of already fetched
    // key-value pairs and thus should not have any stats
    if (state.prePopulateResults) {
      val query = state.query
      val cursors =
        query.createExpressionCursors() // NOTE: We need to create these through the QueryContext so that they get a profiling tracer if profiling is enabled
      val newState = state.withNewCursors(cursors)
      val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)

      input.map {
        original =>
          produceAndPopulate(original, newState, query, cursors, memoryTracker)
          original
      }
    } else {
      input.map {
        original =>
          produce(original, state)
          original
      }
    }
  }

  private def produceAndPopulate(
    original: CypherRow,
    state: QueryState,
    query: QueryContext,
    cursors: ExpressionCursors,
    memoryTracker: MemoryTracker
  ): Unit = {
    val subscriber = state.subscriber
    val nodeCursor = cursors.nodeCursor
    val relCursor = cursors.relationshipScanCursor
    val propertyCursor = cursors.propertyCursor
    var i = 0
    subscriber.onRecord()
    while (i < columns.length) {
      subscriber.onField(
        i,
        populate(columns(i)(original, state), query, nodeCursor, relCursor, propertyCursor, memoryTracker)
      )
      i += 1
    }
    subscriber.onRecordCompleted()
  }

  private def produce(original: CypherRow, state: QueryState): Unit = {
    val subscriber = state.subscriber
    var i = 0
    subscriber.onRecord()
    while (i < columns.length) {
      subscriber.onField(i, columns(i)(original, state))
      i += 1
    }
    subscriber.onRecordCompleted()
  }
}
