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

import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections

case class ExpandAllSlottedPipe(
  source: Pipe,
  fromSlot: Slot,
  relOffset: Int,
  toOffset: Int,
  dir: SemanticDirection,
  types: RelationshipTypes,
  slots: SlotConfiguration
)(val id: Id = Id.INVALID_ID) extends PipeWithSource(source) with Pipe {

  // ===========================================================================
  // Compile-time initializations
  // ===========================================================================
  private val getFromNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(fromSlot)

  // ===========================================================================
  // Runtime code
  // ===========================================================================
  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    input.flatMap {
      inputRow: CypherRow =>
        val fromNode = getFromNodeFunction.applyAsLong(inputRow)

        if (NullChecker.entityIsNull(fromNode)) {
          ClosingIterator.empty
        } else {
          val nodeCursor = state.query.nodeCursor()
          val relCursor = state.query.traversalCursor()
          try {
            val read = state.query.transactionalContext.dataRead
            read.singleNode(fromNode, nodeCursor)
            if (!nodeCursor.next()) {
              ClosingIterator.empty
            } else {
              val selectionCursor = dir match {
                case OUTGOING => RelationshipSelections.outgoingCursor(relCursor, nodeCursor, types.types(state.query))
                case INCOMING => RelationshipSelections.incomingCursor(relCursor, nodeCursor, types.types(state.query))
                case BOTH     => RelationshipSelections.allCursor(relCursor, nodeCursor, types.types(state.query))
              }
              new ExpandIterator(selectionCursor, state.query) {
                override protected def createOutputRow(relationship: Long, otherNode: Long): SlottedRow = {
                  val outputRow = SlottedRow(slots)
                  outputRow.copyAllFrom(inputRow)
                  outputRow.setLongAt(relOffset, relationship)
                  outputRow.setLongAt(toOffset, otherNode)
                  outputRow

                }
              }
            }
          } finally {
            nodeCursor.close()
          }
        }
    }
  }
}

abstract class ExpandIterator(selectionCursor: RelationshipTraversalCursor, queryContext: QueryContext)
    extends ClosingIterator[SlottedRow] {
  queryContext.resources.trace(selectionCursor)

  private var initialized = false
  private var hasMore = false

  override protected[this] def closeMore(): Unit = selectionCursor.close()

  override def innerHasNext: Boolean = {
    if (!initialized) {
      hasMore = selectionCursor.next()
      initialized = true
    }

    hasMore
  }

  protected def createOutputRow(relationship: Long, otherNode: Long): SlottedRow

  override def next(): SlottedRow = {
    if (!hasNext) {
      selectionCursor.close()
      Iterator.empty.next()
    }
    val outputRow = createOutputRow(selectionCursor.relationshipReference(), selectionCursor.otherNodeReference())
    hasMore = selectionCursor.next()
    outputRow
  }
}
