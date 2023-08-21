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
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections
import org.neo4j.values.storable.Values

abstract class OptionalExpandAllSlottedPipe(
  source: Pipe,
  fromSlot: Slot,
  relOffset: Int,
  toOffset: Int,
  dir: SemanticDirection,
  types: RelationshipTypes,
  slots: SlotConfiguration
) extends PipeWithSource(source) with Pipe {

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
          ClosingIterator.single(withNulls(inputRow))
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
              val matchIterator = filter(
                new ExpandIterator(selectionCursor, state.query) {
                  override protected def createOutputRow(relationship: Long, otherNode: Long): SlottedRow = {
                    val outputRow = SlottedRow(slots)
                    outputRow.copyAllFrom(inputRow)
                    outputRow.setLongAt(relOffset, relationship)
                    outputRow.setLongAt(toOffset, otherNode)
                    outputRow
                  }
                },
                state
              )

              if (matchIterator.isEmpty)
                ClosingIterator.single(withNulls(inputRow))
              else
                matchIterator
            }
          } finally {
            nodeCursor.close()
          }
        }
    }
  }
  def filter(iterator: ClosingIterator[SlottedRow], state: QueryState): ClosingIterator[SlottedRow]

  private def withNulls(inputRow: CypherRow): SlottedRow = {
    val outputRow = SlottedRow(slots)
    outputRow.copyAllFrom(inputRow)
    outputRow.setLongAt(relOffset, -1)
    outputRow.setLongAt(toOffset, -1)
    outputRow
  }
}

object OptionalExpandAllSlottedPipe {

  def apply(
    source: Pipe,
    fromSlot: Slot,
    relOffset: Int,
    toOffset: Int,
    dir: SemanticDirection,
    types: RelationshipTypes,
    slots: SlotConfiguration,
    maybePredicate: Option[Expression]
  )(id: Id = Id.INVALID_ID): OptionalExpandAllSlottedPipe = maybePredicate match {
    case Some(predicate) =>
      FilteringOptionalExpandAllSlottedPipe(source, fromSlot, relOffset, toOffset, dir, types, slots, predicate)(id)
    case None => NonFilteringOptionalExpandAllSlottedPipe(source, fromSlot, relOffset, toOffset, dir, types, slots)(id)
  }
}

case class NonFilteringOptionalExpandAllSlottedPipe(
  source: Pipe,
  fromSlot: Slot,
  relOffset: Int,
  toOffset: Int,
  dir: SemanticDirection,
  types: RelationshipTypes,
  slots: SlotConfiguration
)(val id: Id)
    extends OptionalExpandAllSlottedPipe(source: Pipe, fromSlot, relOffset, toOffset, dir, types, slots) {

  override def filter(iterator: ClosingIterator[SlottedRow], state: QueryState): ClosingIterator[SlottedRow] = iterator
}

case class FilteringOptionalExpandAllSlottedPipe(
  source: Pipe,
  fromSlot: Slot,
  relOffset: Int,
  toOffset: Int,
  dir: SemanticDirection,
  types: RelationshipTypes,
  slots: SlotConfiguration,
  predicate: Expression
)(val id: Id)
    extends OptionalExpandAllSlottedPipe(source: Pipe, fromSlot, relOffset, toOffset, dir, types, slots) {

  override def filter(iterator: ClosingIterator[SlottedRow], state: QueryState): ClosingIterator[SlottedRow] =
    iterator.filter(ctx => predicate(ctx, state) eq Values.TRUE)
}
