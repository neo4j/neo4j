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
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingLongIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.RelationshipCursorIterator
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.DirectionConverter.toGraphDb
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExpandIntoPipe.traceRelationshipSelectionCursor
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.helpers.CachingExpandInto
import org.neo4j.values.storable.Values

abstract class OptionalExpandIntoSlottedPipe(
  source: Pipe,
  fromSlot: Slot,
  relOffset: Int,
  toSlot: Slot,
  dir: SemanticDirection,
  lazyTypes: RelationshipTypes,
  slots: SlotConfiguration
) extends PipeWithSource(source) {
  self =>

  // ===========================================================================
  // Compile-time initializations
  // ===========================================================================
  private val kernelDirection = toGraphDb(dir)
  private val getFromNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(fromSlot)
  private val getToNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(toSlot)

  // ===========================================================================
  // Runtime code
  // ===========================================================================
  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val query = state.query
    val expandInto = new CachingExpandInto(
      query.transactionalContext.kernelQueryContext,
      kernelDirection,
      state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)
    )
    state.query.resources.trace(expandInto)
    input.flatMap {
      (inputRow: CypherRow) =>
        val fromNode = getFromNodeFunction.applyAsLong(inputRow)
        val toNode = getToNodeFunction.applyAsLong(inputRow)

        if (entityIsNull(fromNode) || entityIsNull(toNode)) {
          ClosingIterator.single(withNulls(inputRow))
        } else {
          val traversalCursor = query.traversalCursor()
          val nodeCursor = query.nodeCursor()
          try {
            val selectionCursor =
              expandInto.connectingRelationships(nodeCursor, traversalCursor, fromNode, lazyTypes.types(query), toNode)
            traceRelationshipSelectionCursor(query.resources, selectionCursor, traversalCursor)
            val relationships = new RelationshipCursorIterator(selectionCursor, traversalCursor)
            val matchIterator = findMatchIterator(inputRow, state, relationships)

            if (matchIterator.isEmpty) ClosingIterator.single(withNulls(inputRow))
            else matchIterator
          } finally {
            nodeCursor.close()
          }
        }
    }.closing(expandInto)
  }

  def findMatchIterator(
    inputRow: CypherRow,
    state: QueryState,
    relationships: ClosingLongIterator
  ): ClosingIterator[SlottedRow]

  private def withNulls(inputRow: CypherRow) = {
    val outputRow = SlottedRow(slots)
    outputRow.copyAllFrom(inputRow)
    outputRow.setLongAt(relOffset, -1)
    outputRow
  }

}

object OptionalExpandIntoSlottedPipe {

  def apply(
    source: Pipe,
    fromSlot: Slot,
    relOffset: Int,
    toSlot: Slot,
    dir: SemanticDirection,
    lazyTypes: RelationshipTypes,
    slots: SlotConfiguration,
    maybePredicate: Option[Expression]
  )(id: Id = Id.INVALID_ID): OptionalExpandIntoSlottedPipe = maybePredicate match {
    case Some(predicate) =>
      FilteringOptionalExpandIntoSlottedPipe(source, fromSlot, relOffset, toSlot, dir, lazyTypes, slots, predicate)(id)
    case None =>
      NonFilteringOptionalExpandIntoSlottedPipe(source, fromSlot, relOffset, toSlot, dir, lazyTypes, slots)(id)
  }
}

case class NonFilteringOptionalExpandIntoSlottedPipe(
  source: Pipe,
  fromSlot: Slot,
  relOffset: Int,
  toSlot: Slot,
  dir: SemanticDirection,
  lazyTypes: RelationshipTypes,
  slots: SlotConfiguration
)(val id: Id)
    extends OptionalExpandIntoSlottedPipe(source: Pipe, fromSlot, relOffset, toSlot, dir, lazyTypes, slots) {

  override def findMatchIterator(
    inputRow: CypherRow,
    state: QueryState,
    relationships: ClosingLongIterator
  ): ClosingIterator[SlottedRow] = {
    PrimitiveLongHelper.map(
      relationships,
      relId => {
        val outputRow = SlottedRow(slots)
        outputRow.copyAllFrom(inputRow)
        outputRow.setLongAt(relOffset, relId)
        outputRow
      }
    )
  }
}

case class FilteringOptionalExpandIntoSlottedPipe(
  source: Pipe,
  fromSlot: Slot,
  relOffset: Int,
  toSlot: Slot,
  dir: SemanticDirection,
  lazyTypes: RelationshipTypes,
  slots: SlotConfiguration,
  predicate: Expression
)(val id: Id)
    extends OptionalExpandIntoSlottedPipe(source: Pipe, fromSlot, relOffset, toSlot, dir, lazyTypes, slots) {

  override def findMatchIterator(
    inputRow: CypherRow,
    state: QueryState,
    relationships: ClosingLongIterator
  ): ClosingIterator[SlottedRow] = {
    PrimitiveLongHelper.map(
      relationships,
      relId => {
        val outputRow = SlottedRow(slots)
        outputRow.copyAllFrom(inputRow)
        outputRow.setLongAt(relOffset, relId)
        outputRow
      }
    ).filter(ctx => predicate(ctx, state) eq Values.TRUE)
  }
}
