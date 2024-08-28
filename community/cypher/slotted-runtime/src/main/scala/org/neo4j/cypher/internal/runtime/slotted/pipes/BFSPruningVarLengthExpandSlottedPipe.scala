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
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.Expand.ExpansionMode
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.NO_ENTITY_FUNCTION
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.BFSPruningVarLengthExpandPipe.bfsIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TraversalPredicates
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.storable.Values

case class BFSPruningVarLengthExpandSlottedPipe(
  source: Pipe,
  fromSlot: Slot,
  toSlot: Slot,
  maybeDepthOffset: Option[Int],
  types: RelationshipTypes,
  dir: SemanticDirection,
  includeStartNode: Boolean,
  max: Int,
  slots: SlotConfiguration,
  mode: ExpansionMode,
  predicates: TraversalPredicates
)(val id: Id = Id.INVALID_ID) extends PipeWithSource(source) with Pipe {
  self =>

  private val getFromNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(fromSlot, throwOnTypeError = false)

  private val getToNodeFunction =
    if (mode == ExpandAll) NO_ENTITY_FUNCTION // We only need this getter in the ExpandInto case
    else makeGetPrimitiveNodeFromSlotFunctionFor(toSlot, throwOnTypeError = false)

  private val emitDepth: Boolean = maybeDepthOffset.nonEmpty
  private val depthOffset: Int = maybeDepthOffset.getOrElse(-1)

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    input.flatMap {
      inputRow =>
        {
          val fromNode = getFromNodeFunction.applyAsLong(inputRow)
          val toNode = getToNodeFunction.applyAsLong(inputRow)
          if (entityIsNull(fromNode) || (mode == ExpandInto && entityIsNull(toNode))) {
            ClosingIterator.empty
          } else {
            if (predicates.filterNode(inputRow, state, state.query.nodeById(fromNode))) {

              val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)
              val expand = bfsIterator(
                state.query,
                fromNode,
                toNode,
                types,
                dir,
                includeStartNode,
                max,
                mode,
                predicates.asNodeIdPredicate(inputRow, state),
                predicates.asRelCursorPredicate(inputRow, state),
                memoryTracker
              )

              PrimitiveLongHelper.map(
                expand,
                endNode => {
                  val outputRow = SlottedRow(slots)
                  outputRow.copyAllFrom(inputRow)
                  outputRow.setLongAt(toSlot.offset, endNode)
                  if (emitDepth) {
                    outputRow.setRefAt(depthOffset, Values.intValue(expand.currentDepth))
                  }
                  outputRow
                }
              )
            } else {
              ClosingIterator.empty
            }

          }
        }
    }
  }
}
