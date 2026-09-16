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
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyType
import org.neo4j.cypher.internal.runtime.interpreted.pipes.MergeIntoPipe.mergeIntoIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.MergePropertySets
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP

class MergeIntoSlottedPipe(
  src: Pipe,
  fromSlot: Slot,
  direction: SemanticDirection,
  relOffset: Int,
  toSlot: Slot,
  lazyType: LazyType,
  onMatchProperties: MergePropertySets,
  onCreateProperties: MergePropertySets,
  slots: SlotConfiguration,
  fromName: String,
  relName: String,
  toName: String
)(val id: Id = Id.INVALID_ID) extends PipeWithSource(src) {
  private val getFromNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(fromSlot)
  private val getToNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(toSlot)

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val query = state.query
    input.flatMap {
      inputRow =>
        val relType = lazyType.getOrCreateType(inputRow, state)
        val fromNode = getFromNodeFunction.applyAsLong(inputRow)
        val toNode = getToNodeFunction.applyAsLong(inputRow)

        if (entityIsNull(fromNode) || entityIsNull(toNode)) {
          if (state.lenientCreateRelationship) {
            val outputRow = SlottedRow(slots)
            outputRow.copyAllFrom(inputRow)
            outputRow.setLongAt(relOffset, NO_SUCH_RELATIONSHIP)
            ClosingIterator.single(outputRow)
          } else {
            val missingName = if (entityIsNull(fromNode)) fromName else toName
            throw InvalidArgumentException.createRelationshipMissingNode(relName, missingName)
          }
        } else {
          PrimitiveLongHelper.map(
            mergeIntoIterator(
              query,
              fromNode,
              direction,
              relType,
              toNode,
              onMatchProperties.compute(inputRow, state),
              onCreateProperties.compute(inputRow, state)
            ),
            (relId: Long) => {
              val outputRow = SlottedRow(slots)
              outputRow.copyAllFrom(inputRow)
              outputRow.setLongAt(relOffset, relId)
              outputRow
            }
          )
        }
    }
  }
}
