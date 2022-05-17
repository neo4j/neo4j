/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.collection.trackable.HeapTrackingCollections
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.physicalplanning.VariablePredicates
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.RelationshipContainer
import org.neo4j.cypher.internal.runtime.RelationshipIterator
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.interpreted.pipes.VarLengthExpandPipe.projectBackwards
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.runtime.slotted.pipes.VarLengthExpandSlottedPipe.predicateIsTrue
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

/**
 * On predicates... to communicate the tested entity to the predicate, expressions
 * variable slots have been allocated. The offsets of these slots are `temp*Offset`.
 * If no predicate exists the offset will be `SlottedPipeMapper.NO_PREDICATE_OFFSET`
 */
case class VarLengthExpandSlottedPipe(
  source: Pipe,
  fromSlot: Slot,
  relOffset: Int,
  toSlot: Slot,
  dir: SemanticDirection,
  projectedDir: SemanticDirection,
  types: RelationshipTypes,
  min: Int,
  maxDepth: Option[Int],
  shouldExpandAll: Boolean,
  slots: SlotConfiguration,
  tempNodeOffset: Int,
  tempRelationshipOffset: Int,
  nodePredicate: Expression,
  relationshipPredicate: Expression,
  argumentSize: SlotConfiguration.Size
)(val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {
  type LNode = Long

  // ===========================================================================
  // Compile-time initializations
  // ===========================================================================
  private val getFromNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(fromSlot, throwOnTypeError = false)

  private val getToNodeFunction =
    if (shouldExpandAll) {
      null
    } // We only need this getter in the ExpandInto case
    else {
      makeGetPrimitiveNodeFromSlotFunctionFor(toSlot, throwOnTypeError = false)
    }
  private val toOffset = toSlot.offset

  // ===========================================================================
  // Runtime code
  // ===========================================================================

  private def varLengthExpand(
    node: LNode,
    state: QueryState,
    row: CypherRow
  ): ClosingIterator[(LNode, RelationshipContainer)] = {
    val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)
    val stackOfNodes = HeapTrackingCollections.newLongStack(memoryTracker)
    val stackOfRelContainers = HeapTrackingCollections.newArrayDeque[RelationshipContainer](memoryTracker)
    stackOfNodes.push(node)
    stackOfRelContainers.push(RelationshipContainer.EMPTY)

    new ClosingIterator[(LNode, RelationshipContainer)] {
      override def next(): (LNode, RelationshipContainer) = {
        val fromNode = stackOfNodes.pop()
        val rels = stackOfRelContainers.pop()
        if (rels.size < maxDepth.getOrElse(Int.MaxValue)) {
          val relationships: RelationshipIterator =
            state.query.getRelationshipsForIds(fromNode, dir, types.types(state.query))

          // relationships get immediately exhausted. Therefore we do not need a ClosingIterator here.
          while (relationships.hasNext) {
            val relId = relationships.next()
            val relationshipIsUniqueInPath = !rels.contains(relId)

            if (relationshipIsUniqueInPath) {
              // Before expanding, check that both the relationship and node in question fulfil the predicate
              if (
                predicateIsTrue(
                  row,
                  state,
                  tempRelationshipOffset,
                  relationshipPredicate,
                  state.query.relationshipById(
                    relId,
                    relationships.startNodeId(),
                    relationships.endNodeId(),
                    relationships.typeId()
                  )
                ) &&
                predicateIsTrue(
                  row,
                  state,
                  tempNodeOffset,
                  nodePredicate,
                  VirtualValues.node(relationships.otherNodeId(fromNode))
                )
              ) {
                stackOfNodes.push(relationships.otherNodeId(fromNode))
                stackOfRelContainers.push(rels.append(VirtualValues.relationship(
                  relId,
                  relationships.startNodeId(),
                  relationships.endNodeId(),
                  relationships.typeId()
                )))
              }
            }
          }
        }
        val projectedRels =
          if (projectBackwards(dir, projectedDir)) {
            rels.reverse
          } else {
            rels
          }

        (fromNode, projectedRels)
      }

      override def innerHasNext: Boolean = !stackOfNodes.isEmpty

      override protected[this] def closeMore(): Unit = {
        stackOfNodes.close()
        stackOfRelContainers.close()
      }
    }
  }

  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    input.flatMap {
      inputRow =>
        val fromNode = getFromNodeFunction.applyAsLong(inputRow)
        if (entityIsNull(fromNode)) {
          ClosingIterator.empty
        } else {
          // Ensure that the start-node also adheres to the node predicate
          if (predicateIsTrue(inputRow, state, tempNodeOffset, nodePredicate, state.query.nodeById(fromNode))) {

            val paths: ClosingIterator[(LNode, RelationshipContainer)] = varLengthExpand(fromNode, state, inputRow)
            paths collect {
              case (toNode: LNode, rels: RelationshipContainer)
                if rels.size >= min && isToNodeValid(inputRow, toNode) =>
                val resultRow = SlottedRow(slots)
                resultRow.copyFrom(inputRow, argumentSize.nLongs, argumentSize.nReferences)
                if (shouldExpandAll) {
                  resultRow.setLongAt(toOffset, toNode)
                }
                resultRow.setRefAt(relOffset, rels.asList)
                resultRow
            }
          } else {
            ClosingIterator.empty
          }
        }
    }
  }

  private def isToNodeValid(row: CypherRow, node: LNode): Boolean =
    shouldExpandAll || getToNodeFunction.applyAsLong(row) == node
}

object VarLengthExpandSlottedPipe {

  def predicateIsTrue(
    row: ReadableRow,
    state: QueryState,
    tempOffset: Int,
    predicate: Expression,
    entity: AnyValue
  ): Boolean =
    tempOffset == VariablePredicates.NO_PREDICATE_OFFSET || {
      state.expressionVariables(tempOffset) = entity
      predicate(row, state) eq Values.TRUE
    }
}
