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

import org.eclipse.collections.impl.block.factory.primitive.LongPredicates
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.physicalplanning.VariablePredicates.NO_PREDICATE_OFFSET
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.BFSPruningVarLengthExpandPipe.bfsIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.runtime.slotted.pipes.BFSPruningVarLengthExpandSlottedPipe.createPredicates
import org.neo4j.cypher.internal.runtime.slotted.pipes.VarLengthExpandSlottedPipe.predicateIsTrue
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.function.Predicates
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor

import java.util.function.LongPredicate
import java.util.function.Predicate

case class BFSPruningVarLengthExpandSlottedPipe(
  source: Pipe,
  fromSlot: Slot,
  toOffset: Int,
  types: RelationshipTypes,
  dir: SemanticDirection,
  includeStartNode: Boolean,
  max: Int,
  slots: SlotConfiguration,
  tempNodeOffset: Int,
  tempRelationshipOffset: Int,
  nodePredicateExpression: Expression,
  relationshipPredicateExpression: Expression
)(val id: Id = Id.INVALID_ID) extends PipeWithSource(source) with Pipe {
  self =>

  private val getFromNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(fromSlot, throwOnTypeError = false)

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    input.flatMap {
      inputRow =>
        {
          val fromNode = getFromNodeFunction.applyAsLong(inputRow)
          if (entityIsNull(fromNode)) {
            ClosingIterator.empty
          } else {
            if (
              predicateIsTrue(inputRow, state, tempNodeOffset, nodePredicateExpression, state.query.nodeById(fromNode))
            ) {
              val (nodePredicate, relationshipPredicate) =
                createPredicates(
                  state,
                  inputRow,
                  tempNodeOffset,
                  tempRelationshipOffset,
                  nodePredicateExpression,
                  relationshipPredicateExpression
                )

              val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)
              val expand = bfsIterator(
                state.query,
                fromNode,
                types,
                dir,
                includeStartNode,
                max,
                nodePredicate,
                relationshipPredicate,
                memoryTracker
              )

              PrimitiveLongHelper.map(
                expand,
                endNode => {
                  val outputRow = SlottedRow(slots)
                  outputRow.copyAllFrom(inputRow)
                  outputRow.setLongAt(toOffset, endNode)
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

object BFSPruningVarLengthExpandSlottedPipe {

  def createPredicates(
    state: QueryState,
    row: ReadableRow,
    tempNodeOffset: Int,
    tempRelationshipOffset: Int,
    nodePredicate: Expression,
    relationshipPredicate: Expression
  ): (LongPredicate, Predicate[RelationshipTraversalCursor]) = {
    def toLongPredicate(f: Long => Boolean): LongPredicate = (value: Long) => f(value)
    def createNodePredicate =
      toLongPredicate(n => predicateIsTrue(row, state, tempNodeOffset, nodePredicate, state.query.nodeById(n)))

    def createRelationshipPredicate: Predicate[RelationshipTraversalCursor] =
      (t: RelationshipTraversalCursor) =>
        predicateIsTrue(
          row,
          state,
          tempRelationshipOffset,
          relationshipPredicate,
          state.query.relationshipById(
            t.relationshipReference(),
            t.sourceNodeReference(),
            t.targetNodeReference(),
            t.`type`()
          )
        )

    (tempNodeOffset, tempRelationshipOffset) match {
      case (NO_PREDICATE_OFFSET, NO_PREDICATE_OFFSET) =>
        (LongPredicates.alwaysTrue(), Predicates.alwaysTrue[RelationshipTraversalCursor]())
      case (NO_PREDICATE_OFFSET, _) =>
        (LongPredicates.alwaysTrue(), createRelationshipPredicate)
      case (_, NO_PREDICATE_OFFSET) =>
        (createNodePredicate, Predicates.alwaysTrue[RelationshipTraversalCursor]())
      case _ =>
        (createNodePredicate, createRelationshipPredicate)
    }
  }
}
