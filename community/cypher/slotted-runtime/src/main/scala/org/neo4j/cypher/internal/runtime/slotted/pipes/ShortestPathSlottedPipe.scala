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
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths.SameNodeMode
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.DirectionConverter.toGraphDb
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.True
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TraversalPredicates
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.helpers.traversal.BiDirectionalBFS
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.values.virtual.VirtualValues

import scala.jdk.CollectionConverters.IteratorHasAsScala

case class ShortestPathSlottedPipe(
  source: Pipe,
  sourceSlot: Slot,
  targetSlot: Slot,
  pathOffset: Int,
  relsOffset: Int,
  types: RelationshipTypes,
  dir: SemanticDirection,
  predicates: TraversalPredicates,
  pathPredicates: Seq[Predicate],
  returnOneShortestPathOnly: Boolean,
  sameNodeMode: SameNodeMode,
  allowZeroLength: Boolean,
  maxDepth: Option[Int],
  needOnlyOnePath: Boolean,
  slots: SlotConfiguration
)(val id: Id = Id.INVALID_ID) extends PipeWithSource(source) with Pipe {
  self =>

  private val getSourceNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(sourceSlot, throwOnTypeError = false)
  private val getTargetNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(targetSlot, throwOnTypeError = false)

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)

    val nodeCursor = state.query.nodeCursor()
    state.query.resources.trace(nodeCursor)
    val traversalCursor = state.query.traversalCursor()
    state.query.resources.trace(traversalCursor)

    // Create empty BiDirectionalBFS here and (re)set with source/target nodes and predicates for each row below.
    val biDirectionalBFS = BiDirectionalBFS.newEmptyBiDirectionalBFS(
      types.types(state.query),
      toGraphDb(dir),
      maxDepth.getOrElse(Int.MaxValue),
      returnOneShortestPathOnly,
      state.query.transactionalContext.dataRead,
      nodeCursor,
      traversalCursor,
      memoryTracker,
      needOnlyOnePath,
      allowZeroLength
    )
    val pathPredicate = pathPredicates.foldLeft(True(): commands.predicates.Predicate)(_.andWith(_))
    val output = input.flatMap {
      row =>
        {
          val sourceNode = getSourceNodeFunction.applyAsLong(row)
          val targetNode = getTargetNodeFunction.applyAsLong(row)
          if (
            sourceNode != StatementConstants.NO_SUCH_NODE &&
            targetNode != StatementConstants.NO_SUCH_NODE &&
            predicates.filterNode(row, state, state.query.nodeById(sourceNode)) &&
            predicates.filterNode(row, state, state.query.nodeById(targetNode))
          ) {
            if (sameNodeMode.shouldReturnEmptyResult(sourceNode, targetNode, allowZeroLength)) {
              ClosingIterator.empty
            } else {
              biDirectionalBFS.resetForNewRow(
                sourceNode,
                targetNode,
                predicates.asNodeIdPredicate(row, state),
                predicates.asRelCursorPredicate(row, state)
              )

              ClosingIterator.asClosingIterator {
                val shortestPaths = biDirectionalBFS.shortestPathIterator().asScala
                  .map { path =>
                    val rels = VirtualValues.list(path.relationshipIds().map(VirtualValues.relationship): _*)

                    val outputRow = SlottedRow(slots)
                    outputRow.copyAllFrom(row)
                    outputRow.setRefAt(pathOffset, path)
                    outputRow.setRefAt(relsOffset, rels)
                    outputRow
                  }
                  .filter(pathPredicate.isTrue(_, state))
                if (returnOneShortestPathOnly) {
                  shortestPaths.take(1)
                } else {
                  shortestPaths
                }
              }
            }
          } else {
            ClosingIterator.empty
          }
        }
    }
    output.closing(traversalCursor).closing(nodeCursor).closing(biDirectionalBFS)
  }
}
