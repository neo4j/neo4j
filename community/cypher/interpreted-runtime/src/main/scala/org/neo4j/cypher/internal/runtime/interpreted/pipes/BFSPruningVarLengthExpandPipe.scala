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

import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.Expand.ExpansionMode
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingLongIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.PrimitiveCursorIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.BFSPruningVarLengthExpandPipe.bfsIterator
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.internal.kernel.api.RelationshipTraversalEntities
import org.neo4j.internal.kernel.api.helpers.BFSPruningVarExpandCursor.allExpander
import org.neo4j.internal.kernel.api.helpers.BFSPruningVarExpandCursor.incomingExpander
import org.neo4j.internal.kernel.api.helpers.BFSPruningVarExpandCursor.outgoingExpander
import org.neo4j.io.IOUtils
import org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualValues

import java.util.function.LongPredicate
import java.util.function.Predicate

case class BFSPruningVarLengthExpandPipe(
  source: Pipe,
  fromName: String,
  toName: String,
  maybeDepthName: Option[String],
  types: RelationshipTypes,
  dir: SemanticDirection,
  includeStartNode: Boolean,
  max: Int,
  mode: ExpansionMode,
  filteringStep: TraversalPredicates = TraversalPredicates.NONE
)(val id: Id = Id.INVALID_ID) extends PipeWithSource(source) with Pipe {
  self =>

  private val emitDepth: Boolean = maybeDepthName.nonEmpty
  private val depthName: String = maybeDepthName.orNull

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    def expand(row: CypherRow, fromNode: VirtualNodeValue, toNodeId: Long): ClosingIterator[CypherRow] = {
      if (filteringStep.filterNode(row, state, fromNode)) {
        val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)

        val expand = bfsIterator(
          state.query,
          fromNode.id(),
          toNodeId,
          types,
          dir,
          includeStartNode,
          max,
          mode,
          filteringStep.asNodeIdPredicate(row, state),
          filteringStep.asRelCursorPredicate(row, state),
          memoryTracker
        )
        PrimitiveLongHelper.map(
          expand,
          endNode => {
            if (emitDepth) {
              rowFactory.copyWith(
                row,
                toName,
                VirtualValues.node(endNode),
                depthName,
                Values.intValue(expand.currentDepth)
              )
            } else {
              rowFactory.copyWith(row, toName, VirtualValues.node(endNode))
            }
          }
        )
      } else {
        ClosingIterator.empty
      }
    }

    input.flatMap { row =>
      row.getByName(fromName) match {
        case fromNode: VirtualNodeValue =>
          mode match {
            case Expand.ExpandAll => expand(row, fromNode, NO_SUCH_NODE)
            case Expand.ExpandInto =>
              row.getByName(toName) match {
                case toNode: VirtualNodeValue => expand(row, fromNode, toNode.id())
                case IsNoValue()              => ClosingIterator.empty
                case value =>
                  throw new InternalException(s"Expected to find a node at '$toName' but found $value instead")
              }
          }
        case IsNoValue() => ClosingIterator.empty
        case value =>
          throw new InternalException(s"Expected to find a node at '$fromName' but found $value instead")
      }
    }
  }

}

object BFSPruningVarLengthExpandPipe {

  trait ClosingLongIteratorWithDepth extends ClosingLongIterator {
    def currentDepth: Int
  }

  def bfsIterator(
    query: QueryContext,
    node: Long,
    to: Long,
    types: RelationshipTypes,
    dir: SemanticDirection,
    includeStartNode: Boolean,
    max: Int,
    mode: ExpansionMode,
    nodePredicate: LongPredicate,
    relPredicate: Predicate[RelationshipTraversalEntities],
    memoryTracker: MemoryTracker
  ): ClosingLongIteratorWithDepth = {
    val nodeCursor = query.nodeCursor()
    val traversalCursor = query.traversalCursor()

    val cursor = dir match {
      case SemanticDirection.OUTGOING =>
        outgoingExpander(
          node,
          types.types(query),
          includeStartNode,
          max,
          query.transactionalContext.dataRead,
          nodeCursor,
          traversalCursor,
          nodePredicate,
          relPredicate,
          if (mode == ExpandInto) to else NO_SUCH_NODE,
          memoryTracker
        )
      case SemanticDirection.INCOMING =>
        incomingExpander(
          node,
          types.types(query),
          includeStartNode,
          max,
          query.transactionalContext.dataRead,
          nodeCursor,
          traversalCursor,
          nodePredicate,
          relPredicate,
          if (mode == ExpandInto) to else NO_SUCH_NODE,
          memoryTracker
        )
      case SemanticDirection.BOTH =>
        allExpander(
          node,
          types.types(query),
          includeStartNode,
          max,
          query.transactionalContext.dataRead,
          nodeCursor,
          traversalCursor,
          nodePredicate,
          relPredicate,
          if (mode == ExpandInto) to else NO_SUCH_NODE,
          memoryTracker
        )
    }
    query.resources.trace(nodeCursor)
    query.resources.trace(traversalCursor)
    var _nextCurrentDepth: Int = -1
    var _currentDepth: Int = -1
    new PrimitiveCursorIterator with ClosingLongIteratorWithDepth {
      override protected def fetchNext(): Long = {
        _currentDepth = _nextCurrentDepth
        if (cursor.next()) {
          _nextCurrentDepth = cursor.currentDepth()
          cursor.endNode()
        } else {
          -1L
        }
      }
      override def currentDepth: Int = _currentDepth
      override def close(): Unit = IOUtils.closeAll(traversalCursor, nodeCursor, cursor)
    }
  }
}
