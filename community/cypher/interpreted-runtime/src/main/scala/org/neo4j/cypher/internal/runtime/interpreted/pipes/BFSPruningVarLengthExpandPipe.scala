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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.eclipse.collections.impl.block.factory.primitive.LongPredicates
import org.neo4j.cypher.internal.expressions.SemanticDirection
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
import org.neo4j.function.Predicates
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.helpers.BFSPruningVarExpandCursor.incomingExpander
import org.neo4j.internal.kernel.api.helpers.BFSPruningVarExpandCursor.outgoingExpander
import org.neo4j.io.IOUtils
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualValues
import org.neo4j.values.virtual.VirtualValues.relationship

import java.util.function.LongPredicate
import java.util.function.Predicate

case class BFSPruningVarLengthExpandPipe(source: Pipe,
                                         fromName: String,
                                         toName: String,
                                         types: RelationshipTypes,
                                         dir: SemanticDirection,
                                         includeStartNode: Boolean,
                                         max: Int,
                                         filteringStep: VarLengthPredicate = VarLengthPredicate.NONE)
                                        (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) with Pipe {
  self =>

  override protected def internalCreateResults(input: ClosingIterator[CypherRow], state: QueryState): ClosingIterator[CypherRow] = {
    input.flatMap {
      row => {
        row.getByName(fromName) match {
          case node: VirtualNodeValue =>
            if (filteringStep.filterNode(row, state)(node)) {
              val (nodePredicate, relationshipPredicate) = createPredicates(state, node.id(), row)
              val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)
              val expand = bfsIterator(state.query, node.id(), types, dir, includeStartNode, max, nodePredicate, relationshipPredicate, memoryTracker)
              PrimitiveLongHelper.map(expand, endNode => {
                rowFactory.copyWith(row, toName, VirtualValues.node(endNode))
              })
            } else {
              ClosingIterator.empty
            }

          case IsNoValue() => ClosingIterator.empty
          case value => throw new InternalException(s"Expected to find a node at '$fromName' but found $value instead")
        }
      }
    }
  }

  private def createPredicates(state: QueryState,
                               node: Long,
                               row: CypherRow): (LongPredicate, Predicate[RelationshipTraversalCursor]) = {
    def toLongPredicate(f: Long => Boolean): LongPredicate = (value: Long) => f(value)
    filteringStep match {
      case VarLengthPredicate.NONE =>
        (if (includeStartNode) toLongPredicate(_ != node) else LongPredicates.alwaysTrue(), Predicates.alwaysTrue[RelationshipTraversalCursor]())
      case _ =>
        val nodePredicate = if (includeStartNode) {
          toLongPredicate(t => t != node && filteringStep.filterNode(row, state)(VirtualValues.node(t)))
        } else {
          toLongPredicate(t => filteringStep.filterNode(row, state)(VirtualValues.node(t)))
        }
        val relationshipPredicate = new Predicate[RelationshipTraversalCursor] {
          override def test(t: RelationshipTraversalCursor): Boolean = {
            filteringStep.filterRelationship(row, state)(relationship(t.relationshipReference(), t.originNodeReference(), t.targetNodeReference(), t.`type`()))
          }
        }

        (nodePredicate, relationshipPredicate)
    }
  }

}

object BFSPruningVarLengthExpandPipe {
  def bfsIterator(query: QueryContext,
                  node: Long,
                  types: RelationshipTypes,
                  dir: SemanticDirection,
                  includeStartNode: Boolean,
                  max: Int,
                  nodePredicate: LongPredicate,
                  relPredicate: Predicate[RelationshipTraversalCursor],
                  memoryTracker: MemoryTracker): ClosingLongIterator = {
    val nodeCursor = query.nodeCursor()
    val traversalCursor = query.traversalCursor()

    val cursor = dir match {
      case SemanticDirection.OUTGOING =>
        outgoingExpander(
          node,
          types.types(query),
          max,
          query.transactionalContext.dataRead,
          nodeCursor,
          traversalCursor,
          nodePredicate,
          relPredicate,
          memoryTracker)
      case SemanticDirection.INCOMING =>
        incomingExpander(
          node,
          types.types(query),
          max,
          query.transactionalContext.dataRead,
          nodeCursor,
          traversalCursor,
          nodePredicate,
          relPredicate,
          memoryTracker)
      case SemanticDirection.BOTH => throw new IllegalStateException("Undirected BFSPruningVarLength is not supported")
    }
    query.resources.trace(nodeCursor)
    query.resources.trace(traversalCursor)
    val iterator = new PrimitiveCursorIterator {
      override protected def fetchNext(): Long =  if (cursor.next()) cursor.otherNodeReference() else -1L
      override def close(): Unit = IOUtils.closeAll(nodeCursor, traversalCursor)
    }
    if (includeStartNode) ClosingLongIterator.prepend(node, iterator)
    else iterator
  }
}


