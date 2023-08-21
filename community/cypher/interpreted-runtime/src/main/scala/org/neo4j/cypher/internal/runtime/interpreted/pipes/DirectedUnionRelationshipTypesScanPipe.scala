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

import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingLongIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.RelationshipIterator
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.BaseRelationshipCursorIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DirectedUnionRelationshipTypesScanPipe.unionTypeIterator
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.TokenReadSession
import org.neo4j.internal.kernel.api.helpers.UnionRelationshipTypeIndexCursor.ascendingUnionRelationshipTypeIndexCursor
import org.neo4j.internal.kernel.api.helpers.UnionRelationshipTypeIndexCursor.descendingUnionRelationshipTypeIndexCursor
import org.neo4j.io.IOUtils

case class DirectedUnionRelationshipTypesScanPipe(
  ident: String,
  fromNode: String,
  types: Seq[LazyType],
  toNode: String,
  indexOrder: IndexOrder
)(
  val id: Id =
    Id.INVALID_ID
) extends Pipe {

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val query = state.query
    val relIterator = unionTypeIterator(state, types, indexOrder, state.relTypeTokenReadSession.get)
    val ctx = state.newRowWithArgument(rowFactory)
    PrimitiveLongHelper.map(
      relIterator,
      relationshipId => {
        val relationship = query.relationshipById(relationshipId)
        val startNode = query.nodeById(relIterator.startNodeId())
        val endNode = query.nodeById(relIterator.endNodeId())
        rowFactory.copyWith(ctx, ident, relationship, fromNode, startNode, toNode, endNode)
      }
    )
  }
}

object DirectedUnionRelationshipTypesScanPipe {

  def unionTypeIterator(
    state: QueryState,
    types: Seq[LazyType],
    indexOrder: IndexOrder,
    tokenReadSession: TokenReadSession
  ): ClosingLongIterator with RelationshipIterator = {
    val query = state.query
    val ids = types.map(l => l.getId(query)).filter(_ != LazyType.UNKNOWN).toArray
    if (ids.isEmpty) ClosingLongIterator.emptyClosingRelationshipIterator
    else {
      val cursors = ids.map(_ => {
        val c = query.relationshipTypeIndexCursor()
        query.resources.trace(c)
        c
      })
      val read = query.transactionalContext.dataRead
      val cursor = indexOrder match {
        case IndexOrderAscending | IndexOrderNone =>
          ascendingUnionRelationshipTypeIndexCursor(
            read,
            tokenReadSession,
            query.transactionalContext.cursorContext,
            ids,
            cursors
          )
        case IndexOrderDescending => descendingUnionRelationshipTypeIndexCursor(
            read,
            tokenReadSession,
            query.transactionalContext.cursorContext,
            ids,
            cursors
          )
      }

      new BaseRelationshipCursorIterator {

        override protected def fetchNext(): Long = {
          while (cursor.next()) {
            if (cursor.readFromStore()) {
              return cursor.reference()
            }
          }
          -1L
        }

        override def close(): Unit = IOUtils.closeAll(cursors: _*)

        /**
         * Store the current state in case the underlying cursor is closed when calling next.
         */
        override protected def storeState(): Unit = {
          relTypeId = cursor.`type`()
          source = cursor.sourceNodeReference()
          target = cursor.targetNodeReference()
        }
      }
    }
  }
}
