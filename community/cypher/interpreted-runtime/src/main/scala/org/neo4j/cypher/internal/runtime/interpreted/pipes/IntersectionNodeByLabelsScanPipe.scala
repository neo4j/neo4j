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
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.PrimitiveCursorIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IntersectionNodeByLabelsScanPipe.intersectionIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel.UNKNOWN
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.TokenReadSession
import org.neo4j.internal.kernel.api.helpers.IntersectionNodeLabelIndexCursor.ascendingIntersectionNodeLabelIndexCursor
import org.neo4j.internal.kernel.api.helpers.IntersectionNodeLabelIndexCursor.descendingIntersectionNodeLabelIndexCursor
import org.neo4j.io.IOUtils
import org.neo4j.values.virtual.VirtualValues

case class IntersectionNodeByLabelsScanPipe(ident: String, labels: Seq[LazyLabel], indexOrder: IndexOrder)(val id: Id =
  Id.INVALID_ID)
    extends Pipe {

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val nodes = intersectionIterator(state.query, labels, indexOrder, state.nodeLabelTokenReadSession.get)
    val baseContext = state.newRowWithArgument(rowFactory)
    PrimitiveLongHelper.map(nodes, n => rowFactory.copyWith(baseContext, ident, VirtualValues.node(n)))
  }
}

object IntersectionNodeByLabelsScanPipe {

  def intersectionIterator(
    query: QueryContext,
    labels: Seq[LazyLabel],
    indexOrder: IndexOrder,
    tokenReadSession: TokenReadSession
  ): ClosingLongIterator = {
    val ids = labels.map(l => l.getId(query)).filter(_ != UNKNOWN).toArray
    if (ids.isEmpty || ids.length != labels.size) ClosingLongIterator.empty
    else if (ids.length == 1) {
      query.getNodesByLabel(tokenReadSession, ids.head, indexOrder)
    } else {
      val cursors = ids.map(_ => {
        val c = query.nodeLabelIndexCursor()
        query.resources.trace(c)
        c
      })
      val cursor = indexOrder match {
        case IndexOrderAscending | IndexOrderNone =>
          ascendingIntersectionNodeLabelIndexCursor(
            query.transactionalContext.dataRead,
            tokenReadSession,
            query.transactionalContext.cursorContext,
            ids,
            cursors
          )
        case IndexOrderDescending => descendingIntersectionNodeLabelIndexCursor(
            query.transactionalContext.dataRead,
            tokenReadSession,
            query.transactionalContext.cursorContext,
            ids,
            cursors
          )
      }

      new PrimitiveCursorIterator {
        override protected def fetchNext(): Long = if (cursor.next()) cursor.reference() else -1L

        override def close(): Unit = IOUtils.closeAll(cursors: _*)
      }
    }
  }
}
