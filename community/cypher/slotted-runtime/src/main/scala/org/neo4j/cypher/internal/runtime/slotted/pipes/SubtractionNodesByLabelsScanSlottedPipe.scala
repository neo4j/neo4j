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

import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingLongIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.PrimitiveCursorIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.pipes.SubtractionNodesByLabelsScanSlottedPipe.subtractionIterator
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.TokenReadSession
import org.neo4j.internal.kernel.api.helpers.SubtractionNodeLabelIndexCursor.ascendingSubtractionNodeLabelIndexCursor
import org.neo4j.internal.kernel.api.helpers.SubtractionNodeLabelIndexCursor.descendingSubtractionNodeLabelIndexCursor
import org.neo4j.io.IOUtils

case class SubtractionNodesByLabelsScanSlottedPipe(
                                                    nodeOffset: Int,
                                                    positiveLabel: LazyLabel,
                                                    negativeLabel: LazyLabel,
                                                    indexOrder: IndexOrder
)(val id: Id = Id.INVALID_ID) extends Pipe {

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    PrimitiveLongHelper.map(
      subtractionIterator(state.query, positiveLabel, negativeLabel, indexOrder, state.nodeLabelTokenReadSession.get),
      n => {
        val context = state.newRowWithArgument(rowFactory)
        context.setLongAt(nodeOffset, n)
        context
      }
    )
  }
}

object SubtractionNodesByLabelsScanSlottedPipe {
  def subtractionIterator(query: QueryContext, positiveLabel: LazyLabel,
                          negativeLabel: LazyLabel,
                          indexOrder: IndexOrder,
                          tokenReadSession: TokenReadSession
                       ): ClosingLongIterator = {
    val posToken = positiveLabel.getId(query)
    val negToken = negativeLabel.getId(query)
    val posCursor = query.nodeLabelIndexCursor()
    query.resources.trace(posCursor)
    val negCursor = query.nodeLabelIndexCursor()
    query.resources.trace(negCursor)
    val cursor = indexOrder match {
      case IndexOrderDescending =>
        descendingSubtractionNodeLabelIndexCursor(query.dataRead,
          tokenReadSession,
          query.transactionalContext.cursorContext,
          Array(posToken),
          Array(negToken),
          Array(posCursor),
          Array(negCursor)
        )
      case _ =>
        ascendingSubtractionNodeLabelIndexCursor(query.dataRead,
          tokenReadSession,
          query.transactionalContext.cursorContext,
          Array(posToken),
          Array(negToken),
          Array(posCursor),
          Array(negCursor)
        )
    }

    new PrimitiveCursorIterator {
      override protected def fetchNext(): Long = if (cursor.next()) cursor.reference() else -1L

      override def close(): Unit = IOUtils.closeAll(posCursor, negCursor)
    }

  }
}
