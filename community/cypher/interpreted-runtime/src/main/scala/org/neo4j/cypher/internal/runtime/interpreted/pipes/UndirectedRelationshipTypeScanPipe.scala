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

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.RelationshipValue

case class UndirectedRelationshipTypeScanPipe(ident: String, fromNode: String, typ: LazyType, toNode: String)
                                             (val id: Id = Id.INVALID_ID) extends Pipe {

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val typeId = typ.getId(state.query)
    if (typeId == LazyType.UNKNOWN) ClosingIterator.empty
    else {
      new UndirectedIterator(ident, typeId, fromNode, toNode, rowFactory, state)
    }
  }
}

private class UndirectedIterator(relName: String,
                                 relToken: Int,
                                 fromNode: String,
                                 toNode: String,
                                 rowFactory: CypherRowFactory,
                                 state: QueryState) extends ClosingIterator[CypherRow] {

  private var emitSibling = false
  private var lastRelationship: RelationshipValue = _
  private var lastStart: NodeValue = _
  private var lastEnd: NodeValue = _

  private val baseContext = state.newRowWithArgument(rowFactory)
  private val query = state.query
  private val relIterator = query.getRelationshipsByType(state.relTypeTokenReadSession.get, relToken)

  def next(): CypherRow = {
    if (emitSibling) {
      emitSibling = false
      rowFactory.copyWith(baseContext, relName, lastRelationship, fromNode, lastEnd, toNode, lastStart)
    } else {
      emitSibling = true
      lastRelationship = query.relationshipById(relIterator.next())
      lastStart = lastRelationship.startNode()
      lastEnd = lastRelationship.endNode()
      rowFactory.copyWith(baseContext, relName, lastRelationship, fromNode, lastStart, toNode, lastEnd)
    }
  }

  override protected[this] def closeMore(): Unit = {
    relIterator.close()
  }
  override protected[this] def innerHasNext: Boolean = emitSibling || relIterator.hasNext
}
