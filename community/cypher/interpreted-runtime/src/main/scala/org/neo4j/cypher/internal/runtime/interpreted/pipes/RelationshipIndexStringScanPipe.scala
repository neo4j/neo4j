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

import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor
import org.neo4j.values.storable.TextValue

abstract class AbstractRelationshipIndexStringScanPipe(
  ident: String,
  startNode: String,
  endNode: String,
  property: IndexedProperty,
  queryIndexId: Int,
  valueExpr: Expression
) extends Pipe with IndexPipeWithValues {

  override val indexPropertyIndices: Array[Int] = if (property.shouldGetValue) Array(0) else Array.empty
  override val indexCachedProperties: Array[CachedProperty] = Array(property.asCachedProperty(ident))
  protected val needsValues: Boolean = indexPropertyIndices.nonEmpty

  override protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val baseContext = state.newRowWithArgument(rowFactory)
    val value = valueExpr(baseContext, state)

    val resultNodes = value match {
      case value: TextValue =>
        iterator(
          state,
          startNode,
          endNode,
          baseContext,
          queryContextCall(state, state.queryIndexes(queryIndexId), value)
        )
      case _ => ClosingIterator.empty
    }

    resultNodes
  }

  protected def queryContextCall(
    state: QueryState,
    index: IndexReadSession,
    value: TextValue
  ): RelationshipValueIndexCursor

  protected def iterator(
    state: QueryState,
    startNode: String,
    endNode: String,
    baseContext: CypherRow,
    cursor: RelationshipValueIndexCursor
  ): IndexIteratorBase[CypherRow]
}

trait Directed {
  self: AbstractRelationshipIndexStringScanPipe =>

  override protected def iterator(
    state: QueryState,
    startNode: String,
    endNode: String,
    baseContext: CypherRow,
    cursor: RelationshipValueIndexCursor
  ): IndexIteratorBase[CypherRow] =
    new RelIndexIterator(state, startNode, endNode, baseContext, cursor)
}

trait Undirected {
  self: AbstractRelationshipIndexStringScanPipe =>

  override protected def iterator(
    state: QueryState,
    startNode: String,
    endNode: String,
    baseContext: CypherRow,
    cursor: RelationshipValueIndexCursor
  ): IndexIteratorBase[CypherRow] =
    new UndirectedRelIndexIterator(startNode, endNode, state, baseContext, cursor)
}

case class DirectedRelationshipIndexContainsScanPipe(
  ident: String,
  startNode: String,
  endNode: String,
  typeToken: RelationshipTypeToken,
  property: IndexedProperty,
  queryIndexId: Int,
  valueExpr: Expression,
  indexOrder: IndexOrder
)(val id: Id = Id.INVALID_ID)
    extends AbstractRelationshipIndexStringScanPipe(ident, startNode, endNode, property, queryIndexId, valueExpr)
    with Directed {

  override protected def queryContextCall(
    state: QueryState,
    index: IndexReadSession,
    value: TextValue
  ): RelationshipValueIndexCursor =
    state.query.relationshipIndexSeekByContains(index, needsValues, indexOrder, value)
}

case class UndirectedRelationshipIndexContainsScanPipe(
  ident: String,
  startNode: String,
  endNode: String,
  typeToken: RelationshipTypeToken,
  property: IndexedProperty,
  queryIndexId: Int,
  valueExpr: Expression,
  indexOrder: IndexOrder
)(val id: Id = Id.INVALID_ID)
    extends AbstractRelationshipIndexStringScanPipe(ident, startNode, endNode, property, queryIndexId, valueExpr)
    with Undirected {

  override protected def queryContextCall(
    state: QueryState,
    index: IndexReadSession,
    value: TextValue
  ): RelationshipValueIndexCursor =
    state.query.relationshipIndexSeekByContains(index, needsValues, indexOrder, value)
}

case class DirectedRelationshipIndexEndsWithScanPipe(
  ident: String,
  startNode: String,
  endNode: String,
  typeToken: RelationshipTypeToken,
  property: IndexedProperty,
  queryIndexId: Int,
  valueExpr: Expression,
  indexOrder: IndexOrder
)(val id: Id = Id.INVALID_ID)
    extends AbstractRelationshipIndexStringScanPipe(ident, startNode, endNode, property, queryIndexId, valueExpr)
    with Directed {

  override protected def queryContextCall(
    state: QueryState,
    index: IndexReadSession,
    value: TextValue
  ): RelationshipValueIndexCursor =
    state.query.relationshipIndexSeekByEndsWith(index, needsValues, indexOrder, value)
}

case class UndirectedRelationshipIndexEndsWithScanPipe(
  ident: String,
  startNode: String,
  endNode: String,
  typeToken: RelationshipTypeToken,
  property: IndexedProperty,
  queryIndexId: Int,
  valueExpr: Expression,
  indexOrder: IndexOrder
)(val id: Id = Id.INVALID_ID)
    extends AbstractRelationshipIndexStringScanPipe(ident, startNode, endNode, property, queryIndexId, valueExpr)
    with Undirected {

  override protected def queryContextCall(
    state: QueryState,
    index: IndexReadSession,
    value: TextValue
  ): RelationshipValueIndexCursor =
    state.query.relationshipIndexSeekByEndsWith(index, needsValues, indexOrder, value)
}
