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
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.util.attribution.Id

case class UndirectedRelationshipIndexSeekPipe(
  ident: String,
  startNode: String,
  endNode: String,
  relType: RelationshipTypeToken,
  properties: Array[IndexedProperty],
  queryIndexId: Int,
  valueExpr: QueryExpression[Expression],
  indexMode: IndexSeekMode,
  indexOrder: IndexOrder
)(val id: Id = Id.INVALID_ID) extends Pipe with EntityIndexSeeker with IndexPipeWithValues {

  override val propertyIds: Array[Int] = properties.map(_.propertyKeyToken.nameId.id)

  override val indexPropertyIndices: Array[Int] = properties.indices.filter(properties(_).shouldGetValue).toArray

  override val indexCachedProperties: Array[CachedProperty] =
    indexPropertyIndices.map(offset => properties(offset).asCachedProperty(ident))
  private val needsValues: Boolean = indexPropertyIndices.nonEmpty

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val index = state.queryIndexes(queryIndexId)
    val baseContext = state.newRowWithArgument(rowFactory)
    new UndirectedRelIndexIterator(
      startNode,
      endNode,
      state,
      baseContext,
      relationshipIndexSeek(state, index, needsValues, indexOrder, baseContext)
    )
  }
}
