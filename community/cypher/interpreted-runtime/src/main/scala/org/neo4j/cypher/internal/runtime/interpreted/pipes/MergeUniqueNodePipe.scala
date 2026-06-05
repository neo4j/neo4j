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

import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.macros.TranslateExceptionMacros3
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.MergeUniqueNodePipe.cacheProperties
import org.neo4j.cypher.internal.runtime.interpreted.pipes.MergeUniqueNodePipe.computePredicates
import org.neo4j.cypher.internal.runtime.interpreted.pipes.MergeUniqueNodePipe.mergeUniqueNodeId
import org.neo4j.cypher.internal.runtime.makeValueNeoSafe
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InvalidSemanticsException.cannotMergeNodeNullProperty
import org.neo4j.internal.kernel.api.PropertyIndexQuery
import org.neo4j.values.storable.Values

case class MergeUniqueNodePipe(
  node: String,
  labelName: String,
  queryIndexId: Int,
  indexedProperties: Array[IndexedProperty],
  seekExpressions: Array[Expression],
  onMatchProperties: MergePropertySets,
  onCreateProperties: MergePropertySets
)(val id: Id = Id.INVALID_ID) extends Pipe {
  private[this] val properties = indexedProperties.map(_.propertyKeyToken.nameId.id)

  private[this] val cachedPropsKeys: Array[ASTCachedProperty.RuntimeKey] =
    indexedProperties.map(p => p.asCachedProperty(node).runtimeKey)
  private[this] val shouldCacheProperties: Boolean = indexedProperties.exists(_.shouldGetValue)

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val baseContext = state.newRowWithArgument(rowFactory)
    val predicates = computePredicates(labelName, baseContext, state, properties, seekExpressions)

    val newRow = rowFactory.copyWith(
      baseContext,
      node,
      state.query.nodeById(mergeUniqueNodeId(
        baseContext,
        state,
        queryIndexId,
        predicates,
        onMatchProperties,
        onCreateProperties
      ))
    )
    if (shouldCacheProperties) {
      cacheProperties(newRow, indexedProperties, cachedPropsKeys, predicates)
    }
    ClosingIterator.single(newRow)
  }
}

object MergeUniqueNodePipe {

  def cacheProperties(
    row: CypherRow,
    indexedProperties: Array[IndexedProperty],
    cachedPropsKeys: Array[ASTCachedProperty.RuntimeKey],
    predicates: Array[PropertyIndexQuery.ExactPredicate]
  ): Unit = {
    var i = 0
    while (i < indexedProperties.length) {
      val indexedProperty = indexedProperties(i)
      if (indexedProperty.shouldGetValue) {
        row.setCachedProperty(
          cachedPropsKeys(i),
          predicates(i).value()
        )
      }
      i += 1
    }
  }

  def mergeUniqueNodeId(
    row: CypherRow,
    state: QueryState,
    queryIndexId: Int,
    predicates: Array[PropertyIndexQuery.ExactPredicate],
    onMatchProperties: MergePropertySets,
    onCreateProperties: MergePropertySets
  ): Long = {
    val query = state.query
    val cursor = query.nodeValueIndexCursor()
    try {
      val index = state.queryIndexes(queryIndexId)
      val context = query.transactionalContext
      TranslateExceptionMacros3.translateException(
        context.token,
        context.dataWrite.uniqueNodeMerge(
          index,
          cursor,
          predicates,
          onMatchProperties.compute(row, state),
          onCreateProperties.compute(row, state),
          query
        )
      )
    } finally {
      cursor.close()
    }
  }

  def computePredicates(
    labelName: String,
    row: CypherRow,
    state: QueryState,
    properties: Array[Int],
    seekExpressions: Array[Expression]
  ): Array[PropertyIndexQuery.ExactPredicate] = {
    var i = 0
    val res = new Array[PropertyIndexQuery.ExactPredicate](properties.length)
    while (i < properties.length) {
      val value = makeValueNeoSafe(seekExpressions(i)(row, state))
      if (value == Values.NO_VALUE) {
        throw cannotMergeNodeNullProperty(state.query.propertyKeyName(properties(i)), s":$labelName")
      }
      res(i) = PropertyIndexQuery.exact(properties(i), value)
      i += 1
    }
    res
  }
}
