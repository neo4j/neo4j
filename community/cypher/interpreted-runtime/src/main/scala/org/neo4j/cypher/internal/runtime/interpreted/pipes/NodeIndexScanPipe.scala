/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.logical.plans.{IndexOrder, IndexedProperty}
import org.neo4j.cypher.internal.v4_0.expressions.{CachedProperty, LabelToken}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

import scala.collection.Iterator

case class NodeIndexScanPipe(ident: String,
                             label: LabelToken,
                             properties: Seq[IndexedProperty],
                             queryIndexId: Int,
                             indexOrder: IndexOrder)
                            (val id: Id = Id.INVALID_ID) extends Pipe with IndexPipeWithValues {

  override val indexPropertyIndices: Array[Int] =
    properties.indices.filter(properties(_).shouldGetValue).toArray
  override val indexCachedProperties: Array[CachedProperty] =
    indexPropertyIndices.map(offset => properties(offset).asCachedProperty(ident))
  private val needsValues: Boolean = indexPropertyIndices.nonEmpty

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val baseContext = state.newExecutionContext(executionContextFactory)
    val cursor = state.query.indexScan(state.queryIndexes(queryIndexId), needsValues, indexOrder)
    new IndexIterator(state.query, baseContext, cursor)
  }
}
