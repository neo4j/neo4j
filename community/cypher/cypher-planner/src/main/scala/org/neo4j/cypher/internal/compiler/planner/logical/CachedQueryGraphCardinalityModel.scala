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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.RelTypeInfo
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProviderContext
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.util.Cardinality

import scala.collection.mutable

class CachedQueryGraphCardinalityModel(wrapped: QueryGraphCardinalityModel) extends QueryGraphCardinalityModel {

  type QueryGraphCardinalityModelInput =
    (QueryGraph, LabelInfo, RelTypeInfo, SemanticTable, IndexCompatiblePredicatesProviderContext)

  final private val cache: mutable.HashMap[QueryGraphCardinalityModelInput, Cardinality] = mutable.HashMap.empty

  override def apply(
    queryGraph: QueryGraph,
    previousLabelInfo: LabelInfo,
    relTypeInfo: RelTypeInfo,
    semanticTable: SemanticTable,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
    cardinalityModel: CardinalityModel
  ): Cardinality = {
    def cacheKey: QueryGraphCardinalityModelInput =
      (queryGraph, previousLabelInfo, relTypeInfo, semanticTable, indexPredicateProviderContext)

    def wrappedResult: Cardinality = wrapped(
      queryGraph,
      previousLabelInfo,
      relTypeInfo,
      semanticTable,
      indexPredicateProviderContext,
      cardinalityModel // Not part of the cache key
    )

    cache.getOrElseUpdate(cacheKey, wrappedResult)
  }
}
