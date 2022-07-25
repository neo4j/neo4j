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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProviderContext
import org.neo4j.cypher.internal.ir.PlannerQueryPart
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.UnionQuery
import org.neo4j.cypher.internal.util.Cardinality

import scala.collection.mutable

class CachedStatisticsBackedCardinalityModel(wrapped: StatisticsBackedCardinalityModel) extends CardinalityModel {

  type CacheKey =
    (PlannerQueryPart, Metrics.QueryGraphSolverInput, SemanticTable, IndexCompatiblePredicatesProviderContext)

  final private val cache: mutable.HashMap[CacheKey, Cardinality] = mutable.HashMap.empty

  private def cachedSinglePlannerQueryCardinality(
    singlePlannerQuery: SinglePlannerQuery,
    input: QueryGraphSolverInput,
    semanticTable: SemanticTable,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext
  ): Cardinality =
    cache.getOrElseUpdate(
      (singlePlannerQuery, input, semanticTable, indexPredicateProviderContext),
      wrapped.singlePlannerQueryCardinality(singlePlannerQuery, input, semanticTable, indexPredicateProviderContext)
    )

  private def unionQueryQueryCardinality(
    unionQuery: UnionQuery,
    input: QueryGraphSolverInput,
    semanticTable: SemanticTable,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext
  ): Cardinality = {
    var lhs = unionQuery.part
    var cardinality: Cardinality = null
    val rhsCardinality = cachedSinglePlannerQueryCardinality(unionQuery.query, input, semanticTable, indexPredicateProviderContext)
    val unions: mutable.Stack[(UnionQuery, Cardinality)] = mutable.Stack((unionQuery, rhsCardinality))
    while (cardinality == null) {
      lhs match {
        case singlePlannerQuery: SinglePlannerQuery =>
          cardinality = cachedSinglePlannerQueryCardinality(
            singlePlannerQuery,
            input,
            semanticTable,
            indexPredicateProviderContext
          )
        case nestedUnionQuery: UnionQuery =>
          cache.get((nestedUnionQuery, input, semanticTable, indexPredicateProviderContext)) match {
            case Some(nestedUnionQueryCardinality) =>
              cardinality = nestedUnionQueryCardinality
            case None =>
              lhs = nestedUnionQuery.part
              val rhsCardinality = cachedSinglePlannerQueryCardinality(
                nestedUnionQuery.query,
                input,
                semanticTable,
                indexPredicateProviderContext
              )
              unions.push((nestedUnionQuery, rhsCardinality))
          }
      }
    }
    unions.foreach {
      case (unionQuery, rhsCardinality) =>
        val unionCardinality = wrapped.combineUnion(unionQuery, cardinality, rhsCardinality)
        cache.update((unionQuery, input, semanticTable, indexPredicateProviderContext), unionCardinality)
        cardinality = unionCardinality
    }
    cardinality
  }

  override def apply(
    queryPart: PlannerQueryPart,
    input: QueryGraphSolverInput,
    semanticTable: SemanticTable,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext
  ): Cardinality =
    cache.getOrElseUpdate(
      (queryPart, input, semanticTable, indexPredicateProviderContext),
      queryPart match {
        case singlePlannerQuery: SinglePlannerQuery =>
          wrapped.singlePlannerQueryCardinality(singlePlannerQuery, input, semanticTable, indexPredicateProviderContext)
        case unionQuery: UnionQuery =>
          unionQueryQueryCardinality(unionQuery, input, semanticTable, indexPredicateProviderContext)
      }
    )
}
