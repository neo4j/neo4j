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
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.RelTypeInfo
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProviderContext
import org.neo4j.cypher.internal.ir.PlannerQueryPart
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.UnionQuery
import org.neo4j.cypher.internal.util.Cardinality

import scala.collection.mutable

class CachedStatisticsBackedCardinalityModel(wrapped: StatisticsBackedCardinalityModel) extends CardinalityModel {

  type CardinalityModelInput =
    (PlannerQueryPart, LabelInfo, RelTypeInfo, SemanticTable, IndexCompatiblePredicatesProviderContext)

  final private val cache: mutable.HashMap[CardinalityModelInput, Cardinality] = mutable.HashMap.empty

  private def cachedSinglePlannerQueryCardinality(
    singlePlannerQuery: SinglePlannerQuery,
    labelInfo: LabelInfo,
    relTypeInfo: RelTypeInfo,
    semanticTable: SemanticTable,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext
  ): Cardinality =
    cache.getOrElseUpdate(
      (singlePlannerQuery, labelInfo, relTypeInfo, semanticTable, indexPredicateProviderContext),
      wrapped.singlePlannerQueryCardinality(
        singlePlannerQuery,
        labelInfo,
        relTypeInfo,
        semanticTable,
        indexPredicateProviderContext
      )
    )

  private def unionQueryQueryCardinality(
    unionQuery: UnionQuery,
    labelInfo: LabelInfo,
    relTypeInfo: RelTypeInfo,
    semanticTable: SemanticTable,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext
  ): Cardinality = {
    var lhs = unionQuery.part
    var cardinality: Cardinality = null
    val rhsCardinality =
      cachedSinglePlannerQueryCardinality(
        unionQuery.query,
        labelInfo,
        relTypeInfo,
        semanticTable,
        indexPredicateProviderContext
      )
    val unions: mutable.Stack[(UnionQuery, Cardinality)] = mutable.Stack((unionQuery, rhsCardinality))
    while (cardinality == null) {
      lhs match {
        case singlePlannerQuery: SinglePlannerQuery =>
          cardinality = cachedSinglePlannerQueryCardinality(
            singlePlannerQuery,
            labelInfo,
            relTypeInfo,
            semanticTable,
            indexPredicateProviderContext
          )
        case nestedUnionQuery: UnionQuery =>
          cache.get((nestedUnionQuery, labelInfo, relTypeInfo, semanticTable, indexPredicateProviderContext)) match {
            case Some(nestedUnionQueryCardinality) =>
              cardinality = nestedUnionQueryCardinality
            case None =>
              lhs = nestedUnionQuery.part
              val rhsCardinality = cachedSinglePlannerQueryCardinality(
                nestedUnionQuery.query,
                labelInfo,
                relTypeInfo,
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
        cache.update(
          (unionQuery, labelInfo, relTypeInfo, semanticTable, indexPredicateProviderContext),
          unionCardinality
        )
        cardinality = unionCardinality
    }
    cardinality
  }

  override def apply(
    plannerQueryPart: PlannerQueryPart,
    labelInfo: LabelInfo,
    relTypeInfo: RelTypeInfo,
    semanticTable: SemanticTable,
    indexCompatiblePredicatesProviderContext: IndexCompatiblePredicatesProviderContext
  ): Cardinality =
    cache.getOrElseUpdate(
      (plannerQueryPart, labelInfo, relTypeInfo, semanticTable, indexCompatiblePredicatesProviderContext),
      plannerQueryPart match {
        case singlePlannerQuery: SinglePlannerQuery =>
          wrapped.singlePlannerQueryCardinality(
            singlePlannerQuery,
            labelInfo,
            relTypeInfo,
            semanticTable,
            indexCompatiblePredicatesProviderContext
          )
        case unionQuery: UnionQuery =>
          unionQueryQueryCardinality(
            unionQuery,
            labelInfo,
            relTypeInfo,
            semanticTable,
            indexCompatiblePredicatesProviderContext
          )
      }
    )
}
