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
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.RelTypeInfo
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProviderContext
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.UnionQuery
import org.neo4j.cypher.internal.util.Cardinality

import scala.collection.mutable

class CachedStatisticsBackedCardinalityModel(wrapped: StatisticsBackedCardinalityModel) extends CardinalityModel {

  type CardinalityModelInput =
    (PlannerQuery, LabelInfo, RelTypeInfo, SemanticTable, IndexCompatiblePredicatesProviderContext)

  final private val cache: mutable.HashMap[CardinalityModelInput, Cardinality] = mutable.HashMap.empty

  override def apply(
    plannerQuery: PlannerQuery,
    labelInfo: LabelInfo,
    relTypeInfo: RelTypeInfo,
    semanticTable: SemanticTable,
    indexCompatiblePredicatesProviderContext: IndexCompatiblePredicatesProviderContext,
    cardinalityModel: CardinalityModel
  ): Cardinality = {
    def cacheKey(query: PlannerQuery): CardinalityModelInput =
      (query, labelInfo, relTypeInfo, semanticTable, indexCompatiblePredicatesProviderContext)

    def singlePlannerQueryCardinality(singlePlannerQuery: SinglePlannerQuery): Cardinality =
      wrapped.singlePlannerQueryCardinality(
        singlePlannerQuery,
        labelInfo,
        relTypeInfo,
        semanticTable,
        indexCompatiblePredicatesProviderContext,
        cardinalityModel = cardinalityModel // Not part of the cache key
      )

    def cachedSinglePlannerQueryCardinality(singlePlannerQuery: SinglePlannerQuery): Cardinality =
      cache.getOrElseUpdate(cacheKey(singlePlannerQuery), singlePlannerQueryCardinality(singlePlannerQuery))

    // First, try to retrieve the cardinality from the cache
    cache.getOrElseUpdate(
      cacheKey(plannerQuery),
      // If it isn't in the cache, then check the type of the query
      plannerQuery match {
        // If it's a simple SinglePlannerQuery, our base case, we hand it over to the underlying cardinality model, and then store the result
        case singlePlannerQuery: SinglePlannerQuery =>
          singlePlannerQueryCardinality(singlePlannerQuery)

        /*
        If it's a UnionQuery however, which is defined recursively, then it's a bit more involved.
        The naive implementation would look like this:
        wrapped.combineUnion(
          unionQuery,
          apply(unionQuery.part, labelInfo, relTypeInfo, semanticTable, indexCompatiblePredicatesProviderContext),
          apply(unionQuery.query, labelInfo, relTypeInfo, semanticTable, indexCompatiblePredicatesProviderContext)
        )
        But note how the recursive call to apply is not in tail position, and so will stack overflow on a deep enough query.
        The solution here is to "peel off" layers of the union, storing them in a stack, until we reach either a cached cardinality or the final SinglePlannerQuery at the bottom.
        Once we have the base cardinality, we can pop union layers from the stack one by one and feed them to combineUnion, updating cardinality and populating the cache as we go along.
        When the stack is empty, we can return the final cardinality value.

        "Layers! Unions have layers, ogres have layers, you get it, we both have layers." – Shrek (2001)
         */
        case unionQuery: UnionQuery =>
          // the accumulator that we return at the end
          var cardinality: Cardinality = null
          // the stack of union layers that we peel off to get to a cached value / the base layer
          lazy val unions: mutable.Stack[(UnionQuery, Cardinality)] =
            mutable.Stack((unionQuery, cachedSinglePlannerQueryCardinality(unionQuery.rhs)))
          // pointer to the current layer
          var query: PlannerQuery = unionQuery.lhs
          // Phase 1: "peel off" layers of the union until we hit the SinglePlannerQuery at the bottom, or if one of the nested unions is already in the cache
          while (cardinality == null) {
            query match {
              case singlePlannerQuery: SinglePlannerQuery =>
                // We have reached the base layer, try to get the value from the cache or else calculate and store it
                cardinality = cachedSinglePlannerQueryCardinality(singlePlannerQuery)

              case nestedUnionQuery: UnionQuery =>
                // We haven't reached the base layer, first try to find the cardinality of the nested query in the cache
                cache.get(cacheKey(nestedUnionQuery)) match {
                  case Some(unionQueryCardinality) =>
                    // If it is in the cache, we don't need to dig deeper, we can move on to the next step
                    cardinality = unionQueryCardinality

                  case None =>
                    // If it isn't in the cache, then we move the pointer one layer deeper, and push the nexted union in the cache
                    query = nestedUnionQuery.lhs
                    unions.push((nestedUnionQuery, cachedSinglePlannerQueryCardinality(nestedUnionQuery.rhs)))
                }

              case other =>
                throw new IllegalArgumentException(s"Unexpected PlannerQuery: ${other.getClass.getName}")
            }
          }
          // Phase 2: repeatedly pop union layers from the stack
          unions.foreach {
            case (stackedUnionQuery, queryCardinality) =>
              // Update the cardinality accumulator
              cardinality = wrapped.combineUnion(stackedUnionQuery, cardinality, queryCardinality)
              // Populate the cache for future use
              cache.update(cacheKey(stackedUnionQuery), cardinality)
          }
          // We can return the final value
          cardinality

        case other =>
          throw new IllegalArgumentException(s"Unexpected PlannerQuery: ${other.getClass.getName}")
      }
    )
  }
}
