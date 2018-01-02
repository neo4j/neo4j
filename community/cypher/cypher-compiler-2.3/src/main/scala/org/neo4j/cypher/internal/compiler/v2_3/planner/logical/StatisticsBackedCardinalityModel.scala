/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical

import org.neo4j.cypher.internal.compiler.v2_3.helpers.MapSupport._
import org.neo4j.cypher.internal.compiler.v2_3.planner._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Metrics.{CardinalityModel, QueryGraphCardinalityModel, QueryGraphSolverInput}
import org.neo4j.cypher.internal.compiler.v2_3.spi.GraphStatistics
import org.neo4j.cypher.internal.frontend.v2_3.SemanticTable
import org.neo4j.cypher.internal.frontend.v2_3.ast.IntegerLiteral

class StatisticsBackedCardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel) extends CardinalityModel {

  def apply(query: PlannerQuery, input0: QueryGraphSolverInput, semanticTable: SemanticTable): Cardinality = {
    val output = query.fold(input0) {
      case (input, PlannerQuery(graph, horizon, _)) =>
        val QueryGraphSolverInput(newLabels, graphCardinality, lazyness) = calculateCardinalityForQueryGraph(graph, input, semanticTable)

        val horizonCardinality = calculateCardinalityForQueryHorizon(graphCardinality, horizon)
        QueryGraphSolverInput(newLabels, horizonCardinality, lazyness)
    }
    output.inboundCardinality
  }

  private def calculateCardinalityForQueryHorizon(in: Cardinality, horizon: QueryHorizon): Cardinality = horizon match {
    // Normal projection with LIMIT
    case RegularQueryProjection(_, QueryShuffle(_, None, Some(limit: IntegerLiteral))) =>
      Cardinality.min(in, limit.value.toDouble)

    // Normal projection with LIMIT
    case RegularQueryProjection(_, QueryShuffle(_, None, Some(unknownLimit))) =>
      Cardinality.min(in, GraphStatistics.DEFAULT_LIMIT_CARDINALITY)

    // Distinct
    case projection: AggregatingQueryProjection if projection.aggregationExpressions.isEmpty =>
      in * Selectivity.of(0.95).get

    // Aggregates
    case _: AggregatingQueryProjection =>
      // if input cardinality is < 1 the sqrt is bigger than the original value which makes no sense for aggregations
      Cardinality.min(in, Cardinality.sqrt(in))

    // Unwind
    case _: UnwindProjection =>
      in * Multiplier(10)

    case _: RegularQueryProjection =>
      in
  }

  private def calculateCardinalityForQueryGraph(graph: QueryGraph, input: QueryGraphSolverInput,
                                                semanticTable: SemanticTable) = {
    val newLabels = input.labelInfo.fuse(graph.patternNodeLabels)(_ ++ _)
    val newCardinality = queryGraphCardinalityModel(graph, input, semanticTable)
    QueryGraphSolverInput(newLabels, newCardinality, input.strictness)
  }
}
