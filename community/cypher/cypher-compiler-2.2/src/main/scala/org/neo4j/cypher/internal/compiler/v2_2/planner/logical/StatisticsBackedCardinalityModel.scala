/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.compiler.v2_2.ast.IntegerLiteral
import org.neo4j.cypher.internal.compiler.v2_2.helpers.MapSupport._
import org.neo4j.cypher.internal.compiler.v2_2.planner._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics.{QueryGraphCardinalityInput, QueryGraphCardinalityModel}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.LogicalPlan

class StatisticsBackedCardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel)
  extends Metrics.CardinalityModel {
  def apply(plan: LogicalPlan, incomingCardinality: Cardinality): Cardinality =
    computeCardinality(plan.solved, QueryGraphCardinalityInput(Map.empty, incomingCardinality))

  private def computeCardinality(query: PlannerQuery, input0: QueryGraphCardinalityInput): Cardinality = {
    val output = query.fold(input0) {
      case (input, PlannerQuery(graph, horizon, _)) =>
        val QueryGraphCardinalityInput(newLabels, graphCardinality) = calculateCardinalityForQueryGraph(graph, input)
        val horizonCardinality = calculateCardinalityForQueryHorizon(graphCardinality, horizon)
        QueryGraphCardinalityInput(newLabels, horizonCardinality)
    }
    output.inboundCardinality
  }

  private def calculateCardinalityForQueryHorizon(in: Cardinality, horizon: QueryHorizon): Cardinality = horizon match {
    // Normal projection with LIMIT
    case RegularQueryProjection(_, QueryShuffle(_, None, Some(limit: IntegerLiteral))) =>
      Cardinality(Math.min(in.amount.toLong, limit.value))

    // Distinct
    case projection: AggregatingQueryProjection if projection.aggregationExpressions.isEmpty =>
      in * Selectivity(0.95)

    // Aggregates
    case _: AggregatingQueryProjection =>
      Cardinality(Math.sqrt(in.amount))

    // Unwind
    case _: UnwindProjection =>
      in * Multiplier(10)

    case _: RegularQueryProjection =>
      in
  }

  private def calculateCardinalityForQueryGraph(graph: QueryGraph, input: QueryGraphCardinalityInput): QueryGraphCardinalityInput = {
    val newLabels = input.labelInfo.fuse(graph.patternNodeLabels)(_ ++ _)
    val newCardinality = queryGraphCardinalityModel(graph, input)
    QueryGraphCardinalityInput(newLabels, newCardinality)
  }
}
