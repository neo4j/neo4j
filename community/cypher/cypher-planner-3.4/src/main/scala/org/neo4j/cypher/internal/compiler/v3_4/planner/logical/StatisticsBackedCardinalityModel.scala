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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical

import org.neo4j.cypher.internal.compiler.v3_4.helpers.MapSupport._
import org.neo4j.cypher.internal.compiler.v3_4.planner._
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.Metrics.{CardinalityModel, QueryGraphCardinalityModel, QueryGraphSolverInput}
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.ir.v3_4._
import org.neo4j.cypher.internal.planner.v3_4.spi.GraphStatistics
import org.neo4j.cypher.internal.util.v3_4.{Cardinality, Multiplier, Selectivity}
import org.neo4j.cypher.internal.v3_4.expressions.IntegerLiteral
import org.neo4j.values.storable.NumberValue

class StatisticsBackedCardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel, simpleExpressionEvaluator: ExpressionEvaluator) extends CardinalityModel {

  def apply(query: PlannerQuery, input0: QueryGraphSolverInput, semanticTable: SemanticTable): Cardinality = {
    val output = query.fold(input0) {
      case (input, RegularPlannerQuery(graph, horizon, _)) =>
        val QueryGraphSolverInput(newLabels, graphCardinality, lazyness) = calculateCardinalityForQueryGraph(graph, input, semanticTable)

        val horizonCardinality = calculateCardinalityForQueryHorizon(graphCardinality, horizon)
        QueryGraphSolverInput(newLabels, horizonCardinality, lazyness)
    }
    output.inboundCardinality
  }

  private def calculateCardinalityForQueryHorizon(in: Cardinality, horizon: QueryHorizon): Cardinality = horizon match {
    // Normal projection with LIMIT integer literal
    case RegularQueryProjection(_, QueryShuffle(_, _, Some(limit: IntegerLiteral))) =>
      Cardinality.min(in, limit.value.toDouble)

    // Normal projection with LIMIT
    case RegularQueryProjection(_, QueryShuffle(_, _, Some(limit))) =>
      val cannotEvaluateStableValue =
        simpleExpressionEvaluator.hasParameters(limit) ||
          simpleExpressionEvaluator.isNonDeterministic(limit)

      val limitCardinality =
        if (cannotEvaluateStableValue) GraphStatistics.DEFAULT_LIMIT_CARDINALITY
        else {
          val evaluatedValue: Option[Any] = simpleExpressionEvaluator.evaluateExpression(limit)

          if (evaluatedValue.isDefined && evaluatedValue.get.isInstanceOf[NumberValue])
            Cardinality(evaluatedValue.get.asInstanceOf[NumberValue].doubleValue())
          else GraphStatistics.DEFAULT_LIMIT_CARDINALITY
        }

      Cardinality.min(in, limitCardinality)

    // Distinct
    case projection: AggregatingQueryProjection if projection.aggregationExpressions.isEmpty =>
      in * Selectivity.of(0.95).get
    case projection: DistinctQueryProjection =>
      in * Selectivity.of(0.95).get

    // Aggregates
    case _: AggregatingQueryProjection =>
      // if input cardinality is < 1 the sqrt is bigger than the original value which makes no sense for aggregations
      Cardinality.min(in, Cardinality.sqrt(in))

    // Unwind
    case _: UnwindProjection =>
      in * Multiplier(10)

    // ProcedureCall
    case _: ProcedureCallProjection =>
      in * Multiplier(10) min 1.0 max 10000.0

    // Load CSV
    case _: LoadCSVProjection =>
      in

    case _: RegularQueryProjection | _: PassthroughAllHorizon =>
      in
  }

  private def calculateCardinalityForQueryGraph(graph: QueryGraph, input: QueryGraphSolverInput,
                                                semanticTable: SemanticTable) = {
    val newLabels = input.labelInfo.fuse(graph.patternNodeLabels)(_ ++ _)
    val newCardinality = queryGraphCardinalityModel(graph, input, semanticTable)
    QueryGraphSolverInput(newLabels, newCardinality, input.strictness)
  }
}
