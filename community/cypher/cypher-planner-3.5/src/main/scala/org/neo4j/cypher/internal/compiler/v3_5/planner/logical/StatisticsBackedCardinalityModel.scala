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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical

import org.neo4j.cypher.internal.compiler.v3_5.helpers.MapSupport._
import org.neo4j.cypher.internal.compiler.v3_5.planner._
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.Metrics.{CardinalityModel, QueryGraphCardinalityModel, QueryGraphSolverInput}
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.cardinality.{IndependenceCombiner, SelectivityCombiner}
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.planner.v3_5.spi.GraphStatistics
import org.neo4j.values.storable.NumberValue
import org.neo4j.cypher.internal.v3_5.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v3_5.expressions.IntegerLiteral
import org.neo4j.cypher.internal.v3_5.util.{Cardinality, Multiplier, Selectivity}

class StatisticsBackedCardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel, simpleExpressionEvaluator: ExpressionEvaluator) extends CardinalityModel {

  private val expressionSelectivityCalculator = queryGraphCardinalityModel.expressionSelectivityCalculator
  private val combiner: SelectivityCombiner = IndependenceCombiner

  def apply(query: PlannerQuery, input0: QueryGraphSolverInput, semanticTable: SemanticTable): Cardinality = {
    val output = query.fold(input0) {
      case (input, RegularPlannerQuery(graph, _, horizon, _)) =>
        val newInput = calculateCardinalityForQueryGraph(graph, input, semanticTable)

        val horizonCardinality = calculateCardinalityForQueryHorizon(newInput.inboundCardinality, horizon, semanticTable)
        newInput.copy( inboundCardinality = horizonCardinality)
    }
    output.inboundCardinality
  }

  private def calculateCardinalityForQueryHorizon(in: Cardinality, horizon: QueryHorizon, semanticTable: SemanticTable): Cardinality = horizon match {
    // Normal projection with LIMIT integer literal
    case RegularQueryProjection(_, QueryShuffle(_, _, Some(limit: IntegerLiteral)), where) =>
      val cardinalityBeforeSelection = Cardinality.min(in, limit.value.toDouble)
      horizonCardinalityWithSelections(cardinalityBeforeSelection, where, semanticTable)

    // Normal projection with LIMIT
    case RegularQueryProjection(_, QueryShuffle(_, _, Some(limit)), where) =>
      val cannotEvaluateStableValue =
        simpleExpressionEvaluator.hasParameters(limit) ||
          !simpleExpressionEvaluator.isDeterministic(limit)

      val limitCardinality =
        if (cannotEvaluateStableValue) GraphStatistics.DEFAULT_LIMIT_CARDINALITY
        else {
          val evaluatedValue: Option[Any] = simpleExpressionEvaluator.evaluateExpression(limit)

          if (evaluatedValue.isDefined && evaluatedValue.get.isInstanceOf[NumberValue])
            Cardinality(evaluatedValue.get.asInstanceOf[NumberValue].doubleValue())
          else GraphStatistics.DEFAULT_LIMIT_CARDINALITY
        }

      val cardinalityBeforeSelection = Cardinality.min(in, limitCardinality)
      horizonCardinalityWithSelections(cardinalityBeforeSelection, where, semanticTable)

    case projection: RegularQueryProjection =>
      horizonCardinalityWithSelections(in, projection.selections, semanticTable)

    // Distinct
    case projection: AggregatingQueryProjection if projection.aggregationExpressions.isEmpty =>
      val cardinalityBeforeSelection = in * GraphStatistics.DEFAULT_DISTINCT_SELECTIVITY
      horizonCardinalityWithSelections(cardinalityBeforeSelection, projection.selections, semanticTable)
    case projection: DistinctQueryProjection =>
      val cardinalityBeforeSelection = in * GraphStatistics.DEFAULT_DISTINCT_SELECTIVITY
      horizonCardinalityWithSelections(cardinalityBeforeSelection, projection.selections, semanticTable)

    // Aggregates with no grouping
    case projection: AggregatingQueryProjection if projection.groupingExpressions.isEmpty =>
      val cardinalityBeforeSelection = Cardinality.min(in, Cardinality.SINGLE)
      horizonCardinalityWithSelections(cardinalityBeforeSelection, projection.selections, semanticTable)

    // Aggregates
    case projection: AggregatingQueryProjection =>
      // if input cardinality is < 1 the sqrt is bigger than the original value which makes no sense for aggregations
      val cardinalityBeforeSelection = Cardinality.min(in, Cardinality.sqrt(in))
      horizonCardinalityWithSelections(cardinalityBeforeSelection, projection.selections, semanticTable)

    // Unwind
    case _: UnwindProjection =>
      in * Multiplier(10)

    // ProcedureCall
    case _: ProcedureCallProjection =>
      in * Multiplier(10) min 1.0 max 10000.0

    // Load CSV
    case _: LoadCSVProjection =>
      in

    case _: PassthroughAllHorizon =>
      in
  }

  private def horizonCardinalityWithSelections(cardinalityBeforeSelection: Cardinality,
                                               where: Selections,
                                               semanticTable: SemanticTable): Cardinality = {
    implicit val selections: Selections = where
    implicit val implicitSemanticTable: SemanticTable = semanticTable
    val expressionSelectivities = selections.flatPredicates.map(expressionSelectivityCalculator(_))
    val maybeWhereSelectivity = combiner.andTogetherSelectivities(expressionSelectivities)
    maybeWhereSelectivity match {
      case Some(whereSelectivity) => cardinalityBeforeSelection * whereSelectivity
      case None => cardinalityBeforeSelection
    }
  }

  private def calculateCardinalityForQueryGraph(graph: QueryGraph, input: QueryGraphSolverInput,
                                                semanticTable: SemanticTable) = {
    input.copy(labelInfo = input.labelInfo.fuse(graph.patternNodeLabels)(_ ++ _), inboundCardinality = queryGraphCardinalityModel(graph, input, semanticTable))
  }
}
