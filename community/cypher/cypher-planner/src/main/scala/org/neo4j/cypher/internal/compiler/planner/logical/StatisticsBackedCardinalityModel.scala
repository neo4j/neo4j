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
package org.neo4j.cypher.internal.compiler.planner.logical

import PlannerDefaults._
import org.neo4j.cypher.internal.compiler.helpers.MapSupport._
import org.neo4j.cypher.internal.compiler.planner._
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.{CardinalityModel, QueryGraphCardinalityModel, QueryGraphSolverInput}
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.{IndependenceCombiner, SelectivityCombiner}
import org.neo4j.cypher.internal.ir._
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.expressions.IntegerLiteral
import org.neo4j.cypher.internal.v4_0.util.{Cardinality, Multiplier}
import org.neo4j.values.storable.NumberValue

class StatisticsBackedCardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel, simpleExpressionEvaluator: ExpressionEvaluator) extends CardinalityModel {

  private val expressionSelectivityCalculator = queryGraphCardinalityModel.expressionSelectivityCalculator
  private val combiner: SelectivityCombiner = IndependenceCombiner

  def apply(queryPart: PlannerQueryPart, input0: QueryGraphSolverInput, semanticTable: SemanticTable): Cardinality = {
    queryPart match {
      case query: SinglePlannerQuery =>
        val output = query.fold(input0) {
          case (input, RegularSinglePlannerQuery(graph, _, horizon, _, _)) =>
            val QueryGraphSolverInput(newLabels, graphCardinality, laziness) = calculateCardinalityForQueryGraph(graph, input, semanticTable)

            val horizonCardinality = calculateCardinalityForQueryHorizon(graphCardinality, horizon, semanticTable)
            QueryGraphSolverInput(newLabels, horizonCardinality, laziness)
        }
        output.inboundCardinality
      case UnionQuery(part, query, distinct, _) =>
        val unionCardinality = apply(part, input0, semanticTable) + apply(query, input0, semanticTable)
        if (distinct) {
          unionCardinality * DEFAULT_DISTINCT_SELECTIVITY
        } else {
          unionCardinality
        }
    }
  }

  private def calculateCardinalityForQueryHorizon(in: Cardinality, horizon: QueryHorizon, semanticTable: SemanticTable): Cardinality = horizon match {
    // Normal projection with LIMIT integer literal
    case RegularQueryProjection(_, QueryPagination(_, Some(limit: IntegerLiteral)), where) =>
      val cardinalityBeforeSelection = Cardinality.min(in, limit.value.toDouble)
      horizonCardinalityWithSelections(cardinalityBeforeSelection, where, semanticTable)

    // Normal projection with LIMIT
    case RegularQueryProjection(_, QueryPagination(_, Some(limit)), where) =>
      val cannotEvaluateStableValue =
        simpleExpressionEvaluator.hasParameters(limit) ||
          !simpleExpressionEvaluator.isDeterministic(limit)

      val limitCardinality =
        if (cannotEvaluateStableValue) DEFAULT_LIMIT_CARDINALITY
        else {
          val evaluatedValue: Option[Any] = simpleExpressionEvaluator.evaluateExpression(limit)

          if (evaluatedValue.isDefined && evaluatedValue.get.isInstanceOf[NumberValue])
            Cardinality(evaluatedValue.get.asInstanceOf[NumberValue].doubleValue())
          else DEFAULT_LIMIT_CARDINALITY
        }

      val cardinalityBeforeSelection = Cardinality.min(in, limitCardinality)
      horizonCardinalityWithSelections(cardinalityBeforeSelection, where, semanticTable)

    case projection: RegularQueryProjection =>
      horizonCardinalityWithSelections(in, projection.selections, semanticTable)

    // Distinct
    case projection: AggregatingQueryProjection if projection.aggregationExpressions.isEmpty =>
      val cardinalityBeforeSelection = in * DEFAULT_DISTINCT_SELECTIVITY
      horizonCardinalityWithSelections(cardinalityBeforeSelection, projection.selections, semanticTable)
    case projection: DistinctQueryProjection =>
      val cardinalityBeforeSelection = in * DEFAULT_DISTINCT_SELECTIVITY
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

    case CallSubqueryHorizon(subquery) =>
      val subQueryCardinality = apply(subquery, QueryGraphSolverInput.empty, semanticTable)
      in * subQueryCardinality
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
    val newLabels = input.labelInfo.fuse(graph.patternNodeLabels)(_ ++ _)
    val newCardinality = queryGraphCardinalityModel(graph, input, semanticTable)
    QueryGraphSolverInput(newLabels, newCardinality, input.strictness)
  }
}
