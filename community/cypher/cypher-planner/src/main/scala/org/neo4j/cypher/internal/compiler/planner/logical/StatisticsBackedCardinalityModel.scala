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

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.helpers.MapSupport.PowerMap
import org.neo4j.cypher.internal.compiler.planner.ProcedureCallProjection
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_DISTINCT_SELECTIVITY
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_LIMIT_CARDINALITY
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_MULTIPLIER
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.IndependenceCombiner
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.SelectivityCombiner
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.IntegerLiteral
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection
import org.neo4j.cypher.internal.ir.CallSubqueryHorizon
import org.neo4j.cypher.internal.ir.DistinctQueryProjection
import org.neo4j.cypher.internal.ir.LoadCSVProjection
import org.neo4j.cypher.internal.ir.PassthroughAllHorizon
import org.neo4j.cypher.internal.ir.PlannerQueryPart
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryHorizon
import org.neo4j.cypher.internal.ir.QueryPagination
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.UnionQuery
import org.neo4j.cypher.internal.ir.UnwindProjection
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Multiplier
import org.neo4j.values.storable.NumberValue

class StatisticsBackedCardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel, simpleExpressionEvaluator: ExpressionEvaluator) extends CardinalityModel {

  private val expressionSelectivityCalculator = queryGraphCardinalityModel.expressionSelectivityCalculator
  private val combiner: SelectivityCombiner = IndependenceCombiner

  def apply(queryPart: PlannerQueryPart, input0: QueryGraphSolverInput, semanticTable: SemanticTable): Cardinality = {
    queryPart match {
      case query: SinglePlannerQuery =>
        val output = query.fold(input0) {
          case (input, RegularSinglePlannerQuery(graph, _, horizon, _, _)) =>
            val newInput = calculateCardinalityForQueryGraph(graph, input, semanticTable)
            val horizonCardinality = calculateCardinalityForQueryHorizon(newInput.inboundCardinality, horizon, semanticTable)
            newInput.copy( inboundCardinality = horizonCardinality)
          case (input, v) => throw new IllegalStateException(s"cannot handle $input and $v")
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

    // Aggregates
    case projection: AggregatingQueryProjection =>
      val cardinalityBeforeSelection = StatisticsBackedCardinalityModel.aggregateCardinalityBeforeSelection(in, projection.groupingExpressions)
      horizonCardinalityWithSelections(cardinalityBeforeSelection, projection.selections, semanticTable)

    // Unwind
    case UnwindProjection(_, expression) =>
      val multiplier = expression match {
        case ListLiteral(expressions) => Multiplier(expressions.size)
        case FunctionInvocation(Namespace(Seq()), FunctionName("range"), _, Seq(from: IntegerLiteral, to: IntegerLiteral)) =>
          val diff = to.value - from.value + 1
          Multiplier(Math.max(0, diff))
        case FunctionInvocation(Namespace(Seq()), FunctionName("range"), _, Seq(from: IntegerLiteral, to: IntegerLiteral, step: IntegerLiteral)) =>
          val diff = to.value - from.value
          val steps = diff / step.value + 1
          Multiplier(Math.max(0, steps))
        case _ => DEFAULT_MULTIPLIER
      }
      in * multiplier

    // ProcedureCall
    case _: ProcedureCallProjection =>
      Cardinality.max(in * DEFAULT_MULTIPLIER, 1.0) // At least 1 row

    // Load CSV
    case _: LoadCSVProjection =>
      Cardinality.max(in * DEFAULT_MULTIPLIER, 1.0) // At least 1 row

    case _: PassthroughAllHorizon =>
      in

    case CallSubqueryHorizon(subquery, _) =>
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
    input.copy(labelInfo = input.labelInfo.fuse(graph.patternNodeLabels)(_ ++ _), inboundCardinality = queryGraphCardinalityModel(graph, input, semanticTable))
  }
}

object StatisticsBackedCardinalityModel {
  def aggregateCardinalityBeforeSelection(in: Cardinality, groupingExpressions: Map[String, Expression]): Cardinality =
    if (groupingExpressions.isEmpty)
      Cardinality.min(in, Cardinality.SINGLE)
    else
      // if input cardinality is < 1 the sqrt is bigger than the original value which makes no sense for aggregations
      Cardinality.min(in, Cardinality.sqrt(in))
}
