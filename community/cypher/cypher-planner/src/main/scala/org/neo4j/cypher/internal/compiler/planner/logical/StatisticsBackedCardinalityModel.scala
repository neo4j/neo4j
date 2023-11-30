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
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.SelectivityCalculator
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_DISTINCT_SELECTIVITY
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_LIMIT_ROW_COUNT
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_MULTIPLIER
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_SKIP_ROW_COUNT
import org.neo4j.cypher.internal.compiler.planner.logical.StatisticsBackedCardinalityModel.CardinalityAndInput
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProviderContext
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.IntegerLiteral
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.ir.AbstractProcedureCallProjection
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection
import org.neo4j.cypher.internal.ir.CallSubqueryHorizon
import org.neo4j.cypher.internal.ir.CommandProjection
import org.neo4j.cypher.internal.ir.DistinctQueryProjection
import org.neo4j.cypher.internal.ir.LoadCSVProjection
import org.neo4j.cypher.internal.ir.PassthroughAllHorizon
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryHorizon
import org.neo4j.cypher.internal.ir.QueryPagination
import org.neo4j.cypher.internal.ir.QueryProjection
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.UnionQuery
import org.neo4j.cypher.internal.ir.UnwindProjection
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Multiplier
import org.neo4j.cypher.internal.util.helpers.MapSupport.PowerMap

class StatisticsBackedCardinalityModel(
  queryGraphCardinalityModel: QueryGraphCardinalityModel,
  selectivityCalculator: SelectivityCalculator,
  simpleExpressionEvaluator: ExpressionEvaluator
) extends CardinalityModel {

  override def apply(
    query: PlannerQuery,
    labelInfo: LabelInfo,
    relTypeInfo: RelTypeInfo,
    semanticTable: SemanticTable,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
    cardinalityModel: CardinalityModel
  ): Cardinality = query match {
    case singlePlannerQuery: SinglePlannerQuery =>
      singlePlannerQueryCardinality(
        singlePlannerQuery,
        labelInfo,
        relTypeInfo,
        semanticTable,
        indexPredicateProviderContext,
        cardinalityModel
      )
    case uq @ UnionQuery(lhs, rhs, _, _) =>
      combineUnion(
        uq,
        apply(lhs, labelInfo, relTypeInfo, semanticTable, indexPredicateProviderContext, cardinalityModel),
        apply(rhs, labelInfo, relTypeInfo, semanticTable, indexPredicateProviderContext, cardinalityModel)
      )
  }

  def singlePlannerQueryCardinality(
    query: SinglePlannerQuery,
    labelInfo: LabelInfo,
    relTypeInfo: RelTypeInfo,
    semanticTable: SemanticTable,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
    cardinalityModel: CardinalityModel
  ): Cardinality = {
    val output = query.fold(CardinalityAndInput(Cardinality.SINGLE, labelInfo, relTypeInfo)) {
      case (CardinalityAndInput(inboundCardinality, labelInfo, relTypeInfo), plannerQuery) =>
        val CardinalityAndInput(qgCardinality, labelInfoAfterQG, relTypeInfoAfterQG) =
          calculateCardinalityForQueryGraph(
            plannerQuery.queryGraph,
            labelInfo,
            relTypeInfo,
            semanticTable,
            indexPredicateProviderContext,
            cardinalityModel
          )
        val beforeHorizonCardinality = qgCardinality * inboundCardinality
        val afterHorizon = calculateCardinalityForQueryHorizon(
          CardinalityAndInput(beforeHorizonCardinality, labelInfoAfterQG, relTypeInfoAfterQG),
          plannerQuery.horizon,
          semanticTable,
          indexPredicateProviderContext,
          cardinalityModel
        )
        afterHorizon.withFusedLabelInfo(plannerQuery.firstLabelInfo)
    }
    output.cardinality
  }

  def combineUnion(unionQuery: UnionQuery, lhsCardinality: Cardinality, rhsCardinality: Cardinality): Cardinality = {
    val unionCardinality = lhsCardinality + rhsCardinality
    if (unionQuery.distinct) {
      unionCardinality * DEFAULT_DISTINCT_SELECTIVITY
    } else {
      unionCardinality
    }
  }

  private def calculateCardinalityForQueryHorizon(
    cardinalityAndInput: CardinalityAndInput,
    horizon: QueryHorizon,
    semanticTable: SemanticTable,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
    cardinalityModel: CardinalityModel
  ): CardinalityAndInput = horizon match {
    case projection: QueryProjection =>
      val cardinalityBeforeSkip = queryProjectionCardinalityBeforeLimit(cardinalityAndInput.cardinality, projection)
      val cardinalityBeforeLimit = queryProjectionCardinalityWithSkip(cardinalityBeforeSkip, projection.queryPagination)
      val cardinalityBeforeSelection =
        queryProjectionCardinalityWithLimit(cardinalityBeforeLimit, projection.queryPagination)
      queryProjectionCardinalityWithSelections(
        CardinalityAndInput(cardinalityBeforeSelection, cardinalityAndInput.labelInfo, cardinalityAndInput.relTypeInfo),
        projection.selections,
        semanticTable,
        indexPredicateProviderContext,
        cardinalityModel
      )

    // Unwind
    case UnwindProjection(_, expression) =>
      val multiplier = expression match {
        case ListLiteral(expressions) => Multiplier(expressions.size)
        case FunctionInvocation(
            Namespace(Seq()),
            FunctionName("range"),
            _,
            Seq(from: IntegerLiteral, to: IntegerLiteral)
          ) =>
          val diff = to.value - from.value + 1
          Multiplier(Math.max(0, diff))
        case FunctionInvocation(
            Namespace(Seq()),
            FunctionName("range"),
            _,
            Seq(from: IntegerLiteral, to: IntegerLiteral, step: IntegerLiteral)
          ) =>
          val diff = to.value - from.value
          val steps = diff / step.value + 1
          Multiplier(Math.max(0, steps))
        case _ => DEFAULT_MULTIPLIER
      }
      cardinalityAndInput.copy(cardinality = cardinalityAndInput.cardinality * multiplier)

    // ProcedureCall
    case _: AbstractProcedureCallProjection =>
      cardinalityAndInput.copy(cardinality =
        Cardinality.max(cardinalityAndInput.cardinality * DEFAULT_MULTIPLIER, 1.0)
      ) // At least 1 row

    // Show command - e.g. ShowIndexesCommand
    case _: CommandProjection =>
      cardinalityAndInput.copy(cardinality =
        Cardinality.max(cardinalityAndInput.cardinality * DEFAULT_MULTIPLIER, 1.0)
      ) // At least 1 row

    // Load CSV
    case _: LoadCSVProjection =>
      cardinalityAndInput.copy(cardinality =
        Cardinality.max(cardinalityAndInput.cardinality * DEFAULT_MULTIPLIER, 1.0)
      ) // At least 1 row

    case _: PassthroughAllHorizon =>
      cardinalityAndInput

    case CallSubqueryHorizon(subquery, _, true, _) =>
      val subQueryCardinality = apply(
        subquery,
        cardinalityAndInput.labelInfo,
        cardinalityAndInput.relTypeInfo,
        semanticTable,
        indexPredicateProviderContext,
        cardinalityModel
      )
      // Cardinality of the subquery times current cardinality is the result
      cardinalityAndInput.copy(cardinality = cardinalityAndInput.cardinality * subQueryCardinality)

    case CallSubqueryHorizon(_, _, false, _) =>
      // Unit subquery call does not affect the driving table
      cardinalityAndInput
  }

  private def queryProjectionCardinalityBeforeLimit(in: Cardinality, projection: QueryProjection): Cardinality =
    projection match {
      case _: RegularQueryProjection =>
        in

      case _: DistinctQueryProjection =>
        in * DEFAULT_DISTINCT_SELECTIVITY
      case agg: AggregatingQueryProjection if agg.aggregationExpressions.isEmpty =>
        in * DEFAULT_DISTINCT_SELECTIVITY

      case agg: AggregatingQueryProjection =>
        StatisticsBackedCardinalityModel.aggregateCardinalityEstimation(in, agg.groupingExpressions)
    }

  private def queryProjectionCardinalityWithLimit(
    cardinalityBeforeLimit: Cardinality,
    queryPagination: QueryPagination
  ): Cardinality = {
    queryPagination.limit match {
      case None => cardinalityBeforeLimit
      case Some(limitExpression) =>
        val limitRowCount: Long =
          simpleExpressionEvaluator.evaluateLongIfStable(limitExpression).getOrElse(DEFAULT_LIMIT_ROW_COUNT)

        if (limitRowCount >= cardinalityBeforeLimit.amount) cardinalityBeforeLimit
        else Cardinality(limitRowCount)
    }
  }

  private def queryProjectionCardinalityWithSkip(
    cardinalityBeforeSkip: Cardinality,
    queryPagination: QueryPagination
  ): Cardinality = {
    queryPagination.skip match {
      case None => cardinalityBeforeSkip
      case Some(skipExpression) =>
        val skipRowCount: Long =
          simpleExpressionEvaluator.evaluateLongIfStable(skipExpression).getOrElse(DEFAULT_SKIP_ROW_COUNT)

        if (skipRowCount == 0) cardinalityBeforeSkip
        else if (skipRowCount >= cardinalityBeforeSkip.amount) Cardinality.EMPTY
        else cardinalityBeforeSkip.map(c => c - skipRowCount)

    }
  }

  private def queryProjectionCardinalityWithSelections(
    inputBeforeSelection: CardinalityAndInput,
    where: Selections,
    semanticTable: SemanticTable,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
    cardinalityModel: CardinalityModel
  ): CardinalityAndInput = {
    val inboundCardinality = inputBeforeSelection.cardinality
    val fusedInput = inputBeforeSelection.withFusedLabelInfo(where.labelInfo)
    val whereSelectivity = selectivityCalculator(
      where,
      fusedInput.labelInfo,
      fusedInput.relTypeInfo,
      semanticTable,
      indexPredicateProviderContext,
      cardinalityModel
    )
    val cardinality = inboundCardinality * whereSelectivity
    CardinalityAndInput(cardinality, fusedInput.labelInfo, fusedInput.relTypeInfo)
  }

  private def calculateCardinalityForQueryGraph(
    graph: QueryGraph,
    labelInfo: LabelInfo,
    relTypeInfo: RelTypeInfo,
    semanticTable: SemanticTable,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
    cardinalityModel: CardinalityModel
  ): CardinalityAndInput = {
    val fusedLabelInfo = labelInfo.fuse(graph.patternNodeLabels)(_ ++ _)
    val fusedRelTypeInfo = relTypeInfo ++ graph.patternRelationshipTypes
    val cardinality =
      queryGraphCardinalityModel(
        graph,
        fusedLabelInfo,
        fusedRelTypeInfo,
        semanticTable,
        indexPredicateProviderContext,
        cardinalityModel
      )
    CardinalityAndInput(cardinality, fusedLabelInfo, fusedRelTypeInfo)
  }
}

object StatisticsBackedCardinalityModel {

  case class CardinalityAndInput(cardinality: Cardinality, labelInfo: LabelInfo, relTypeInfo: RelTypeInfo) {

    def withFusedLabelInfo(newLabelInfo: LabelInfo): CardinalityAndInput =
      copy(labelInfo = labelInfo.fuse(newLabelInfo)(_ ++ _))

    def withFusedRelTypeInfo(newRelTypeInfo: RelTypeInfo): CardinalityAndInput =
      copy(relTypeInfo = relTypeInfo ++ newRelTypeInfo)
  }

  def aggregateCardinalityEstimation(in: Cardinality, groupingExpressions: Map[LogicalVariable, Expression]): Cardinality =
    if (groupingExpressions.isEmpty)
      Cardinality.SINGLE
    else
      // if input cardinality is < 1 the sqrt is bigger than the original value which makes no sense for aggregations
      Cardinality.min(in, Cardinality.sqrt(in))
}
