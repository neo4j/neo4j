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
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.PropertyAccess
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CostModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.SelectivityCalculator
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.CompositeExpressionSelectivityCalculator
import org.neo4j.cypher.internal.compiler.planner.logical.limit.LimitSelectivityConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProviderContext
import org.neo4j.cypher.internal.evaluator.SimpleInternalExpressionEvaluator
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.IntegerLiteral
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.functions.DeterministicFunction.isFunctionDeterministic
import org.neo4j.cypher.internal.frontend.phases.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.CypherException
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.helpers.MapSupport.PowerMap
import org.neo4j.values.storable.NumberValue

object Metrics {

  object QueryGraphSolverInput {

    def empty: QueryGraphSolverInput = QueryGraphSolverInput(Map.empty, Map.empty)
  }

  case class QueryGraphSolverInput(
    labelInfo: LabelInfo,
    relTypeInfo: RelTypeInfo,
    limitSelectivityConfig: LimitSelectivityConfig = LimitSelectivityConfig.default,
    activePlanner: PlannerType = PlannerType.Match
  ) {

    def withUpdatedLabelInfo(fromPlan: LogicalPlan, solveds: Solveds): QueryGraphSolverInput = {
      val newLabels = (labelInfo fuse solveds.get(fromPlan.id).asSinglePlannerQuery.lastLabelInfo)(_ ++ _)
      copy(labelInfo = newLabels)
    }

    def withFusedLabelInfo(newLabelInfo: LabelInfo): QueryGraphSolverInput =
      copy(labelInfo = labelInfo.fuse(newLabelInfo)(_ ++ _))

    def withFusedRelTypeInfo(newRelTypeInfo: RelTypeInfo): QueryGraphSolverInput =
      copy(relTypeInfo = relTypeInfo ++ newRelTypeInfo)

    def withLimitSelectivityConfig(cfg: LimitSelectivityConfig): QueryGraphSolverInput =
      copy(limitSelectivityConfig = cfg)

    def limitSelectivity: Selectivity = activePlanner match {
      case PlannerType.Match   => limitSelectivityConfig.forMatch
      case PlannerType.Horizon => limitSelectivityConfig.forHorizon
    }
  }

  trait CostModel {

    /**
     * This metric calculates how expensive executing a logical plan is.
     * (e.g. by looking at cardinality, expression selectivity and taking into account the effort
     * required to execute a step)
     *
     * @return the cost of the given plan
     */
    def costFor(
      plan: LogicalPlan,
      input: QueryGraphSolverInput,
      semanticTable: SemanticTable,
      cardinalities: Cardinalities,
      providedOrders: ProvidedOrders,
      propertyAccess: Set[PropertyAccess],
      statistics: GraphStatistics,
      monitor: CostModelMonitor
    ): Cost
  }

  trait CardinalityModel {

    /**
     * This metric estimates how many rows of data a query produces
     * (e.g. by asking the database for statistics)
     *
     * @param query        the query to estimate cardinality for
     * @param cardinalityModel subquery expressions need to recursively call into the top-level cardinality model, so we pass it down.
     * @return the cardinality of the query
     */
    def apply(
      query: PlannerQuery,
      labelInfo: LabelInfo,
      relTypeInfo: RelTypeInfo,
      semanticTable: SemanticTable,
      indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
      cardinalityModel: CardinalityModel
    ): Cardinality

    /**
     * Passes down this CardinalityModel for recursive calls.
     */
    final def apply(
      plannerQuery: PlannerQuery,
      labelInfo: LabelInfo,
      relTypeInfo: RelTypeInfo,
      semanticTable: SemanticTable,
      indexCompatiblePredicatesProviderContext: IndexCompatiblePredicatesProviderContext
    ): Cardinality = apply(
      plannerQuery,
      labelInfo,
      relTypeInfo,
      semanticTable,
      indexCompatiblePredicatesProviderContext,
      this
    )
  }

  trait QueryGraphCardinalityModel {

    /**
     * @param queryGraph       the query graph to estimate cardinality
     * @param cardinalityModel subquery expressions need to recursively call into the top-level cardinality model, so we pass it down.
     * @return the cardinality of the query graph
     */
    def apply(
      queryGraph: QueryGraph,
      labelInfo: LabelInfo,
      relTypeInfo: RelTypeInfo,
      semanticTable: SemanticTable,
      indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
      cardinalityModel: CardinalityModel
    ): Cardinality
  }

  trait SelectivityCalculator {

    /**
     * Calculate a selectivity for the given selections.
     *
     * @param cardinalityModel subquery expressions need to recursively call into the top-level cardinality model, so we pass it down.
     */
    def apply(
      selections: Selections,
      labelInfo: LabelInfo,
      relTypeInfo: RelTypeInfo,
      semanticTable: SemanticTable,
      indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
      cardinalityModel: CardinalityModel
    ): Selectivity
  }

  /**
   * A node can have multiple labels at the same time. If labelInfo("n") = Set(LabelName("Foo"), LabelName("Bar")), then n:Foo AND n:Bar.
   */
  type LabelInfo = Map[String, Set[LabelName]]
  val LabelInfo: Map.type = Map

  /**
   * A relationship can only have one type. If labelInfo("r") = RelTypeName("Foo"), then we are certain that r:Foo.
   * If a relationship is given with multiple OR'ed types, such as ()-[:T1|T2]-(), then none of those types should be present in this map.
   */
  type RelTypeInfo = Map[String, RelTypeName]
  val RelTypeInfo: Map.type = Map
}

trait ExpressionEvaluator {

  def hasParameters(expr: Expression): Boolean = expr.folder.findAllByClass[Expression].exists {
    case Parameter(_, _, _) => true
    case _                  => false
  }

  def isDeterministic(expr: Expression): Boolean = {
    expr.folder.findAllByClass[Expression].forall {
      case func: FunctionInvocation => isFunctionDeterministic(func.function)
      // for UDFs we don't know but the result might be non-deterministic
      case _: ResolvedFunctionInvocation => false
      case _                             => true
    }
  }

  def evaluateExpression(expr: Expression): Option[Any]

  /*
   * Returns the evaluated long value from the specified expression if the expression is stable and can be evaluated to a long.
   */
  def evaluateLongIfStable(expression: Expression): Option[Long] = {
    def isStable(expression: Expression): Boolean = {
      !hasParameters(expression) && isDeterministic(expression)
    }

    expression match {
      case literal: IntegerLiteral => Some(literal.value)
      case nonLiteral if isStable(nonLiteral) =>
        evaluateExpression(nonLiteral)
          .collect { case number: NumberValue => number.longValue() }
      case _ => None
    }
  }
}

/**
 * Wrapper around [[SimpleInternalExpressionEvaluator]] that catches exceptions and returns an Option.
 */
object simpleExpressionEvaluator extends ExpressionEvaluator {
  private val expressionEvaluator = new SimpleInternalExpressionEvaluator()

  override def evaluateExpression(expr: Expression): Option[Any] =
    try {
      Some(expressionEvaluator.evaluate(expr))
    } catch {
      case _: CypherException => None // Silently disregard expressions that cannot be evaluated in an empty context
    }
}

case class Metrics(cost: CostModel, cardinality: CardinalityModel)

trait MetricsFactory {

  def newCardinalityEstimator(
    queryGraphCardinalityModel: QueryGraphCardinalityModel,
    calculator: SelectivityCalculator,
    expressionEvaluator: ExpressionEvaluator
  ): CardinalityModel

  def newCostModel(executionModel: ExecutionModel): CostModel

  def newQueryGraphCardinalityModel(
    planContext: PlanContext,
    calculator: SelectivityCalculator,
    labelInference: Boolean
  ): QueryGraphCardinalityModel

  def newSelectivityCalculator(planContext: PlanContext): SelectivityCalculator =
    CompositeExpressionSelectivityCalculator(planContext)

  def newMetrics(
    planContext: PlanContext,
    expressionEvaluator: ExpressionEvaluator,
    executionModel: ExecutionModel,
    labelInference: Boolean = false
  ): Metrics = {
    val selectivityCalculator = newSelectivityCalculator(planContext)
    val queryGraphCardinalityModel = newQueryGraphCardinalityModel(planContext, selectivityCalculator, labelInference)
    val cardinality = newCardinalityEstimator(queryGraphCardinalityModel, selectivityCalculator, expressionEvaluator)
    Metrics(newCostModel(executionModel), cardinality)
  }
}
