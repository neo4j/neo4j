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

import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.{CardinalityModel, CostModel, QueryGraphCardinalityModel}
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.ExpressionSelectivityCalculator
import org.neo4j.cypher.internal.evaluator.SimpleInternalExpressionEvaluator
import org.neo4j.cypher.internal.ir.{PlannerQueryPart, QueryGraph, StrictnessMode}
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.logical.plans.{LogicalPlan, ResolvedFunctionInvocation}
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.expressions.functions.Rand
import org.neo4j.cypher.internal.v4_0.expressions.{Expression, FunctionInvocation, LabelName, Parameter}
import org.neo4j.cypher.internal.v4_0.util.{Cardinality, Cost, CypherException}

import scala.language.implicitConversions

object Metrics {

  import org.neo4j.cypher.internal.compiler.helpers.MapSupport._

  object QueryGraphSolverInput {

    def empty = QueryGraphSolverInput(Map.empty, Cardinality(1), strictness = None)
  }

  case class QueryGraphSolverInput(labelInfo: LabelInfo, inboundCardinality: Cardinality,
                                   strictness: Option[StrictnessMode]) {

    def recurse(fromPlan: LogicalPlan, solveds: Solveds, cardinalities: Cardinalities): QueryGraphSolverInput = {
      val newCardinalityInput = cardinalities.get(fromPlan.id)
      val newLabels = (labelInfo fuse solveds.get(fromPlan.id).asSinglePlannerQuery.labelInfo) (_ ++ _)
      copy(labelInfo = newLabels, inboundCardinality = newCardinalityInput, strictness = strictness)
    }

    def withPreferredStrictness(strictness: StrictnessMode): QueryGraphSolverInput = copy(strictness = Some(strictness))
  }

  // This metric calculates how expensive executing a logical plan is.
  // (e.g. by looking at cardinality, expression selectivity and taking into account the effort
  // required to execute a step)
  type CostModel = (LogicalPlan, QueryGraphSolverInput, Cardinalities) => Cost

  // This metric estimates how many rows of data a logical plan produces
  // (e.g. by asking the database for statistics)
  trait CardinalityModel {
    def apply(query: PlannerQueryPart, input: QueryGraphSolverInput, semanticTable: SemanticTable): Cardinality
  }

  trait QueryGraphCardinalityModel {
    def apply(queryGraph: QueryGraph, input: QueryGraphSolverInput, semanticTable: SemanticTable): Cardinality
    def expressionSelectivityCalculator: ExpressionSelectivityCalculator
  }

  type LabelInfo = Map[String, Set[LabelName]]
}

trait ExpressionEvaluator {

  def hasParameters(expr: Expression): Boolean = expr.inputs.exists {
    case (Parameter(_, _), _) => true
    case _ => false
  }

  def isDeterministic(expr: Expression): Boolean = expr.inputs.forall {
      case (func@FunctionInvocation(_, _, _, _), _) if func.function == Rand => false
      //for UDFs we don't know but the result might be non-deterministic
      case (_:ResolvedFunctionInvocation, _) => false
      case _ => true
    }

  def evaluateExpression(expr: Expression): Option[Any]
}

/**
  * Wrapper around [[SimpleInternalExpressionEvaluator]] that catches exceptions and returns an Option.
  */
object simpleExpressionEvaluator extends ExpressionEvaluator {
  private val expressionEvaluator = new SimpleInternalExpressionEvaluator()
  override def evaluateExpression(expr: Expression): Option[Any] = try {
    Some(expressionEvaluator.evaluate(expr))
  } catch {
    case _: CypherException => None // Silently disregard expressions that cannot be evaluated in an empty context
  }
}

case class Metrics(cost: CostModel,
                   cardinality: CardinalityModel,
                   queryGraphCardinalityModel: QueryGraphCardinalityModel)

trait MetricsFactory {
  def newCardinalityEstimator(queryGraphCardinalityModel: QueryGraphCardinalityModel, expressionEvaluator: ExpressionEvaluator): CardinalityModel
  def newCostModel(config: CypherPlannerConfiguration): CostModel
  def newQueryGraphCardinalityModel(statistics: GraphStatistics): QueryGraphCardinalityModel

  def newMetrics(statistics: GraphStatistics, expressionEvaluator: ExpressionEvaluator, config: CypherPlannerConfiguration) = {
    val queryGraphCardinalityModel = newQueryGraphCardinalityModel(statistics)
    val cardinality = newCardinalityEstimator(queryGraphCardinalityModel, expressionEvaluator)
    Metrics(newCostModel(config), cardinality, queryGraphCardinalityModel)
  }
}


