/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.cypher.internal.compiler.v3_5.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.Metrics.{CardinalityModel, CostModel, QueryGraphCardinalityModel}
import org.opencypher.v9_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.ir.v3_5.{PlannerQuery, _}
import org.neo4j.cypher.internal.planner.v3_5.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.opencypher.v9_0.util.{Cardinality, Cost}
import org.neo4j.cypher.internal.v3_5.logical.plans.LogicalPlan
import org.opencypher.v9_0.expressions.{Expression, FunctionInvocation, LabelName, Parameter}
import org.opencypher.v9_0.expressions.functions.{Rand, Timestamp}

import scala.language.implicitConversions

object Metrics {

  import org.neo4j.cypher.internal.compiler.v3_5.helpers.MapSupport._

  object QueryGraphSolverInput {

    def empty = QueryGraphSolverInput(Map.empty, Cardinality(1), strictness = None)
  }

  case class QueryGraphSolverInput(labelInfo: LabelInfo, inboundCardinality: Cardinality,
                                   strictness: Option[StrictnessMode]) {

    def recurse(fromPlan: LogicalPlan, solveds: Solveds, cardinalities: Cardinalities): QueryGraphSolverInput = {
      val newCardinalityInput = cardinalities.get(fromPlan.id)
      val newLabels = (labelInfo fuse solveds.get(fromPlan.id).labelInfo) (_ ++ _)
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
  type CardinalityModel = (PlannerQuery, QueryGraphSolverInput, SemanticTable) => Cardinality

  type QueryGraphCardinalityModel = (QueryGraph, QueryGraphSolverInput, SemanticTable) => Cardinality

  type LabelInfo = Map[String, Set[LabelName]]
}

trait ExpressionEvaluator {

  def hasParameters(expr: Expression): Boolean = expr.inputs.exists {
    case (Parameter(_, _), _) => true
    case _ => false
  }

  def isNonDeterministic(expr: Expression): Boolean =
    expr.inputs.exists {
      case (func@FunctionInvocation(_, _, _, _), _) if func.function == Rand => true
      case (func@FunctionInvocation(_, _, _, _), _) if func.function == Timestamp => true
      case _ => false
    }

  def evaluateExpression(expr: Expression): Option[Any]
}

case class Metrics(cost: CostModel,
                   cardinality: CardinalityModel,
                   queryGraphCardinalityModel: QueryGraphCardinalityModel)

trait MetricsFactory {
  def newCardinalityEstimator(queryGraphCardinalityModel: QueryGraphCardinalityModel, expressionEvaluator: ExpressionEvaluator): CardinalityModel
  def newCostModel(config: CypherPlannerConfiguration): CostModel
  def newQueryGraphCardinalityModel(statistics: GraphStatistics): QueryGraphCardinalityModel

  def newMetrics(statistics: GraphStatistics,expressionEvaluator: ExpressionEvaluator, config: CypherPlannerConfiguration) = {
    val queryGraphCardinalityModel = newQueryGraphCardinalityModel(statistics)
    val cardinality = newCardinalityEstimator(queryGraphCardinalityModel, expressionEvaluator)
    Metrics(newCostModel(config), cardinality, queryGraphCardinalityModel)
  }
}


