/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.compiler.planner.logical.steps.BestPlans
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters.asQueryGraph
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

trait QueryGraphSolver {
  def plan(queryGraph: QueryGraph, interestingOrder: InterestingOrder, context: LogicalPlanningContext): BestPlans
  def planPatternExpression(planArguments: Set[String], expr: PatternExpression, context: LogicalPlanningContext): LogicalPlan
  def planPatternComprehension(planArguments: Set[String], expr: PatternComprehension, context: LogicalPlanningContext): LogicalPlan
}

trait PatternExpressionSolving {

  self: QueryGraphSolver =>

  def planPatternExpression(planArguments: Set[String], expr: PatternExpression, context: LogicalPlanningContext): LogicalPlan = {
    val qg = asQueryGraph(expr, planArguments, context.innerVariableNamer)
    self.plan(qg, InterestingOrder.empty, context).result
  }

  def planPatternComprehension(planArguments: Set[String], expr: PatternComprehension, context: LogicalPlanningContext): LogicalPlan = {
    val qg = asQueryGraph(expr, planArguments, context.innerVariableNamer)
    self.plan(qg, InterestingOrder.empty, context).result
  }
}

