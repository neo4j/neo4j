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

import org.neo4j.cypher.internal.ir.{InterestingOrder, QueryGraph}
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.rewriting.rewriters.PatternExpressionPatternElementNamer
import org.neo4j.exceptions.InternalException

trait QueryGraphSolver {
  def plan(queryGraph: QueryGraph, interestingOrder: InterestingOrder, context: LogicalPlanningContext): LogicalPlan
  def planPatternExpression(planArguments: Set[String], expr: PatternExpression, context: LogicalPlanningContext): (LogicalPlan, PatternExpression)
  def planPatternComprehension(planArguments: Set[String], expr: PatternComprehension, context: LogicalPlanningContext): (LogicalPlan, PatternComprehension)
}

trait PatternExpressionSolving {

  self: QueryGraphSolver =>

  import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters._

  def planPatternExpression(planArguments: Set[String], expr: PatternExpression, context: LogicalPlanningContext): (LogicalPlan, PatternExpression) = {
    val dependencies = expr.dependencies.map(_.name)
    val qgArguments = planArguments intersect dependencies
    val (namedExpr, namedMap) = PatternExpressionPatternElementNamer(expr)
    val qg = asQueryGraph(namedExpr, context.innerVariableNamer).withArgumentIds(qgArguments)
    val plan = planQueryGraph(qg, namedMap, context)
    (plan, namedExpr)
  }

  def planPatternComprehension(planArguments: Set[String], expr: PatternComprehension, context: LogicalPlanningContext): (LogicalPlan, PatternComprehension) = {
    val queryGraph = asQueryGraph(expr, context.innerVariableNamer)
    val qgArguments = planArguments intersect queryGraph.idsWithoutOptionalMatchesOrUpdates
    val qg = queryGraph.withArgumentIds(qgArguments).addPredicates(expr.predicate.toIndexedSeq:_*)
    val plan: LogicalPlan = planQueryGraph(qg, Map.empty, context)
    (plan, expr)
  }

  private def planQueryGraph(qg: QueryGraph, namedMap: Map[PatternElement, Variable], context: LogicalPlanningContext) = {
    val namedNodes = namedMap.collect { case (_: NodePattern, identifier) => identifier }
    val namedRels = namedMap.collect { case (_: RelationshipChain, identifier) => identifier }
    val patternPlanningContext = context.forExpressionPlanning(namedNodes, namedRels)
    self.plan(qg, InterestingOrder.empty, patternPlanningContext)
  }
}

trait TentativeQueryGraphSolver extends QueryGraphSolver with PatternExpressionSolving {
  def tryPlan(queryGraph: QueryGraph, context: LogicalPlanningContext): Option[LogicalPlan]
  def plan(queryGraph: QueryGraph, context: LogicalPlanningContext): LogicalPlan =
    tryPlan(queryGraph, context).getOrElse(throw new InternalException("Failed to create a plan for the given QueryGraph " + queryGraph))
}
