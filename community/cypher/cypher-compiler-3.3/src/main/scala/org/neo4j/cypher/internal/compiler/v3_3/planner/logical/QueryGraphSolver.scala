/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical

import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.frontend.v3_2.InternalException
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters.PatternExpressionPatternElementNamer
import org.neo4j.cypher.internal.ir.v3_2.{IdName, QueryGraph}

trait QueryGraphSolver {
  def plan(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext): LogicalPlan
  def planPatternExpression(planArguments: Set[IdName], expr: PatternExpression)
                           (implicit context: LogicalPlanningContext): (LogicalPlan, PatternExpression)
  def planPatternComprehension(planArguments: Set[IdName], expr: PatternComprehension)
                              (implicit context: LogicalPlanningContext): (LogicalPlan, PatternComprehension)
}

trait PatternExpressionSolving {

  self: QueryGraphSolver =>

  import org.neo4j.cypher.internal.ir.v3_2.helpers.ExpressionConverters._

  def planPatternExpression(planArguments: Set[IdName], expr: PatternExpression)
                           (implicit context: LogicalPlanningContext): (LogicalPlan, PatternExpression) = {
    val dependencies = expr.dependencies.map(IdName.fromVariable)
    val qgArguments = planArguments intersect dependencies
    val (namedExpr, namedMap) = PatternExpressionPatternElementNamer(expr)
    val qg = namedExpr.asQueryGraph.withArgumentIds(qgArguments)
    val plan = planQueryGraph(qg, namedMap)
    (plan, namedExpr)
  }

  def planPatternComprehension(planArguments: Set[IdName], expr: PatternComprehension)
                              (implicit context: LogicalPlanningContext): (LogicalPlan, PatternComprehension) = {
    val dependencies = expr.dependencies.map(IdName.fromVariable)
    val qgArguments = planArguments intersect dependencies
    val qg = expr.asQueryGraph.withArgumentIds(qgArguments).addPredicates(expr.predicate.toIndexedSeq:_*)
    val plan: LogicalPlan = planQueryGraph(qg, Map.empty)
    (plan, expr)
  }

  private def planQueryGraph(qg: QueryGraph, namedMap: Map[PatternElement, Variable])
                            (implicit context: LogicalPlanningContext): LogicalPlan = {
    val argLeafPlan = Some(context.logicalPlanProducer.planQueryArgumentRow(qg))
    val namedNodes = namedMap.collect { case (elem: NodePattern, identifier) => identifier }
    val namedRels = namedMap.collect { case (elem: RelationshipChain, identifier) => identifier }
    val patternPlanningContext = context.forExpressionPlanning(namedNodes, namedRels)
    self.plan(qg)(patternPlanningContext)
  }
}

trait TentativeQueryGraphSolver extends QueryGraphSolver with PatternExpressionSolving {
  def tryPlan(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext): Option[LogicalPlan]
  def plan(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext): LogicalPlan =
    tryPlan(queryGraph).getOrElse(throw new InternalException("Failed to create a plan for the given QueryGraph " + queryGraph))
}
