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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.PatternExpressionSolving
import org.neo4j.cypher.internal.compiler.planner.logical.QueryGraphSolver
import org.neo4j.cypher.internal.compiler.planner.logical.QueryPlannerKit
import org.neo4j.cypher.internal.compiler.planner.logical.SortPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.SortPlanner.SatisfiedForPlan
import org.neo4j.cypher.internal.compiler.planner.logical.SortPlanner.orderSatisfaction
import org.neo4j.cypher.internal.compiler.planner.logical.steps.planShortestPaths
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

import scala.annotation.tailrec

trait IDPQueryGraphSolverMonitor extends IDPSolverMonitor {
  def noIDPIterationFor(graph: QueryGraph, result: LogicalPlan): Unit
  def initTableFor(graph: QueryGraph): Unit
  def startIDPIterationFor(graph: QueryGraph): Unit
  def endIDPIterationFor(graph: QueryGraph, result: LogicalPlan): Unit
  def emptyComponentPlanned(graph: QueryGraph, plan: LogicalPlan): Unit
  def startConnectingComponents(graph: QueryGraph): Unit
  def endConnectingComponents(graph: QueryGraph, result: LogicalPlan): Unit
}

object IDPQueryGraphSolver {
  val VERBOSE: Boolean = java.lang.Boolean.getBoolean("pickBestPlan.VERBOSE")

  def composeGenerators[Solvable](
    queryGraph: QueryGraph,
    interestingOrder: InterestingOrder,
    kit: QueryPlannerKit,
    context: LogicalPlanningContext,
    generators: Seq[IDPSolverStep[Solvable, LogicalPlan, LogicalPlanningContext]],
  ): IDPSolverStep[Solvable, LogicalPlan, LogicalPlanningContext] = {
    val selectingGenerators = generators.map(_.map(plan => kit.select(plan, queryGraph)))
    val sortingGenerators = if (interestingOrder.isEmpty) Seq.empty else
      selectingGenerators.map(_.flatMap(plan => SortPlanner.maybeSortedPlan(plan, interestingOrder, context).filterNot(_ == plan)))
    val combinedGenerators = selectingGenerators ++ sortingGenerators
    combinedGenerators.foldLeft(IDPSolverStep.empty[Solvable, LogicalPlan, LogicalPlanningContext])(_ ++ _)
  }

  def extraRequirementForInterestingOrder(context: LogicalPlanningContext, interestingOrder: InterestingOrder): ExtraRequirement[LogicalPlan] = {
    if (interestingOrder.isEmpty) {
      ExtraRequirement.empty
    } else {
      new ExtraRequirement[LogicalPlan]() {
        override def fulfils(plan: LogicalPlan): Boolean = {
          val asSortedAsPossible = SatisfiedForPlan(plan)
          orderSatisfaction(interestingOrder, context, plan) match {
            case asSortedAsPossible() => true
            case _ => false
          }
        }
      }
    }

  }
}

/**
 * This planner is based on the paper
 *
 *   "Iterative Dynamic Programming: A New Class of Query Optimization Algorithms"
 *
 * written by Donald Kossmann and Konrad Stocker
 */
case class IDPQueryGraphSolver(singleComponentSolver: SingleComponentPlannerTrait,
                               componentConnector: JoinDisconnectedQueryGraphComponents,
                               monitor: IDPQueryGraphSolverMonitor) extends QueryGraphSolver with PatternExpressionSolving {

  override def plan(queryGraph: QueryGraph, interestingOrder: InterestingOrder, context: LogicalPlanningContext): LogicalPlan = {
    val kit = kitWithShortestPathSupport(context.config.toKit(interestingOrder, context), context)
    val components = queryGraph.connectedComponents
    val plans = if (components.isEmpty) planEmptyComponent(queryGraph, context, kit) else planComponents(components, interestingOrder, context, kit)

    monitor.startConnectingComponents(queryGraph)
    val result = componentConnector.connectComponentsAndSolveOptionalMatch(plans.toSet, queryGraph, interestingOrder, context, kit, singleComponentSolver)
    monitor.endConnectingComponents(queryGraph, result)
    result
  }

  private def kitWithShortestPathSupport(kit: QueryPlannerKit, context: LogicalPlanningContext) =
    kit.copy(select = (initialPlan: LogicalPlan, qg: QueryGraph) => selectShortestPath(kit, initialPlan, qg, context))

  private def selectShortestPath(kit: QueryPlannerKit, initialPlan: LogicalPlan, qg: QueryGraph, context: LogicalPlanningContext): LogicalPlan =
    qg.shortestPathPatterns.foldLeft(kit.select(initialPlan, qg)) {
      case (plan, sp) if sp.isFindableFrom(plan.availableSymbols) =>
        val shortestPath = planShortestPaths(plan, qg, sp, context)
        kit.select(shortestPath, qg)
      case (plan, _) => plan
    }

  private def planComponents(components: Seq[QueryGraph], interestingOrder: InterestingOrder, context: LogicalPlanningContext, kit: QueryPlannerKit): Seq[PlannedComponent] =
    components.map { qg =>
      PlannedComponent(qg, singleComponentSolver.planComponent(qg, context, kit, interestingOrder))
    }

  private def planEmptyComponent(queryGraph: QueryGraph, context: LogicalPlanningContext, kit: QueryPlannerKit): Seq[PlannedComponent] = {
    val plan = context.logicalPlanProducer.planQueryArgument(queryGraph, context)
    val result: LogicalPlan = kit.select(plan, queryGraph)
    monitor.emptyComponentPlanned(queryGraph, result)
    Seq(PlannedComponent(queryGraph, BestResults(result, None)))
  }
}

