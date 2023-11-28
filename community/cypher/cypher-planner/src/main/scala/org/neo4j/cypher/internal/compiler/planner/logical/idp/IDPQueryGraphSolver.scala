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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.QueryGraphSolver
import org.neo4j.cypher.internal.compiler.planner.logical.QueryPlannerKit
import org.neo4j.cypher.internal.compiler.planner.logical.SortPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.SortPlanner.SatisfiedForPlan
import org.neo4j.cypher.internal.compiler.planner.logical.SortPlanner.orderSatisfaction
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.BestPlans
import org.neo4j.cypher.internal.compiler.planner.logical.steps.ExistsSubqueryPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.planShortestRelationships
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.ast.ExistsIRExpression
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

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

  def composeSolverSteps[Solvable](
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    kit: QueryPlannerKit,
    context: LogicalPlanningContext,
    generators: Seq[IDPSolverStep[Solvable, LogicalPlan, LogicalPlanningContext]]
  ): IDPSolverStep[Solvable, LogicalPlan, LogicalPlanningContext] = {
    val combinedSolverSteps =
      generators.map(selectingAndSortingSolverStep(queryGraph, interestingOrderConfig, kit, context, _))
    combinedSolverSteps.foldLeft(IDPSolverStep.empty[Solvable, LogicalPlan, LogicalPlanningContext])(_ ++ _)
  }

  def selectingAndSortingSolverStep[Solvable](
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    kit: QueryPlannerKit,
    context: LogicalPlanningContext,
    solverStep: IDPSolverStep[Solvable, LogicalPlan, LogicalPlanningContext]
  ): IDPSolverStep[Solvable, LogicalPlan, LogicalPlanningContext] = {
    val selectingSolverStep = solverStep.map(plan => kit.select(plan, queryGraph))
    if (interestingOrderConfig.orderToSolve.isEmpty) {
      selectingSolverStep
    } else {
      val sortingSolverStep = selectingSolverStep.flatMap(plan =>
        SortPlanner.maybeSortedPlan(
          plan,
          interestingOrderConfig,
          isPushDownSort = true,
          context,
          updateSolved = true
        ).filterNot(_ == plan)
      )
      selectingSolverStep ++ sortingSolverStep
    }
  }

  def extraRequirementForInterestingOrder(
    context: LogicalPlanningContext,
    interestingOrderConfig: InterestingOrderConfig
  ): ExtraRequirement[LogicalPlan] = {
    if (interestingOrderConfig.orderToSolve.isEmpty) {
      ExtraRequirement.empty
    } else {
      new ExtraRequirement[LogicalPlan]() {
        override def fulfils(plan: LogicalPlan): Boolean = {
          val asSortedAsPossible = SatisfiedForPlan(plan)
          orderSatisfaction(interestingOrderConfig, context, plan) match {
            case asSortedAsPossible() => true
            case _                    => false
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
case class IDPQueryGraphSolver(
  singleComponentSolver: SingleComponentPlannerTrait,
  componentConnector: JoinDisconnectedQueryGraphComponents,
  existsSubqueryPlanner: ExistsSubqueryPlanner
)(monitor: IDPQueryGraphSolverMonitor)
    extends QueryGraphSolver {

  override def plan(
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): BestPlans = {
    val kit = kitWithShortestPathSupport(context.plannerState.config.toKit(interestingOrderConfig, context), context)
    val components = queryGraph.connectedComponents
    val plannedComponents =
      if (components.isEmpty)
        planEmptyComponent(queryGraph, context, kit)
      else
        planComponents(components, interestingOrderConfig, context, kit)

    connectComponentsAndSolveOptionalMatch(plannedComponents, queryGraph, interestingOrderConfig, context, kit)
  }

  override def planInnerOfExistsSubquery(
    subquery: ExistsIRExpression,
    labelInfo: LabelInfo,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    existsSubqueryPlanner.planInnerOfExistsSubquery(subquery, labelInfo, context)
  }

  private def kitWithShortestPathSupport(kit: QueryPlannerKit, context: LogicalPlanningContext) =
    kit.copy(select = (initialPlan: LogicalPlan, qg: QueryGraph) => selectShortestPath(kit, initialPlan, qg, context))

  private def selectShortestPath(
    kit: QueryPlannerKit,
    initialPlan: LogicalPlan,
    qg: QueryGraph,
    context: LogicalPlanningContext
  ): LogicalPlan =
    qg.shortestRelationshipPatterns.foldLeft(kit.select(initialPlan, qg)) {
      case (plan, sp) if sp.isFindableFrom(plan.availableSymbols) =>
        val shortestPath = planShortestRelationships(plan, qg, sp, context)
        kit.select(shortestPath, qg)
      case (plan, _) => plan
    }

  private def planComponents(
    components: Seq[QueryGraph],
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext,
    kit: QueryPlannerKit
  ): Seq[PlannedComponent] =
    components.map { qg =>
      PlannedComponent(qg, singleComponentSolver.planComponent(qg, context, kit, interestingOrderConfig))
    }

  private def planEmptyComponent(
    queryGraph: QueryGraph,
    context: LogicalPlanningContext,
    kit: QueryPlannerKit
  ): Seq[PlannedComponent] = {
    val plan = context.staticComponents.logicalPlanProducer.planQueryArgument(queryGraph, context)
    val result: LogicalPlan = kit.select(plan, queryGraph)
    monitor.emptyComponentPlanned(queryGraph, result)
    Seq(PlannedComponent(queryGraph, BestResults(result, None)))
  }

  private def connectComponentsAndSolveOptionalMatch(
    plannedComponents: Seq[PlannedComponent],
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext,
    kit: QueryPlannerKit
  ): BestPlans = {
    monitor.startConnectingComponents(queryGraph)
    val bestPlans = componentConnector.connectComponentsAndSolveOptionalMatch(
      plannedComponents.toSet,
      queryGraph,
      interestingOrderConfig,
      context,
      kit,
      singleComponentSolver
    )
    monitor.endConnectingComponents(queryGraph, bestPlans.result)
    bestPlans
  }
}
