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
import org.neo4j.cypher.internal.compiler.planner.logical.QueryPlannerKit
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPQueryGraphSolver.extraRequirementForInterestingOrder
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPTable.SORTED_BIT
import org.neo4j.cypher.internal.compiler.planner.logical.idp.cartesianProductsOrValueJoins.planLotsOfCartesianProducts
import org.neo4j.cypher.internal.compiler.planner.logical.steps.BestPlans
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.time.Stopwatch

/**
 * This class is responsible for connecting all disconnected logical plans, which can be
 * done with hash joins when an useful predicate connects the two plans, or with cartesian
 * product lacking that.
 *
 * The input is a set of disconnected patterns and this class will find the
 * cheapest way to connect all components using IDP.
 */
case class ComponentConnectorPlanner(singleComponentPlanner: SingleComponentPlannerTrait, config: IDPSolverConfig, monitor: IDPSolverMonitor)
  extends JoinDisconnectedQueryGraphComponents {

  private val connectors = Seq(
    CartesianProductComponentConnector,
    NestedIndexJoinComponentConnector(singleComponentPlanner),
    ValueHashJoinComponentConnector,
    OptionalMatchConnector,
  )

  override def connectComponentsAndSolveOptionalMatch(components: Set[PlannedComponent],
                                                      queryGraph: QueryGraph,
                                                      interestingOrder: InterestingOrder,
                                                      context: LogicalPlanningContext,
                                                      kit: QueryPlannerKit,
                                                      singleComponentPlanner: SingleComponentPlannerTrait): BestPlans = {

    // kit.select plans predicates and shortest path patterns. If nothing is left in this area, we can skip IDP.
    val allSolved = components.flatMap(_.queryGraph.selections.predicates)
    val notYetSolved = queryGraph.selections.predicates -- allSolved
    if (notYetSolved.isEmpty && queryGraph.optionalMatches.isEmpty && queryGraph.shortestPathPatterns.isEmpty) {
      if (components.size == 1) {
        // If there is only 1 component and no optional matches there is nothing we need to do.
        components.head.plan
      } else {
        // If there are no predicates left to be solved, that also means no joins are possible (because they would need to join on a predicate).
        // Also, the order of cartesian products does not need a search algorithm, since no Selections can be put in-between.
        // The best plan is a simple left-deep tree of cartesian products.
        planLotsOfCartesianProducts(components, queryGraph, context, kit, considerSelections = false).plan
      }
    } else {
      connectWithIDP(components, queryGraph, interestingOrder, context, kit)
    }
  }

  private def connectWithIDP(components: Set[PlannedComponent],
                             queryGraph: QueryGraph,
                             interestingOrder: InterestingOrder,
                             context: LogicalPlanningContext,
                             kit: QueryPlannerKit): BestPlans = {
    val orderRequirement = extraRequirementForInterestingOrder(context, interestingOrder)
    val goalBitAllocation = GoalBitAllocation(
      numComponents = components.size,
      numOptionalMatches = queryGraph.optionalMatches.size
    )
    val generators = connectors.map(_.solverStep(goalBitAllocation, queryGraph, interestingOrder, kit))
    val generator = IDPQueryGraphSolver.composeGenerators(queryGraph, interestingOrder, kit, context, generators)

    val solver = new IDPSolver[QueryGraph, LogicalPlan, LogicalPlanningContext](
      generator = generator,
      projectingSelector = kit.pickBest,
      maxTableSize = config.maxTableSize,
      iterationDurationLimit = config.iterationDurationLimit,
      extraRequirement = orderRequirement,
      monitor = monitor,
      stopWatchFactory = Stopwatch.start
    )

    val seed: Seed[QueryGraph, LogicalPlan] = components.flatMap {
      case PlannedComponent(queryGraph, plan) => Set(
        ((Set(queryGraph), false), plan.bestResult)
      ) ++ plan.bestResultFulfillingReq.map { bestSortedResult =>
        ((Set(queryGraph), true), bestSortedResult)
      }
    }
    // This is ordered such that components come before optional matches, which is required for GoalBitAllocation to work.
    val initialTodo = components.toSeq.map(_.queryGraph) ++ queryGraph.optionalMatches

    solver(seed, initialTodo, context)
  }
}

trait ComponentConnector {
  def solverStep(goalBitAllocation: GoalBitAllocation, queryGraph: QueryGraph, interestingOrder: InterestingOrder, kit: QueryPlannerKit): ComponentConnectorSolverStep
}

/**
 * Helper class to keep track of which bit areas in a Goal refer to either components or optional matches.
 * @param numComponents the number of disconnected components to solve.
 * @param numOptionalMatches the number of optional matches to solve.
 */
case class GoalBitAllocation(numComponents: Int, numOptionalMatches: Int) {
  private val numSorted = 1
  private val startSorted = SORTED_BIT
  private val startComponents = startSorted + numSorted
  private val startOptionals = startComponents + numComponents
  private val startCompacted = startOptionals + numOptionalMatches

  /**
   * @param goal a goal that potentially contains bits for components and optional matches.
   * @return the largest subset of the given goal that only refers to components.
   */
  def componentsGoal(goal: Goal): Goal = Goal(goal.bitSet.range(startComponents, startOptionals))

  /**
   * @param goal a goal that potentially contains bits for components and optional matches.
   * @return the largest subset of the given goal that only refers to optional matches.
   */
  def optionalMatchesGoal(goal: Goal): Goal = Goal(goal.bitSet.range(startOptionals, startCompacted))

}
