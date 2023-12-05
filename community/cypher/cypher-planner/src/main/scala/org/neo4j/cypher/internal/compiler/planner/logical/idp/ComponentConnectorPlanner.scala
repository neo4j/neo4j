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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.QueryPlannerKit
import org.neo4j.cypher.internal.compiler.planner.logical.idp.GoalBitAllocation.startComponents
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPQueryGraphSolver.extraRequirementForInterestingOrder
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPTable.SORTED_BIT
import org.neo4j.cypher.internal.compiler.planner.logical.idp.cartesianProductsOrValueJoins.COMPONENT_THRESHOLD_FOR_CARTESIAN_PRODUCT
import org.neo4j.cypher.internal.compiler.planner.logical.idp.cartesianProductsOrValueJoins.planLotsOfCartesianProducts
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.BestPlans
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.time.Stopwatch

import scala.collection.immutable.BitSet

/**
 * This class is responsible for connecting all disconnected logical plans, which can be
 * done with hash joins when an useful predicate connects the two plans, or with cartesian
 * product lacking that.
 *
 * The input is a set of disconnected patterns and this class will find the
 * cheapest way to connect all components using IDP.
 */
case class ComponentConnectorPlanner(singleComponentPlanner: SingleComponentPlannerTrait, config: IDPSolverConfig)
                                    (monitor: IDPSolverMonitor)
  extends JoinDisconnectedQueryGraphComponents {

  private val cpConnector = CartesianProductComponentConnector
  private val joinConnectors = Seq(
    NestedIndexJoinComponentConnector(singleComponentPlanner),
    ValueHashJoinComponentConnector,
  )
  private val omConnector = OptionalMatchConnector

  override def connectComponentsAndSolveOptionalMatch(components: Set[PlannedComponent],
                                                      queryGraph: QueryGraph,
                                                      interestingOrderConfig: InterestingOrderConfig,
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
        planLotsOfCartesianProducts(components, queryGraph, interestingOrderConfig, context, kit, considerSelections = false).plan
      }
    } else {
      connectWithIDP(components, queryGraph, interestingOrderConfig, context, kit)
    }
  }

  private def connectWithIDP(components: Set[PlannedComponent],
                             queryGraph: QueryGraph,
                             interestingOrderConfig: InterestingOrderConfig,
                             context: LogicalPlanningContext,
                             kit: QueryPlannerKit): BestPlans = {
    val orderRequirement = extraRequirementForInterestingOrder(context, interestingOrderConfig)
    val (goalBitAllocation, initialTodo) = GoalBitAllocation.create(components.map(_.queryGraph), queryGraph)

    val joinSolverSteps = joinConnectors.map(_.solverStep(goalBitAllocation, queryGraph, interestingOrderConfig, kit, context))
    val composedJoinSolverStep = IDPQueryGraphSolver.composeSolverSteps(queryGraph, interestingOrderConfig, kit, context, joinSolverSteps)
    val cpSolverStep = cpConnector.solverStep(goalBitAllocation, queryGraph, interestingOrderConfig, kit, context)
    val composedCPSolverStep = IDPQueryGraphSolver.selectingAndSortingSolverStep(queryGraph, interestingOrderConfig, kit, context, cpSolverStep)
    val omSolverStep = omConnector.solverStep(goalBitAllocation, queryGraph, interestingOrderConfig, kit, context)
    val composedOmSolverStep = IDPQueryGraphSolver.selectingAndSortingSolverStep(queryGraph, interestingOrderConfig, kit, context, omSolverStep)

    val generator = {
      val solverSteps = if (components.size < COMPONENT_THRESHOLD_FOR_CARTESIAN_PRODUCT) {
        composedJoinSolverStep ++ composedCPSolverStep ++ composedOmSolverStep
      } else {
        // Only even generate CP plans if no joins are available
        (composedJoinSolverStep || composedCPSolverStep) ++ composedOmSolverStep
      }

      // Filter out goals that are not solvable before even asking the connectors
      solverSteps.filterGoals(goalBitAllocation.goalIsSolvable)
    }

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
    solver(seed, initialTodo, context)
  }
}

trait ComponentConnector {
  def solverStep(goalBitAllocation: GoalBitAllocation,
                 queryGraph: QueryGraph,
                 interestingOrderConfig: InterestingOrderConfig,
                 kit: QueryPlannerKit,
                 context: LogicalPlanningContext): ComponentConnectorSolverStep
}

object GoalBitAllocation {
  private val numSorted = 1
  private val startSorted = SORTED_BIT
  private val startComponents = startSorted + numSorted

  /**
   * Given the components and the overall query graph, return a [[GoalBitAllocation]] and the initialTodo for the [[IDPSolver]].
   * Capture all dependencies from optional matches.
   */
  def create(components: Set[QueryGraph],
             queryGraph: QueryGraph): (GoalBitAllocation, Seq[QueryGraph]) = {
    val initialTodo = components.toSeq ++ queryGraph.optionalMatches

    // For each optional match, find dependencies to components and other optional matches
    val optionalMatchDependencies = queryGraph.optionalMatches.map { om =>
      om.argumentIds.map { arg =>
        val index = initialTodo.indexWhere(x => x.idsWithoutOptionalMatchesOrUpdates.contains(arg))
        AssertMacros.checkOnlyWhenAssertionsAreEnabled(index >= 0, "Did not find which QG introduces dependency of optional match.")
        startComponents + index
      }(collection.breakOut): BitSet // directly create a BitSet using CanBuildFrom magic
    }

    val gba = GoalBitAllocation(
      numComponents = components.size,
      numOptionalMatches = queryGraph.optionalMatches.size,
      optionalMatchDependencies
    )

    (gba, initialTodo)
  }
}

/**
 * Helper class to keep track of which bit areas in a Goal refer to either components or optional matches.
 *
 * @param numComponents             the number of disconnected components to solve.
 * @param numOptionalMatches        the number of optional matches to solve.
 * @param optionalMatchDependencies for each optional match a BitSet describing its dependencies to components and other optional matches.
 *                                  Each bit that is set in this BitSet signifies a dependency to the component or optional match at that bit position.
 */
case class GoalBitAllocation(numComponents: Int, numOptionalMatches: Int, optionalMatchDependencies: Seq[BitSet]) {
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

  /**
   * Test whether a goal can be solved by testing if all bits have their dependant bits set as well.
   */
  def goalIsSolvable(registry: IdRegistry[_], goal: Goal): Boolean = {
    // The original bits of the goal.
    val originalGoalBits = registry.exlodedBitSet(goal.bitSet)
    // All not-yet compacted optional match bits.
    optionalMatchesGoal(goal).bitSet
      // All dependency bits
      .flatMap(i => optionalMatchDependencies(i - startOptionals))
      // Are all dependency bits included in the goal?
      .subsetOf(originalGoalBits)
  }
}
