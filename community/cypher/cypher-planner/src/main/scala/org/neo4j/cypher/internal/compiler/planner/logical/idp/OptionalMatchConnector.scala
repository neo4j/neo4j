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
import org.neo4j.cypher.internal.compiler.planner.logical.QueryPlannerKit
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.OptionalSolver
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

case object OptionalMatchConnector
    extends ComponentConnector {

  override def solverStep(
    goalBitAllocation: GoalBitAllocation,
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    kit: QueryPlannerKit,
    context: LogicalPlanningContext
  ): ComponentConnectorSolverStep = {

    // A map from each OPTIONAL MATCH QueryGraph to solvers that can connect the plan for that optional match
    // to the current LHS plan.
    val optionalSolvers: Map[QueryGraph, Seq[OptionalSolver.Solver]] = queryGraph.optionalMatches.map { optionalQG =>
      optionalQG -> context.plannerState.config.optionalSolvers.map { getSolver =>
        getSolver.solver(optionalQG, queryGraph, interestingOrderConfig, context)
      }
    }.toMap

    (registry: IdRegistry[QueryGraph], goal: Goal, table: IDPCache[LogicalPlan], context: LogicalPlanningContext) => {
      val optionalsGoal = goalBitAllocation.optionalMatchesGoal(goal)
      for {
        id <- optionalsGoal.bitSet.iterator
        optionalQg <- registry.lookup(id).iterator
        leftGoal = Goal(goal.bitSet - id)
        leftPlan <- table(leftGoal).iterator
        canPlan = optionalQg.argumentIds subsetOf leftPlan.availableSymbols
        if canPlan
        optionalSolver <- optionalSolvers(optionalQg)
        plan <- optionalSolver.connect(leftPlan)
      } yield plan
    }
  }
}
