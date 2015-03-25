/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.planner.execution.PipeExecutionPlanBuilder
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.greedy.{expandsOrJoins, expandsOnly, GreedyQueryGraphSolver}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.idp.{IDPQueryGraphSolverMonitor, IDPQueryGraphSolver}
import org.neo4j.helpers.Clock

final case class CostBasedPlanningStrategy(plannerName: CostBasedPlannerName, acceptQuery: QueryAcceptor)

object CostBasedPlanningStrategy {
  def default = apply(PlannerName.default)

  def apply(plannerName: CostBasedPlannerName): CostBasedPlanningStrategy =
    CostBasedPlanningStrategy(plannerName, allQueryAcceptor)
}

object CostBasedPipeBuilderFactory {

  def apply(monitors: Monitors,
            metricsFactory: MetricsFactory,
            monitor: PlanningMonitor,
            clock: Clock,
            tokenResolver: SimpleTokenResolver = new SimpleTokenResolver(),
            maybeExecutionPlanBuilder: Option[PipeExecutionPlanBuilder] = None,
            queryPlanner: QueryPlanner = new DefaultQueryPlanner(),
            planningStrategy: CostBasedPlanningStrategy = CostBasedPlanningStrategy.default
           ) = {

    val executionPlanBuilder: PipeExecutionPlanBuilder =
      maybeExecutionPlanBuilder.getOrElse(new PipeExecutionPlanBuilder(clock, monitors))

    val plannerName = planningStrategy.plannerName
    val queryGraphSolver = plannerName match {
      case IDPPlannerName =>
        IDPQueryGraphSolver(monitors.newMonitor[IDPQueryGraphSolverMonitor]())

      case DPPlannerName =>
        IDPQueryGraphSolver(monitors.newMonitor[IDPQueryGraphSolverMonitor](), maxTableSize = Int.MaxValue)

      case _ =>
//        IDPQueryGraphSolver(monitors.newMonitor[IDPQueryGraphSolverMonitor]())
        new CompositeQueryGraphSolver(
          new GreedyQueryGraphSolver(expandsOrJoins),
          new GreedyQueryGraphSolver(expandsOnly)
        )
    }

    CostBasedPipeBuilder(monitors, metricsFactory, monitor, clock, tokenResolver, executionPlanBuilder, queryPlanner, planningStrategy.acceptQuery, queryGraphSolver, plannerName)
  }
}
