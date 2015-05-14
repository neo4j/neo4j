/*
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
package org.neo4j.cypher.internal.compiler.v2_3.planner

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.planner.execution.PipeExecutionPlanBuilder
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.greedy.{GreedyQueryGraphSolver, expandsOnly, expandsOrJoins}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.idp.{IDPQueryGraphSolver, IDPQueryGraphSolverMonitor}
import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.RewriterStepSequencer
import org.neo4j.helpers.Clock

object CostBasedPipeBuilderFactory {

  def apply(monitors: Monitors,
            metricsFactory: MetricsFactory,
            clock: Clock,
            queryPlanner: QueryPlanner,
            rewriterSequencer: (String) => RewriterStepSequencer,
            tokenResolver: SimpleTokenResolver = new SimpleTokenResolver(),
            maybeExecutionPlanBuilder: Option[PipeExecutionPlanBuilder] = None,
            plannerName: CostBasedPlannerName = PlannerName.default,
            runtimeName: RuntimeName = RuntimeName.default
           ) = {

    val executionPlanBuilder: PipeExecutionPlanBuilder =
      maybeExecutionPlanBuilder.getOrElse(new PipeExecutionPlanBuilder(clock, monitors))

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

    CostBasedExecutablePlanBuilder(monitors, metricsFactory, clock, tokenResolver, executionPlanBuilder, queryPlanner, queryGraphSolver, rewriterSequencer, plannerName, runtimeName)
  }
}
