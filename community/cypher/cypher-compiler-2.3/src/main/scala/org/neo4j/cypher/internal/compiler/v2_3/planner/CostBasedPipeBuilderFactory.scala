/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.greedy.{GreedyQueryGraphSolver, expandsOnly, expandsOrJoins}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.idp._
import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.RewriterStepSequencer

object CostBasedPipeBuilderFactory {

  def create(monitors: Monitors,
             metricsFactory: MetricsFactory,
             queryPlanner: QueryPlanner,
             rewriterSequencer: (String) => RewriterStepSequencer,
             semanticChecker: SemanticChecker,
             tokenResolver: SimpleTokenResolver = new SimpleTokenResolver(),
             plannerName: Option[CostBasedPlannerName],
             runtimeBuilder: RuntimeBuilder,
             useErrorsOverWarnings: Boolean,
             idpMaxTableSize: Int,
             idpIterationDuration: Long
    ) = {

    def createQueryGraphSolver(n: CostBasedPlannerName): QueryGraphSolver = n match {
      case IDPPlannerName =>
        IDPQueryGraphSolver(monitors.newMonitor[IDPQueryGraphSolverMonitor](), solverConfig = new ConfigurableIDPSolverConfig(
          maxTableSize = idpMaxTableSize,
          iterationDurationLimit = idpIterationDuration
        ))

      case DPPlannerName =>
        IDPQueryGraphSolver(monitors.newMonitor[IDPQueryGraphSolverMonitor](), solverConfig = DPSolverConfig)

      case GreedyPlannerName =>
        new CompositeQueryGraphSolver(
          new GreedyQueryGraphSolver(expandsOrJoins),
          new GreedyQueryGraphSolver(expandsOnly))
    }

    val actualPlannerName = plannerName.getOrElse(CostBasedPlannerName.default)
    CostBasedExecutablePlanBuilder(monitors, metricsFactory, tokenResolver, queryPlanner, createQueryGraphSolver(actualPlannerName), rewriterSequencer, semanticChecker, actualPlannerName, runtimeBuilder, useErrorsOverWarnings)
  }
}
