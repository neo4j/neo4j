/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.planner

import org.neo4j.cypher.internal.compiler.v3_1._
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical._
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.idp._
import org.neo4j.cypher.internal.compiler.v3_1.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_1.InternalException

object CostBasedPipeBuilderFactory {

  def create(monitors: Monitors,
             metricsFactory: MetricsFactory,
             queryPlanner: QueryPlanner,
             rewriterSequencer: (String) => RewriterStepSequencer,
             semanticChecker: SemanticChecker,
             tokenResolver: SimpleTokenResolver = new SimpleTokenResolver(),
             plannerName: Option[CostBasedPlannerName],
             runtimeBuilder: RuntimeBuilder,
             config: CypherCompilerConfiguration,
             updateStrategy: Option[UpdateStrategy],
             publicTypeConverter: Any => Any
  ) = {

    def createQueryGraphSolver(n: CostBasedPlannerName): QueryGraphSolver = n match {
      case IDPPlannerName =>
        val monitor = monitors.newMonitor[IDPQueryGraphSolverMonitor]()
        val solverConfig = new ConfigurableIDPSolverConfig(
          maxTableSize = config.idpMaxTableSize,
          iterationDurationLimit = config.idpIterationDuration
        )
        val singleComponentPlanner = SingleComponentPlanner(monitor, solverConfig)

        IDPQueryGraphSolver(singleComponentPlanner, cartesianProductsOrValueJoins, monitor)

      case DPPlannerName =>
        val monitor = monitors.newMonitor[IDPQueryGraphSolverMonitor]()
        val singleComponentPlanner = SingleComponentPlanner(monitor, DPSolverConfig)
        IDPQueryGraphSolver(singleComponentPlanner, cartesianProductsOrValueJoins, monitor)

      case _ => throw new InternalException(s"$n is not a valid planner")
    }

    val actualPlannerName = plannerName.getOrElse(CostBasedPlannerName.default)
    val actualUpdateStrategy = updateStrategy.getOrElse(defaultUpdateStrategy)
    CostBasedExecutablePlanBuilder(monitors, metricsFactory, tokenResolver, queryPlanner,
      createQueryGraphSolver(actualPlannerName), rewriterSequencer, semanticChecker, actualPlannerName, runtimeBuilder,
      actualUpdateStrategy,
      config, publicTypeConverter)
  }
}
