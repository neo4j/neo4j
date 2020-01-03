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
package org.neo4j.cypher.internal.compatibility.v2_3

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.{CypherPlannerOption, CypherRuntimeOption}
import org.neo4j.helpers.Clock
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.impl.core.EmbeddedProxySPI
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log

case class Cost23Compiler(graph: GraphDatabaseQueryService,
                          config: CypherCompilerConfiguration,
                          clock: Clock,
                          kernelMonitors: KernelMonitors,
                          log: Log,
                          planner: CypherPlannerOption,
                          runtime: CypherRuntimeOption) extends Cypher23Compiler {

  protected val compiler = {
    val plannerName = planner match {
      case CypherPlannerOption.default => None
      case CypherPlannerOption.cost | CypherPlannerOption.idp => Some(IDPPlannerName)
      case CypherPlannerOption.greedy => Some(GreedyPlannerName)
      case CypherPlannerOption.dp => Some(DPPlannerName)
      case _ => throw new IllegalArgumentException(s"unknown cost based planner: ${planner.name}")
    }

    val runtimeName: Option[RuntimeName] = runtime match {
      case CypherRuntimeOption.default => None
      case CypherRuntimeOption.interpreted => Some(InterpretedRuntimeName)
      case _ => throw new IllegalArgumentException("Runtime is not supported in Cypher 2.3")
    }

    val proxySpi = graph.getDependencyResolver.resolveDependency(classOf[EmbeddedProxySPI])
    CypherCompilerFactory.costBasedCompiler(
      graph.asInstanceOf[GraphDatabaseCypherService].getGraphDatabaseService, new EntityAccessorWrapper(proxySpi), config, clock, new WrappedMonitors(kernelMonitors),
      new StringInfoLogger(log), rewriterSequencer, plannerName, runtimeName)
  }

  override val queryCacheSize: Int = config.queryCacheSize
}
