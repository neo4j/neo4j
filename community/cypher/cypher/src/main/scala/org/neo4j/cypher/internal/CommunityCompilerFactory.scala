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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.planning.CypherPlanner
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.{CypherPlannerOption, CypherRuntimeOption, CypherUpdateStrategy, CypherVersion}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.logging.{Log, LogProvider}
import org.neo4j.monitoring.{Monitors => KernelMonitors}

/**
  * Factory which creates cypher compilers.
  */
class CommunityCompilerFactory(graph: GraphDatabaseQueryService,
                               kernelMonitors: KernelMonitors,
                               logProvider: LogProvider,
                               plannerConfig: CypherPlannerConfiguration,
                               runtimeConfig: CypherRuntimeConfiguration
                              ) extends CompilerFactory {

  private val log: Log = logProvider.getLog(getClass)

  override def createCompiler(cypherVersion: CypherVersion,
                              cypherPlanner: CypherPlannerOption,
                              cypherRuntime: CypherRuntimeOption,
                              cypherUpdateStrategy: CypherUpdateStrategy,
                              executionEngineProvider: () => ExecutionEngine): Compiler = {

    val compatibilityMode = cypherVersion match {
      case CypherVersion.`v3_5` => true
      case CypherVersion.v4_0 => false
    }

    val planner =
      CypherPlanner(
        plannerConfig,
        MasterCompiler.CLOCK,
        kernelMonitors,
        log,
        cypherPlanner,
        cypherUpdateStrategy,
        LastCommittedTxIdProvider(graph),
        compatibilityMode)

    val runtime = if (plannerConfig.planSystemCommands)
      CommunityAdministrationCommandRuntime(executionEngineProvider(), graph.getDependencyResolver)
    else
      CommunityRuntimeFactory.getRuntime(cypherRuntime, plannerConfig.useErrorsOverWarnings)

    CypherCurrentCompiler(
      planner,
      runtime,
      CommunityRuntimeContextManager(log, runtimeConfig),
      kernelMonitors)
  }
}
