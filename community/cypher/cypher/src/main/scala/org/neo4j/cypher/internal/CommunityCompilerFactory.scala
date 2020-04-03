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

import org.neo4j.cypher.CypherPlannerOption
import org.neo4j.cypher.CypherRuntimeOption
import org.neo4j.cypher.CypherUpdateStrategy
import org.neo4j.cypher.CypherVersion
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.phases.Compatibility3_5
import org.neo4j.cypher.internal.compiler.phases.Compatibility4_0
import org.neo4j.cypher.internal.compiler.phases.Compatibility4_1
import org.neo4j.cypher.internal.planning.CypherPlanner
import org.neo4j.exceptions.SyntaxException
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.logging.Log
import org.neo4j.logging.LogProvider
import org.neo4j.monitoring

/**
 * Factory which creates cypher compilers.
 */
class CommunityCompilerFactory(graph: GraphDatabaseQueryService,
                               kernelMonitors: monitoring.Monitors,
                               logProvider: LogProvider,
                               plannerConfig: CypherPlannerConfiguration,
                               runtimeConfig: CypherRuntimeConfiguration
                              ) extends CompilerFactory {

  private val log: Log = logProvider.getLog(getClass)

  override def supportsAdministrativeCommands(): Boolean = plannerConfig.planSystemCommands

  override def createCompiler(cypherVersion: CypherVersion,
                              cypherPlanner: CypherPlannerOption,
                              cypherRuntime: CypherRuntimeOption,
                              cypherUpdateStrategy: CypherUpdateStrategy,
                              executionEngineProvider: () => ExecutionEngine): Compiler = {

    val compatibilityMode = cypherVersion match {
      case CypherVersion.v3_5 => Compatibility3_5
      case CypherVersion.v4_0 => Compatibility4_0
      case CypherVersion.v4_1 => Compatibility4_1
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
      cypherVersion match {
        case CypherVersion.v3_5 => throw new SyntaxException("Commands towards system database are not supported in this Cypher version.")
        case _ => CommunityAdministrationCommandRuntime(executionEngineProvider(), graph.getDependencyResolver)
      }
    else
      CommunityRuntimeFactory.getRuntime(cypherRuntime, plannerConfig.useErrorsOverWarnings)

    CypherCurrentCompiler(
      planner,
      runtime,
      CommunityRuntimeContextManager(log, runtimeConfig),
      kernelMonitors)
  }
}
