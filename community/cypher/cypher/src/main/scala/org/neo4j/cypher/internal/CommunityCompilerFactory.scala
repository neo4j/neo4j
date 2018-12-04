/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compatibility.v3_5.Cypher3_5Planner
import org.neo4j.cypher.internal.compatibility.v4_0.Cypher4_0Planner
import org.neo4j.cypher.internal.compiler.v4_0.CypherPlannerConfiguration
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log
import org.neo4j.logging.LogProvider

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
                              cypherUpdateStrategy: CypherUpdateStrategy
                             ): Compiler = {

    (cypherVersion, cypherPlanner) match {
        // 3.5
      case (CypherVersion.v3_5, _) =>
        CypherCurrentCompiler(
          Cypher3_5Planner(plannerConfig, MasterCompiler.CLOCK, kernelMonitors, log,
            cypherPlanner, cypherUpdateStrategy, LastCommittedTxIdProvider(graph)),
          CommunityRuntimeFactory.getRuntime(cypherRuntime, plannerConfig.useErrorsOverWarnings),
          CommunityRuntimeContextCreator(plannerConfig),
          kernelMonitors
        )

        // 4.0
      case (CypherVersion.v4_0, _) =>
        CypherCurrentCompiler(
          Cypher4_0Planner(plannerConfig, MasterCompiler.CLOCK, kernelMonitors, log,
                          cypherPlanner, cypherUpdateStrategy, LastCommittedTxIdProvider(graph)),
          CommunityRuntimeFactory.getRuntime(cypherRuntime, plannerConfig.useErrorsOverWarnings),
          CommunityRuntimeContextCreator(plannerConfig),
          kernelMonitors
        )
    }
  }
}
