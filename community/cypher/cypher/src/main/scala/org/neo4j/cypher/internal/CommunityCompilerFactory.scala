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

import org.neo4j.cypher.internal.compatibility.v2_3.helpers._
import org.neo4j.cypher.internal.compatibility.v3_1.helpers._
import org.neo4j.cypher.internal.compatibility.v3_4.Cypher34Planner
import org.neo4j.cypher.internal.compatibility.v3_5.Cypher35Planner
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compiler.v3_5.CypherPlannerConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.LastCommittedTxIdProvider
import org.neo4j.cypher.{CypherPlannerOption, CypherRuntimeOption, CypherUpdateStrategy, CypherVersion}
import org.neo4j.helpers.Clock
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.{Log, LogProvider}
import org.neo4j.cypher.internal.v3_5.util.InvalidArgumentException

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

        // 2.3
      case (CypherVersion.v2_3, CypherPlannerOption.rule) =>
        v2_3.Rule23Compiler(graph, as2_3(plannerConfig), Clock.SYSTEM_CLOCK, kernelMonitors)
      case (CypherVersion.v2_3, _) =>
        v2_3.Cost23Compiler(graph, as2_3(plannerConfig), Clock.SYSTEM_CLOCK, kernelMonitors, log, cypherPlanner, cypherRuntime)

        // 3.1
      case (CypherVersion.v3_1, CypherPlannerOption.rule) =>
        v3_1.Rule31Compiler(graph, as3_1(plannerConfig), MasterCompiler.CLOCK, kernelMonitors)
      case (CypherVersion.v3_1, _) =>
        v3_1.Cost31Compiler(graph, as3_1(plannerConfig), MasterCompiler.CLOCK, kernelMonitors, log, cypherPlanner, cypherRuntime, cypherUpdateStrategy)

        // 3.3 or 3.5 + rule
      case (_, CypherPlannerOption.rule) =>
        throw new InvalidArgumentException(s"The rule planner is no longer a valid planner option in Neo4j ${cypherVersion.name}. If you need to use it, please select compatibility mode Cypher 3.1")

        // 3.4
      case (CypherVersion.v3_4, _) =>
        CypherCurrentCompiler(
          Cypher34Planner(plannerConfig, MasterCompiler.CLOCK, kernelMonitors, log,
            cypherPlanner, cypherUpdateStrategy, LastCommittedTxIdProvider(graph)),
          CommunityRuntimeFactory.getRuntime(cypherRuntime, plannerConfig.useErrorsOverWarnings),
          CommunityRuntimeContextCreator(log, plannerConfig),
          kernelMonitors
        )

        // 3.5
      case (CypherVersion.v3_5, _) =>
        CypherCurrentCompiler(
          Cypher35Planner(plannerConfig, MasterCompiler.CLOCK, kernelMonitors, log,
                          cypherPlanner, cypherUpdateStrategy, LastCommittedTxIdProvider(graph)),
          CommunityRuntimeFactory.getRuntime(cypherRuntime, plannerConfig.useErrorsOverWarnings),
          CommunityRuntimeContextCreator(log, plannerConfig),
          kernelMonitors
        )
    }
  }
}
