/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal

import java.time.Clock

import org.neo4j.cypher.internal.MorselRuntime.MorselRuntimeState
import org.neo4j.cypher.internal.compatibility.v3_4.Cypher34Planner
import org.neo4j.cypher.internal.compatibility.v3_5.Cypher35Planner
import org.neo4j.cypher.internal.compatibility.{CypherCurrentCompiler, CypherPlanner, CypherRuntimeConfiguration, RuntimeContext, RuntimeContextCreator}
import org.neo4j.cypher.internal.compiler.v3_5._
import org.neo4j.cypher.internal.executionplan.GeneratedQuery
import org.neo4j.cypher.internal.planner.v3_5.spi.TokenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.CodeStructure
import org.neo4j.cypher.internal.runtime.interpreted.LastCommittedTxIdProvider
import org.neo4j.cypher.internal.spi.codegen.GeneratedQueryStructure
import org.neo4j.cypher.{CypherPlannerOption, CypherRuntimeOption, CypherUpdateStrategy, CypherVersion}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.{Log, LogProvider}
import org.neo4j.scheduler.JobScheduler
import org.opencypher.v9_0.frontend.phases.InternalNotificationLogger

class EnterpriseCompilerFactory(community: CommunityCompilerFactory,
                                graph: GraphDatabaseQueryService,
                                kernelMonitors: KernelMonitors,
                                logProvider: LogProvider,
                                plannerConfig: CypherPlannerConfiguration,
                                runtimeConfig: CypherRuntimeConfiguration
                               ) extends CompilerFactory {
  /*
  One compiler is created for every Planner:Runtime:Version combination, e.g., Cost-Morsel-3.4 & Cost-Morsel-3.5.
  Each compiler contains a runtime instance, and each morsel runtime instance requires a dispatcher instance.
  This ensures only one (shared) dispatcher/tracer instance is created, even when there are multiple morsel runtime instances.
   */
  private val morselRuntimeState = MorselRuntimeState(runtimeConfig, graph.getDependencyResolver.resolveDependency(classOf[JobScheduler]))

  override def createCompiler(cypherVersion: CypherVersion,
                              cypherPlanner: CypherPlannerOption,
                              cypherRuntime: CypherRuntimeOption,
                              cypherUpdateStrategy: CypherUpdateStrategy): Compiler = {

    val log = logProvider.getLog(getClass)
    val createPlanner: PartialFunction[CypherVersion, CypherPlanner] = {
      case CypherVersion.v3_4 =>
        Cypher34Planner(
          plannerConfig,
          MasterCompiler.CLOCK,
          kernelMonitors,
          log,
          cypherPlanner,
          cypherUpdateStrategy,
          LastCommittedTxIdProvider(graph))

      case CypherVersion.v3_5 =>
        Cypher35Planner(
          plannerConfig,
          MasterCompiler.CLOCK,
          kernelMonitors,
          log,
          cypherPlanner,
          cypherUpdateStrategy,
          LastCommittedTxIdProvider(graph))
      }

    if (cypherPlanner != CypherPlannerOption.rule && createPlanner.isDefinedAt(cypherVersion)) {
      val planner = createPlanner(cypherVersion)

      CypherCurrentCompiler(
        planner,
        EnterpriseRuntimeFactory.getRuntime(cypherRuntime, plannerConfig.useErrorsOverWarnings),
        EnterpriseRuntimeContextCreator(GeneratedQueryStructure, log, plannerConfig, morselRuntimeState),
        kernelMonitors)

    } else
      community.createCompiler(cypherVersion, cypherPlanner, cypherRuntime, cypherUpdateStrategy)
  }
}

/**
  * Enterprise runtime context. Enriches the community runtime context with infrastructure needed for
  * query compilation and parallel execution.
  */
case class EnterpriseRuntimeContext(notificationLogger: InternalNotificationLogger,
                                    tokenContext: TokenContext,
                                    readOnly: Boolean,
                                    codeStructure: CodeStructure[GeneratedQuery],
                                    log: Log,
                                    clock: Clock,
                                    debugOptions: Set[String],
                                    config: CypherPlannerConfiguration,
                                    morselRuntimeState: MorselRuntimeState) extends RuntimeContext

/**
  * Creator of EnterpriseRuntimeContext
  */
case class EnterpriseRuntimeContextCreator(codeStructure: CodeStructure[GeneratedQuery],
                                           log: Log,
                                           config: CypherPlannerConfiguration,
                                           morselRuntimeState: MorselRuntimeState)
  extends RuntimeContextCreator[EnterpriseRuntimeContext] {

  override def create(notificationLogger: InternalNotificationLogger,
                      tokenContext: TokenContext,
                      clock: Clock,
                      debugOptions: Set[String],
                      readOnly: Boolean): EnterpriseRuntimeContext =
    EnterpriseRuntimeContext(notificationLogger,
                             tokenContext,
                             readOnly,
                             codeStructure,
                             log,
                             clock,
                             debugOptions,
                             config,
                             morselRuntimeState)
}
