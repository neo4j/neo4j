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

import org.neo4j.cypher.internal.compatibility.v3_3.Cypher33Planner
import org.neo4j.cypher.internal.compatibility.v3_5.Cypher35Planner
import org.neo4j.cypher.internal.compatibility.{CypherCurrentCompiler, CypherPlanner, RuntimeContext, RuntimeContextCreator}
import org.neo4j.cypher.internal.compiler.v3_5._
import org.neo4j.cypher.internal.executionplan.GeneratedQuery
import org.neo4j.cypher.internal.planner.v3_5.spi.TokenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.CodeStructure
import org.neo4j.cypher.internal.runtime.interpreted.LastCommittedTxIdProvider
import org.neo4j.cypher.internal.runtime.vectorized.dispatcher.{Dispatcher, ParallelDispatcher, SingleThreadedExecutor}
import org.neo4j.cypher.internal.spi.codegen.GeneratedQueryStructure
import org.neo4j.cypher.{CypherPlannerOption, CypherRuntimeOption, CypherUpdateStrategy, CypherVersion}
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.{Log, LogProvider}
import org.neo4j.scheduler.JobScheduler
import org.opencypher.v9_0.frontend.phases.InternalNotificationLogger

class EnterpriseCompilerFactory(community: CommunityCompilerFactory,
                                graph: GraphDatabaseQueryService,
                                kernelMonitors: KernelMonitors,
                                logProvider: LogProvider
                               ) extends CompilerFactory {

  override def createCompiler(cypherVersion: CypherVersion,
                              cypherPlanner: CypherPlannerOption,
                              cypherRuntime: CypherRuntimeOption,
                              cypherUpdateStrategy: CypherUpdateStrategy,
                              config: CypherPlannerConfiguration
                             ): Compiler = {

    val log = logProvider.getLog(getClass)
    val createPlanner: PartialFunction[CypherVersion, CypherPlanner] = {
      case CypherVersion.v3_3 =>
        Cypher33Planner(
          config,
          MasterCompiler.CLOCK,
          kernelMonitors,
          log,
          cypherPlanner,
          cypherUpdateStrategy,
          LastCommittedTxIdProvider(graph))

      case CypherVersion.v3_5 =>
        Cypher35Planner(
          config,
          MasterCompiler.CLOCK,
          kernelMonitors,
          log,
          cypherPlanner,
          cypherUpdateStrategy,
          LastCommittedTxIdProvider(graph))
      }

    if (cypherPlanner != CypherPlannerOption.rule && createPlanner.isDefinedAt(cypherVersion)) {

      val planner = createPlanner(cypherVersion)
      val settings = graph.getDependencyResolver.resolveDependency(classOf[Config])
      val morselSize: Int = settings.get(GraphDatabaseSettings.cypher_morsel_size)
      val workers: Int = settings.get(GraphDatabaseSettings.cypher_worker_count)
      val dispatcher =
        if (workers == 1) new SingleThreadedExecutor(morselSize)
        else {
          val numberOfThreads = if (workers == 0) java.lang.Runtime.getRuntime.availableProcessors() else workers
          val jobScheduler = graph.getDependencyResolver.resolveDependency(classOf[JobScheduler])
          val executorService = jobScheduler.workStealingExecutor(JobScheduler.Groups.cypherWorker, numberOfThreads)

          new ParallelDispatcher(morselSize, numberOfThreads, executorService)
        }

      CypherCurrentCompiler(
        planner,
        EnterpriseRuntimeFactory.getRuntime(cypherRuntime, config.useErrorsOverWarnings),
        EnterpriseRuntimeContextCreator(GeneratedQueryStructure, dispatcher, log),
        kernelMonitors)

    } else
      community.createCompiler(cypherVersion, cypherPlanner, cypherRuntime, cypherUpdateStrategy, config)
  }
}

/**
  * Enterprise runtime context. Enriches the community runtime context with infrastructure needed for
  * query compilation and parallel execution.
  */
case class EnterpriseRuntimeContext(notificationLogger: InternalNotificationLogger,
                                    tokenContext: TokenContext,
                                    codeStructure: CodeStructure[GeneratedQuery],
                                    dispatcher: Dispatcher,
                                    log: Log,
                                    clock: Clock,
                                    debugOptions: Set[String]
                                   ) extends RuntimeContext

/**
  * Creator of EnterpriseRuntimeContext
  */
case class EnterpriseRuntimeContextCreator(codeStructure: CodeStructure[GeneratedQuery],
                                           dispatcher: Dispatcher,
                                           log: Log)
  extends RuntimeContextCreator[EnterpriseRuntimeContext] {

  override def create(notificationLogger: InternalNotificationLogger,
                      tokenContext: TokenContext,
                      clock: Clock,
                      debugOptions: Set[String]
                     ): EnterpriseRuntimeContext =
    EnterpriseRuntimeContext(notificationLogger, tokenContext, codeStructure, dispatcher, log, clock, debugOptions)
}
