/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.cypher.CypherPlanner
import org.neo4j.cypher.internal.compatibility.v3_4.Compatibility
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.EnterpriseRuntimeContextCreator
import org.neo4j.cypher.internal.compatibility.{v2_3, v3_1, v3_3 => v3_3compat}
import org.neo4j.cypher.internal.compiler.v3_4._
import org.neo4j.cypher.internal.runtime.vectorized.dispatcher.{ParallelDispatcher, SingleThreadedExecutor}
import org.neo4j.cypher.internal.spi.v3_4.codegen.GeneratedQueryStructure
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.LogProvider
import org.neo4j.scheduler.JobScheduler

class EnterpriseCompatibilityFactory(inner: CompatibilityFactory, graph: GraphDatabaseQueryService,
                                     kernelMonitors: KernelMonitors,
                                     logProvider: LogProvider) extends CompatibilityFactory {
  override def create(spec: PlannerSpec_v2_3, config: CypherCompilerConfiguration): v2_3.Compatibility =
    inner.create(spec, config)

  override def create(spec: PlannerSpec_v3_1, config: CypherCompilerConfiguration): v3_1.Compatibility =
    inner.create(spec, config)

  override def create(spec: PlannerSpec_v3_3, config: CypherCompilerConfiguration): v3_3compat.Compatibility[_,_,_] =
    inner.create(spec, config)

  override def create(spec: PlannerSpec_v3_4, config: CypherCompilerConfiguration): Compatibility[_,_] =
    (spec.planner, spec.runtime) match {
      case (CypherPlanner.rule, _) => inner.create(spec, config)

      case _ =>
        val settings = graph.getDependencyResolver.resolveDependency(classOf[Config])
        val morselSize: Int = settings.get(GraphDatabaseSettings.cypher_morsel_size)
        val workers: Int = settings.get(GraphDatabaseSettings.cypher_worker_count)
        val dispatcher =
          if (workers == 1) new SingleThreadedExecutor(morselSize)
          else {
            val numberOfThreads = if (workers == 0) Runtime.getRuntime.availableProcessors() else workers
            val jobScheduler = graph.getDependencyResolver.resolveDependency(classOf[JobScheduler])
            val executorService = jobScheduler.workStealingExecutor(JobScheduler.Groups.cypherWorker, numberOfThreads)

            new ParallelDispatcher(morselSize, numberOfThreads, executorService)
          }
        Compatibility(config, CompilerEngineDelegator.CLOCK, kernelMonitors, logProvider.getLog(getClass),
                          spec.planner, spec.runtime, spec.updateStrategy, EnterpriseRuntimeBuilder,
                          EnterpriseRuntimeContextCreator(GeneratedQueryStructure, dispatcher))
    }
}
