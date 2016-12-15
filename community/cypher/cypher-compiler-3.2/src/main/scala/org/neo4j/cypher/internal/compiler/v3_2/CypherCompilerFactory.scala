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
package org.neo4j.cypher.internal.compiler.v3_2

import java.time.Clock

import org.neo4j.cypher.internal.compiler.v3_2.codegen.spi.CodeStructure
import org.neo4j.cypher.internal.compiler.v3_2.executionplan._
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.procs.DelegatingProcedureExecutablePlanBuilder
import org.neo4j.cypher.internal.compiler.v3_2.helpers.RuntimeTypeConverter
import org.neo4j.cypher.internal.compiler.v3_2.planner.CostBasedPipeBuilderFactory
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.rewriter.LogicalPlanRewriter
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.{CachedMetricsFactory, DefaultQueryPlanner, SimpleMetricsFactory}
import org.neo4j.cypher.internal.compiler.v3_2.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_2.ast.Statement

object CypherCompilerFactory {
  val monitorTag = "cypher3.2"

  def costBasedCompiler(config: CypherCompilerConfiguration, clock: Clock,
                        structure: CodeStructure[GeneratedQuery],
                        monitors: Monitors, logger: InfoLogger,
                        rewriterSequencer: (String) => RewriterStepSequencer,
                        plannerName: Option[CostBasedPlannerName],
                        runtimeName: Option[RuntimeName],
                        updateStrategy: Option[UpdateStrategy],
                        typeConverter: RuntimeTypeConverter): CypherCompiler = {
    val rewriter = new ASTRewriter(rewriterSequencer)
    val metricsFactory = CachedMetricsFactory(SimpleMetricsFactory)
    val queryPlanner = DefaultQueryPlanner(LogicalPlanRewriter(rewriterSequencer))

    val compiledPlanBuilder = CompiledPlanBuilder(clock, structure)
    val interpretedPlanBuilder = InterpretedPlanBuilder(clock, monitors, typeConverter)

    // Pick runtime based on input
    val runtimeBuilder = RuntimeBuilder.create(runtimeName, interpretedPlanBuilder, compiledPlanBuilder, config.useErrorsOverWarnings)

    val costPlanProducer = CostBasedPipeBuilderFactory.create(
      monitors = monitors,
      metricsFactory = metricsFactory,
      queryPlanner = queryPlanner,
      rewriterSequencer = rewriterSequencer,
      plannerName = plannerName,
      runtimeBuilder = runtimeBuilder,
      config = config,
      updateStrategy = updateStrategy,
      publicTypeConverter = typeConverter.asPublicType
    )
    val procedurePlanProducer = DelegatingProcedureExecutablePlanBuilder(costPlanProducer, typeConverter.asPublicType)

    val createFingerprintReference: (Option[PlanFingerprint]) => PlanFingerprintReference =
      new PlanFingerprintReference(clock, config.queryPlanTTL, config.statsDivergenceThreshold, _)
    val planCacheFactory = () => new LFUCache[Statement, ExecutionPlan](config.queryCacheSize)
    monitors.addMonitorListener(logStalePlanRemovalMonitor(logger), monitorTag)
    val cacheMonitor = monitors.newMonitor[AstCacheMonitor](monitorTag)
    val cache = new MonitoringCacheAccessor[Statement, ExecutionPlan](cacheMonitor)

    CypherCompiler(procedurePlanProducer, rewriter, cache, planCacheFactory, cacheMonitor, monitors, rewriterSequencer, createFingerprintReference)
  }

  private def logStalePlanRemovalMonitor(log: InfoLogger) = new AstCacheMonitor {
    override def cacheDiscard(key: Statement, userKey: String) {
      log.info(s"Discarded stale query from the query cache: $userKey")
    }
  }
}
