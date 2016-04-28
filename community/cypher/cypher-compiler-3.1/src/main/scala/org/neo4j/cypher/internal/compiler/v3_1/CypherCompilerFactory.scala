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
package org.neo4j.cypher.internal.compiler.v3_1

import org.neo4j.cypher.internal.compiler.v3_1.codegen.CodeStructure
import org.neo4j.cypher.internal.compiler.v3_1.executionplan._
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.procs.DelegatingProcedureExecutablePlanBuilder
import org.neo4j.cypher.internal.compiler.v3_1.planner._
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans.rewriter.LogicalPlanRewriter
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.{CachedMetricsFactory, DefaultQueryPlanner, SimpleMetricsFactory}
import org.neo4j.cypher.internal.compiler.v3_1.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_1.ast.Statement
import org.neo4j.cypher.internal.frontend.v3_1.parser.CypherParser
import org.neo4j.helpers.Clock
import org.neo4j.kernel.GraphDatabaseQueryService

object CypherCompilerFactory {
  val monitorTag = "cypher3.1"

  def costBasedCompiler(graph: GraphDatabaseQueryService, config: CypherCompilerConfiguration, clock: Clock, structure: CodeStructure[GeneratedQuery],
                        monitors: Monitors, logger: InfoLogger,
                        rewriterSequencer: (String) => RewriterStepSequencer,
                        plannerName: Option[CostBasedPlannerName],
                        runtimeName: Option[RuntimeName],
                        updateStrategy: Option[UpdateStrategy]): CompilationOrchestrator = {
    val parser = new CypherParser
    val checker = new SemanticChecker
    val rewriter = new ASTRewriter(rewriterSequencer)
    val planBuilderMonitor = monitors.newMonitor[NewLogicalPlanSuccessRateMonitor](monitorTag)
    val metricsFactory = CachedMetricsFactory(SimpleMetricsFactory)
    val queryPlanner = new DefaultQueryPlanner(LogicalPlanRewriter(rewriterSequencer))

    val compiledPlanBuilder = CompiledPlanBuilder(clock, structure)
    val interpretedPlanBuilder = InterpretedPlanBuilder(clock, monitors)

    // Pick runtime based on input
    val runtimeBuilder = RuntimeBuilder.create(runtimeName, interpretedPlanBuilder, compiledPlanBuilder, config.useErrorsOverWarnings)

    val costPlanProducer = CostBasedPipeBuilderFactory.create(
      monitors = monitors,
      metricsFactory = metricsFactory,
      queryPlanner = queryPlanner,
      rewriterSequencer = rewriterSequencer,
      semanticChecker = checker,
      plannerName = plannerName,
      runtimeBuilder = runtimeBuilder,
      config = config,
      updateStrategy = updateStrategy
    )
    val procedurePlanProducer = new DelegatingProcedureExecutablePlanBuilder(costPlanProducer)
    val rulePlanProducer = new LegacyExecutablePlanBuilder(monitors, config, rewriterSequencer)

    // Pick planner based on input
    val planBuilder = ExecutablePlanBuilder.create(plannerName, rulePlanProducer,
                                                   procedurePlanProducer, planBuilderMonitor, config.useErrorsOverWarnings)

    val execPlanBuilder = new ExecutionPlanBuilder(graph,clock, planBuilder, PlanFingerprintReference(clock, config.queryPlanTTL, config.statsDivergenceThreshold, _) )
    val planCacheFactory = () => new LRUCache[Statement, ExecutionPlan](config.queryCacheSize)
    monitors.addMonitorListener(logStalePlanRemovalMonitor(logger), monitorTag)
    val cacheMonitor = monitors.newMonitor[AstCacheMonitor](monitorTag)
    val cache = new MonitoringCacheAccessor[Statement, ExecutionPlan](cacheMonitor)

    new CypherCompiler(parser, checker, execPlanBuilder, rewriter, cache, planCacheFactory, cacheMonitor, monitors)
  }

  def ruleBasedCompiler(graph: GraphDatabaseQueryService,
                        config: CypherCompilerConfiguration, clock: Clock, monitors: Monitors,
                        rewriterSequencer: (String) => RewriterStepSequencer): CompilationOrchestrator = {
    val parser = new CypherParser
    val checker = new SemanticChecker
    val rewriter = new ASTRewriter(rewriterSequencer)
    val pipeBuilder = new DelegatingProcedureExecutablePlanBuilder(new LegacyExecutablePlanBuilder(monitors, config, rewriterSequencer))

    val execPlanBuilder = new ExecutionPlanBuilder(graph, clock, pipeBuilder, PlanFingerprintReference(clock, config.queryPlanTTL, config.statsDivergenceThreshold, _))
    val planCacheFactory = () => new LRUCache[Statement, ExecutionPlan](config.queryCacheSize)
    val cacheMonitor = monitors.newMonitor[AstCacheMonitor](monitorTag)
    val cache = new MonitoringCacheAccessor[Statement, ExecutionPlan](cacheMonitor)

    new CypherCompiler(parser, checker, execPlanBuilder, rewriter, cache, planCacheFactory, cacheMonitor, monitors)
  }

  private def logStalePlanRemovalMonitor(log: InfoLogger) = new AstCacheMonitor {
    override def cacheDiscard(key: Statement, userKey: String) {
      log.info(s"Discarded stale query from the query cache: $userKey")
    }
  }
}
