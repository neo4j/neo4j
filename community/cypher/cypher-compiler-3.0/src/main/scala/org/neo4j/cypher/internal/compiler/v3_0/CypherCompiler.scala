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
package org.neo4j.cypher.internal.compiler.v3_0

import org.neo4j.cypher.internal.compiler.v3_0.CompilationPhaseTracer.CompilationPhase.{AST_REWRITE, PARSING, SEMANTIC_CHECK}
import org.neo4j.cypher.internal.compiler.v3_0.ast.rewriters.{normalizeReturnClauses, normalizeWithClauses}
import org.neo4j.cypher.internal.compiler.v3_0.codegen.CodeStructure
import org.neo4j.cypher.internal.compiler.v3_0.executionplan._
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.procs.DelegatingProcedureExecutablePlanBuilder
import org.neo4j.cypher.internal.compiler.v3_0.helpers.closing
import org.neo4j.cypher.internal.compiler.v3_0.planner._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.rewriter.LogicalPlanRewriter
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.{CachedMetricsFactory, DefaultQueryPlanner, SimpleMetricsFactory}
import org.neo4j.cypher.internal.compiler.v3_0.spi.{PlanContext, ProcedureSignatureResolver}
import org.neo4j.cypher.internal.compiler.v3_0.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_0.ast.{ASTAnnotationMap, ResolvedCall, Statement, UnresolvedCall}
import org.neo4j.cypher.internal.frontend.v3_0.notification.InternalNotification
import org.neo4j.cypher.internal.frontend.v3_0.parser.CypherParser
import org.neo4j.cypher.internal.frontend.v3_0.{InputPosition, SemanticTable, inSequence, _}
import org.neo4j.helpers.Clock
import org.neo4j.kernel.GraphDatabaseQueryService

trait AstRewritingMonitor {
  def abortedRewriting(obj: AnyRef)
  def abortedRewritingDueToLargeDNF(obj: AnyRef)
}

trait CypherCacheFlushingMonitor[T] {
  def cacheFlushDetected(justBeforeKey: T) {}
}

trait CypherCacheHitMonitor[T] {
  def cacheHit(key: T) {}
  def cacheMiss(key: T) {}
  def cacheDiscard(key: T, userKey: String) {}
}

trait InfoLogger {
  def info(message: String)
}

trait CypherCacheMonitor[T, E] extends CypherCacheHitMonitor[T] with CypherCacheFlushingMonitor[E]

trait AstCacheMonitor extends CypherCacheMonitor[Statement, CacheAccessor[Statement, ExecutionPlan]]
case class CypherCompilerConfiguration(queryCacheSize: Int,
                                       statsDivergenceThreshold: Double,
                                       queryPlanTTL: Long,
                                       useErrorsOverWarnings: Boolean,
                                       idpMaxTableSize: Int,
                                       idpIterationDuration: Long,
                                       errorIfShortestPathFallbackUsedAtRuntime: Boolean,
                                       nonIndexedLabelWarningThreshold: Long)

object CypherCompilerFactory {
  val monitorTag = "cypher3.0"

  def costBasedCompiler(graph: GraphDatabaseQueryService, config: CypherCompilerConfiguration, clock: Clock, structure: CodeStructure[GeneratedQuery],
                        monitors: Monitors, logger: InfoLogger,
                        rewriterSequencer: (String) => RewriterStepSequencer,
                        plannerName: Option[CostBasedPlannerName],
                        runtimeName: Option[RuntimeName],
                        updateStrategy: Option[UpdateStrategy]): CypherCompiler = {
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
                        rewriterSequencer: (String) => RewriterStepSequencer): CypherCompiler = {
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

case class CypherCompiler(parser: CypherParser,
                          semanticChecker: SemanticChecker,
                          executionPlanBuilder: ExecutionPlanBuilder,
                          astRewriter: ASTRewriter,
                          cacheAccessor: CacheAccessor[Statement, ExecutionPlan],
                          planCacheFactory: () => LRUCache[Statement, ExecutionPlan],
                          cacheMonitor: CypherCacheFlushingMonitor[CacheAccessor[Statement, ExecutionPlan]],
                          monitors: Monitors) {

  def planQuery(queryText: String, context: PlanContext, notificationLogger: InternalNotificationLogger,
                plannerName: String = "",
                offset: Option[InputPosition] = None): (ExecutionPlan, Map[String, Any]) =
    planPreparedQuery(prepareQuery(queryText, queryText, notificationLogger, plannerName), context, CompilationPhaseTracer.NO_TRACING)


  def prepareQuery(queryText: String, rawQueryText: String, notificationLogger: InternalNotificationLogger,
                   plannerName: String = "",
                   offset: Option[InputPosition] = None,
                   tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING): PreparedQuery = {

    val parsedStatement = closing(tracer.beginPhase(PARSING)) {
      parser.parse(queryText, offset)
    }

    val syntaxDeprecations = syntaxDeprecationNotifications(parsedStatement)
    syntaxDeprecations.foreach(notificationLogger.log)

    val mkException = new SyntaxExceptionCreator(rawQueryText, offset)
    val cleanedStatement: Statement = parsedStatement.endoRewrite(inSequence(normalizeReturnClauses(mkException),
                                                                             normalizeWithClauses(mkException)))
    val originalSemanticState = closing(tracer.beginPhase(SEMANTIC_CHECK)) {
      semanticChecker.check(queryText, cleanedStatement, mkException)
    }
    originalSemanticState.notifications.foreach(notificationLogger += _)

    val (rewrittenStatement, extractedParams, postConditions) = closing(tracer.beginPhase(AST_REWRITE)) {
      astRewriter.rewrite(queryText, cleanedStatement, originalSemanticState)
    }
    val postRewriteSemanticState = closing(tracer.beginPhase(SEMANTIC_CHECK)) {
      semanticChecker.check(queryText, rewrittenStatement, mkException)
    }

    val table = SemanticTable(types = postRewriteSemanticState.typeTable, recordedScopes = postRewriteSemanticState.recordedScopes)
    PreparedQuery(rewrittenStatement, queryText, extractedParams)(table, postConditions, postRewriteSemanticState.scopeTree, notificationLogger, plannerName, offset)
  }

  def planPreparedQuery(parsedQuery: PreparedQuery, context: PlanContext, tracer: CompilationPhaseTracer):
  (ExecutionPlan, Map[String, Any]) = {
    val cache = provideCache(cacheAccessor, cacheMonitor, context)
    val (executionPlan, _) = cache.getOrElseUpdate(parsedQuery.statement, parsedQuery.queryText,
      plan => plan.isStale(context.txIdProvider, context.statistics), {
        executionPlanBuilder.build(context, resolveCallSignatures(parsedQuery, context), tracer)
      }
    )
    (executionPlan, parsedQuery.extractedParams)
  }

  // TODO: Break out
  private def resolveCallSignatures(query: PreparedQuery, signatureResolver: ProcedureSignatureResolver): PreparedQuery = {
    // Build resolved replacement calls
    val rewrites = query.statement.treeFold(ASTAnnotationMap.empty[UnresolvedCall, ResolvedCall]) {
      case unresolved@UnresolvedCall(call) =>
        val signature = signatureResolver.procedureSignature(call.procedureName)
        val resolved = unresolved.resolve(signature)
        (acc) => acc.updated(unresolved, resolved) -> None
    }

    // Rewriter for updating the statement to use resolved calls
    val rewriter = Rewriter.lift {
      case unresolved@UnresolvedCall(call) =>
        rewrites.get(unresolved).get
    }

    // Do it all
    val result = query.rewrite(bottomUp(rewriter), _.replaceNodes(rewrites.toSeq: _*))
    result
  }

  private def syntaxDeprecationNotifications(statement: Statement) =
  // We don't have any deprecations in 3.0 yet
    Seq.empty[InternalNotification]

  private def provideCache(cacheAccessor: CacheAccessor[Statement, ExecutionPlan],
                           monitor: CypherCacheFlushingMonitor[CacheAccessor[Statement, ExecutionPlan]],
                           context: PlanContext) =
    context.getOrCreateFromSchemaState(cacheAccessor, {
      monitor.cacheFlushDetected(cacheAccessor)
      val lRUCache = planCacheFactory()
      new QueryCache(cacheAccessor, lRUCache)
    })
}
