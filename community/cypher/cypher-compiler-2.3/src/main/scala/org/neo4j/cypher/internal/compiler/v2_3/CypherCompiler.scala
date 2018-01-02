/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.internal.compiler.v2_3.CompilationPhaseTracer.CompilationPhase._
import org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters.{normalizeReturnClauses, normalizeWithClauses}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan._
import org.neo4j.cypher.internal.compiler.v2_3.helpers.closing
import org.neo4j.cypher.internal.compiler.v2_3.planner._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.rewriter.LogicalPlanRewriter
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{CachedMetricsFactory, DefaultQueryPlanner, SimpleMetricsFactory}
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v2_3.ast.{NodePattern, Statement}
import org.neo4j.cypher.internal.frontend.v2_3.notification.{BareNodeSyntaxDeprecatedNotification, InternalNotification}
import org.neo4j.cypher.internal.frontend.v2_3.parser.CypherParser
import org.neo4j.cypher.internal.frontend.v2_3.{InputPosition, SemanticTable, inSequence}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.helpers.Clock

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
  def cacheDiscard(key: T) {}
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
                                       nonIndexedLabelWarningThreshold: Long)

object CypherCompilerFactory {
  val monitorTag = "cypher2.3"

  def costBasedCompiler(graph: GraphDatabaseService, entityAccessor: EntityAccessor,
                        config: CypherCompilerConfiguration, clock: Clock,
                        monitors: Monitors, logger: InfoLogger,
                        rewriterSequencer: (String) => RewriterStepSequencer,
                        plannerName: Option[CostBasedPlannerName],
                        runtimeName: Option[RuntimeName]): CypherCompiler = {
    val parser = new CypherParser
    val checker = new SemanticChecker
    val rewriter = new ASTRewriter(rewriterSequencer)
    val planBuilderMonitor = monitors.newMonitor[NewLogicalPlanSuccessRateMonitor](monitorTag)
    val metricsFactory = CachedMetricsFactory(SimpleMetricsFactory)
    val queryPlanner = new DefaultQueryPlanner(LogicalPlanRewriter(rewriterSequencer))

    val interpretedPlanBuilder = InterpretedPlanBuilder(clock, monitors)

    // Pick runtime based on input
    val runtimeBuilder = RuntimeBuilder.create(runtimeName, interpretedPlanBuilder)
    val costPlanProducer = CostBasedPipeBuilderFactory.create(
      monitors = monitors,
      metricsFactory = metricsFactory,
      queryPlanner = queryPlanner,
      rewriterSequencer = rewriterSequencer,
      plannerName = plannerName,
      runtimeBuilder = runtimeBuilder,
      semanticChecker = checker,
      useErrorsOverWarnings = config.useErrorsOverWarnings,
      idpMaxTableSize = config.idpMaxTableSize,
      idpIterationDuration = config.idpIterationDuration
    )
    val rulePlanProducer = new LegacyExecutablePlanBuilder(monitors, rewriterSequencer)

    // Pick planner based on input
    val planBuilder = ExecutablePlanBuilder.create(plannerName, rulePlanProducer,
                                                   costPlanProducer, planBuilderMonitor, config.useErrorsOverWarnings)

    val execPlanBuilder = new ExecutionPlanBuilder(graph, entityAccessor, config, clock, planBuilder)
    val planCacheFactory = () => new LRUCache[Statement, ExecutionPlan](config.queryCacheSize)
    monitors.addMonitorListener(logStalePlanRemovalMonitor(logger), monitorTag)
    val cacheMonitor = monitors.newMonitor[AstCacheMonitor](monitorTag)
    val cache = new MonitoringCacheAccessor[Statement, ExecutionPlan](cacheMonitor)

    new CypherCompiler(parser, checker, execPlanBuilder, rewriter, cache, planCacheFactory, cacheMonitor, monitors)
  }

  def ruleBasedCompiler(graph: GraphDatabaseService, entityAccessor: EntityAccessor,
                        config: CypherCompilerConfiguration, clock: Clock, monitors: Monitors,
                        rewriterSequencer: (String) => RewriterStepSequencer): CypherCompiler = {
    val parser = new CypherParser
    val checker = new SemanticChecker
    val rewriter = new ASTRewriter(rewriterSequencer)
    val pipeBuilder = new LegacyExecutablePlanBuilder(monitors, rewriterSequencer)

    val execPlanBuilder = new ExecutionPlanBuilder(graph, entityAccessor, config, clock, pipeBuilder)
    val planCacheFactory = () => new LRUCache[Statement, ExecutionPlan](config.queryCacheSize)
    val cacheMonitor = monitors.newMonitor[AstCacheMonitor](monitorTag)
    val cache = new MonitoringCacheAccessor[Statement, ExecutionPlan](cacheMonitor)

    new CypherCompiler(parser, checker, execPlanBuilder, rewriter, cache, planCacheFactory, cacheMonitor, monitors)
  }

  private def logStalePlanRemovalMonitor(log: InfoLogger) = new AstCacheMonitor {
    override def cacheDiscard(key: Statement) {
      log.info(s"Discarded stale query from the query cache: $key")
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
    PreparedQuery(rewrittenStatement, queryText, extractedParams)(table, postConditions, postRewriteSemanticState.scopeTree, notificationLogger, plannerName)
  }

  def planPreparedQuery(parsedQuery: PreparedQuery, context: PlanContext, tracer: CompilationPhaseTracer):
  (ExecutionPlan, Map[String, Any]) = {
    val cache = provideCache(cacheAccessor, cacheMonitor, context)
    val (executionPlan, _) = cache.getOrElseUpdate(parsedQuery.statement,
      plan => plan.isStale(context.txIdProvider, context.statistics), {
        executionPlanBuilder.build(context, parsedQuery, tracer)
      }
    )
    (executionPlan, parsedQuery.extractedParams)
  }

  private def syntaxDeprecationNotifications(statement: Statement) =
    statement.treeFold(Seq.empty[InternalNotification]) {
      case pat: NodePattern if pat.naked =>
        (acc, children) =>
          children(acc :+ BareNodeSyntaxDeprecatedNotification(pat.position))
    }

  private def provideCache(cacheAccessor: CacheAccessor[Statement, ExecutionPlan],
                           monitor: CypherCacheFlushingMonitor[CacheAccessor[Statement, ExecutionPlan]],
                           context: PlanContext) =
    context.getOrCreateFromSchemaState(cacheAccessor, {
      monitor.cacheFlushDetected(cacheAccessor)
      val lRUCache = planCacheFactory()
      new QueryCache(cacheAccessor, lRUCache)
    })
}
