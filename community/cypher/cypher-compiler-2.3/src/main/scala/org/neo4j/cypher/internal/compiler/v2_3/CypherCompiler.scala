/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v2_3.CompilationPhaseTracer.CompilationPhase.{AST_REWRITE, PARSING, SEMANTIC_CHECK}
import org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters.{normalizeReturnClauses, normalizeWithClauses}
import org.neo4j.cypher.internal.compiler.v2_3.codegen.CodeStructure
import org.neo4j.cypher.internal.compiler.v2_3.executionplan._
import org.neo4j.cypher.internal.compiler.v2_3.helpers.closing
import org.neo4j.cypher.internal.compiler.v2_3.planner._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.rewriter.LogicalPlanRewriter
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{CachedMetricsFactory, DefaultQueryPlanner, SimpleMetricsFactory}
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v2_3.ast.{LabelName, NodePattern, Statement}
import org.neo4j.cypher.internal.frontend.v2_3.notification.{BareNodeSyntaxDeprecatedNotification, InternalNotification}
import org.neo4j.cypher.internal.frontend.v2_3.parser.CypherParser
import org.neo4j.cypher.internal.frontend.v2_3.{InputPosition, SemanticTable, inSequence}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.helpers.Clock

trait AstRewritingMonitor {
  def abortedRewriting(obj: AnyRef)
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
                                       nonIndexedLabelWarningThreshold: Long)

object CypherCompilerFactory {
  val monitorTag = "cypher2.3"

  def costBasedCompiler(graph: GraphDatabaseService, config: CypherCompilerConfiguration,
                        clock: Clock, structure: CodeStructure[GeneratedQuery], monitors: Monitors,
                        logger: InfoLogger,
                        rewriterSequencer: (String) => RewriterStepSequencer,
                        plannerName: Option[CostBasedPlannerName],
                        runtimeName: Option[RuntimeName]): CypherCompiler = {
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
      plannerName = plannerName,
      runtimeBuilder = runtimeBuilder,
      semanticChecker = checker,
      useErrorsOverWarnings = config.useErrorsOverWarnings
    )
    val rulePlanProducer = new LegacyExecutablePlanBuilder(monitors, rewriterSequencer)

    // Pick planner based on input
    val planBuilder = ExecutablePlanBuilder.create(plannerName, rulePlanProducer,
                                                   costPlanProducer, planBuilderMonitor, config.useErrorsOverWarnings)

    val execPlanBuilder = new ExecutionPlanBuilder(graph, config, clock, planBuilder)
    val planCacheFactory = () => new LRUCache[Statement, ExecutionPlan](config.queryCacheSize)
    monitors.addMonitorListener(logStalePlanRemovalMonitor(logger), monitorTag)
    val cacheMonitor = monitors.newMonitor[AstCacheMonitor](monitorTag)
    val cache = new MonitoringCacheAccessor[Statement, ExecutionPlan](cacheMonitor)

    new CypherCompiler(parser, checker, execPlanBuilder, rewriter, cache, planCacheFactory, cacheMonitor, monitors)
  }

  def ruleBasedCompiler(graph: GraphDatabaseService, config: CypherCompilerConfiguration, clock: Clock, monitors: Monitors,
                        rewriterSequencer: (String) => RewriterStepSequencer): CypherCompiler = {
    val parser = new CypherParser
    val checker = new SemanticChecker
    val rewriter = new ASTRewriter(rewriterSequencer)
    val pipeBuilder = new LegacyExecutablePlanBuilder(monitors, rewriterSequencer)

    val execPlanBuilder = new ExecutionPlanBuilder(graph, config, clock, pipeBuilder)
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
                offset: Option[InputPosition] = None): (ExecutionPlan, Map[String, Any]) =
    planPreparedQuery(prepareQuery(queryText, queryText, notificationLogger), context, CompilationPhaseTracer.NO_TRACING)


  def prepareQuery(queryText: String, rawQueryText: String, notificationLogger: InternalNotificationLogger,
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
    PreparedQuery(rewrittenStatement, queryText, extractedParams)(table, postConditions, postRewriteSemanticState.scopeTree, notificationLogger)
  }

  def planPreparedQuery(parsedQuery: PreparedQuery, context: PlanContext,
                        tracer: CompilationPhaseTracer): (ExecutionPlan, Map[String, Any]) = {
    val cache = provideCache(cacheAccessor, cacheMonitor, context)
    var planned = false
    val plan = Iterator.continually {
      cacheAccessor.getOrElseUpdate(cache)(parsedQuery.statement, {
        planned = true
        executionPlanBuilder.build(context, parsedQuery, tracer)
      })
    }.flatMap { plan =>
      if (!planned && plan.isStale(context.txIdProvider, context.statistics)) {
        cacheAccessor.remove(cache)(parsedQuery.statement)
        None
      } else {
        Some(plan)
      }
    }.next()
    (plan, parsedQuery.extractedParams)
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
      planCacheFactory()
    })
}
