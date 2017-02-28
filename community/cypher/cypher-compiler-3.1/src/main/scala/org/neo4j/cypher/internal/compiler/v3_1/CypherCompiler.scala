/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.time.Clock

import org.neo4j.cypher.internal.compiler.v3_1.CompilationPhaseTracer.CompilationPhase.{AST_REWRITE, PARSING, SEMANTIC_CHECK}
import org.neo4j.cypher.internal.compiler.v3_1.ast.ResolvedCall
import org.neo4j.cypher.internal.compiler.v3_1.ast.rewriters.replaceAliasedFunctionInvocations.aliases
import org.neo4j.cypher.internal.compiler.v3_1.ast.rewriters.{expandCallWhere, normalizeReturnClauses, normalizeWithClauses, replaceAliasedFunctionInvocations}
import org.neo4j.cypher.internal.compiler.v3_1.codegen.CodeStructure
import org.neo4j.cypher.internal.compiler.v3_1.executionplan._
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.procs.DelegatingProcedureExecutablePlanBuilder
import org.neo4j.cypher.internal.compiler.v3_1.helpers.{RuntimeTypeConverter, closing}
import org.neo4j.cypher.internal.compiler.v3_1.planner._
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans.rewriter.LogicalPlanRewriter
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.{CachedMetricsFactory, DefaultQueryPlanner, SimpleMetricsFactory}
import org.neo4j.cypher.internal.compiler.v3_1.spi.{PlanContext, ProcedureSignature}
import org.neo4j.cypher.internal.compiler.v3_1.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_1.ast.{FunctionInvocation, FunctionName, Statement}
import org.neo4j.cypher.internal.frontend.v3_1.notification.{DeprecatedFunctionNotification, DeprecatedProcedureNotification, InternalNotification}
import org.neo4j.cypher.internal.frontend.v3_1.parser.CypherParser
import org.neo4j.cypher.internal.frontend.v3_1.{InputPosition, SemanticTable, inSequence}
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
                                       errorIfShortestPathHasCommonNodesAtRuntime: Boolean,
                                       legacyCsvQuoteEscaping: Boolean,
                                       nonIndexedLabelWarningThreshold: Long)

object CypherCompilerFactory {
  val monitorTag = "cypher3.1"

  def costBasedCompiler(graph: GraphDatabaseQueryService, config: CypherCompilerConfiguration, clock: Clock,
                        structure: CodeStructure[GeneratedQuery],
                        monitors: Monitors, logger: InfoLogger,
                        rewriterSequencer: (String) => RewriterStepSequencer,
                        plannerName: Option[CostBasedPlannerName],
                        runtimeName: Option[RuntimeName],
                        updateStrategy: Option[UpdateStrategy],
                        typeConverter: RuntimeTypeConverter): CypherCompiler = {
    val parser = new CypherParser
    val checker = new SemanticChecker
    val rewriter = new ASTRewriter(rewriterSequencer)
    val planBuilderMonitor = monitors.newMonitor[NewLogicalPlanSuccessRateMonitor](monitorTag)
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
      semanticChecker = checker,
      plannerName = plannerName,
      runtimeBuilder = runtimeBuilder,
      config = config,
      updateStrategy = updateStrategy,
      publicTypeConverter = typeConverter.asPublicType
    )
    val procedurePlanProducer = DelegatingProcedureExecutablePlanBuilder(costPlanProducer, typeConverter.asPublicType)
    val rulePlanProducer = new LegacyExecutablePlanBuilder(monitors, config, rewriterSequencer,
      typeConverter = typeConverter)

    // Pick planner based on input
    val planBuilder = ExecutablePlanBuilder.create(plannerName, rulePlanProducer,
                                                   procedurePlanProducer, planBuilderMonitor, config.useErrorsOverWarnings)

    val execPlanBuilder = new ExecutionPlanBuilder(graph,clock, planBuilder, new PlanFingerprintReference(clock, config.queryPlanTTL, config.statsDivergenceThreshold, _) )
    val planCacheFactory = () => new LFUCache[Statement, ExecutionPlan](config.queryCacheSize)
    monitors.addMonitorListener(logStalePlanRemovalMonitor(logger), monitorTag)
    val cacheMonitor = monitors.newMonitor[AstCacheMonitor](monitorTag)
    val cache = new MonitoringCacheAccessor[Statement, ExecutionPlan](cacheMonitor)

    CypherCompiler(parser, checker, execPlanBuilder, rewriter, cache, planCacheFactory, cacheMonitor, monitors)
  }

  def ruleBasedCompiler(graph: GraphDatabaseQueryService,
                        config: CypherCompilerConfiguration, clock: Clock, monitors: Monitors,
                        rewriterSequencer: (String) => RewriterStepSequencer,
                        typeConverter: RuntimeTypeConverter): CypherCompiler = {
    val parser = new CypherParser
    val checker = new SemanticChecker
    val rewriter = new ASTRewriter(rewriterSequencer)
    val pipeBuilder = DelegatingProcedureExecutablePlanBuilder(
      new LegacyExecutablePlanBuilder(monitors, config, rewriterSequencer,
        typeConverter = typeConverter), typeConverter.asPublicType)

    val execPlanBuilder = new ExecutionPlanBuilder(graph, clock, pipeBuilder, new PlanFingerprintReference(clock, config.queryPlanTTL, config.statsDivergenceThreshold, _))
    val planCacheFactory = () => new LFUCache[Statement, ExecutionPlan](config.queryCacheSize)
    val cacheMonitor = monitors.newMonitor[AstCacheMonitor](monitorTag)
    val cache = new MonitoringCacheAccessor[Statement, ExecutionPlan](cacheMonitor)

    CypherCompiler(parser, checker, execPlanBuilder, rewriter, cache, planCacheFactory, cacheMonitor, monitors)
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
                          planCacheFactory: () => LFUCache[Statement, ExecutionPlan],
                          cacheMonitor: CypherCacheFlushingMonitor[CacheAccessor[Statement, ExecutionPlan]],
                          monitors: Monitors) {

  def planQuery(queryText: String, context: PlanContext, notificationLogger: InternalNotificationLogger,
                plannerName: String = "",
                offset: Option[InputPosition] = None): (ExecutionPlan, Map[String, Any]) = {
    planPreparedQuery(prepareSyntacticQuery(queryText, queryText, notificationLogger, plannerName), notificationLogger, context, offset, CompilationPhaseTracer.NO_TRACING)
  }

  def planPreparedQuery(syntacticQuery: PreparedQuerySyntax, notificationLogger: InternalNotificationLogger,
                        context: PlanContext,
                        offset: Option[InputPosition] = None,
                        tracer: CompilationPhaseTracer): (ExecutionPlan, Map[String, Any]) = {
    val semanticQuery = prepareSemanticQuery(syntacticQuery, notificationLogger, context, offset, tracer)
    val cache = provideCache(cacheAccessor, cacheMonitor, context)
    val (executionPlan, _) = cache.getOrElseUpdate(syntacticQuery.statement, syntacticQuery.queryText,
      _.isStale (context.txIdProvider, context.statistics),
      executionPlanBuilder.build(context, semanticQuery, tracer)
    )
    (executionPlan, semanticQuery.extractedParams)
  }

  def prepareSyntacticQuery(queryText: String, rawQueryText: String, notificationLogger: InternalNotificationLogger,
                            plannerName: String = "",
                            offset: Option[InputPosition] = None,
                            tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING): PreparedQuerySyntax = {

    val parsedStatement = closing(tracer.beginPhase(PARSING)) {
      parser.parse(queryText, offset)
    }

    val syntaxDeprecations = syntaxDeprecationNotifications(parsedStatement)
    syntaxDeprecations.foreach(notificationLogger.log)

    val mkException = new SyntaxExceptionCreator(rawQueryText, offset)
    val cleanedStatement: Statement = parsedStatement.endoRewrite(inSequence(normalizeReturnClauses(mkException),
                                                                             normalizeWithClauses(mkException),
                                                                             expandCallWhere,
                                                                             replaceAliasedFunctionInvocations))
    val originalSemanticState = closing(tracer.beginPhase(SEMANTIC_CHECK)) {
      semanticChecker.check(queryText, cleanedStatement, mkException)
    }
    originalSemanticState.notifications.foreach(notificationLogger += _)

    val (rewrittenStatement, extractedParams, postConditions) = closing(tracer.beginPhase(AST_REWRITE)) {
      astRewriter.rewrite(queryText, cleanedStatement, originalSemanticState)
    }

    PreparedQuerySyntax(rewrittenStatement, queryText, offset, extractedParams)(plannerName, postConditions)
  }

  def prepareSemanticQuery(syntacticQuery: PreparedQuerySyntax, notificationLogger: InternalNotificationLogger,
                           context: PlanContext,
                           offset: Option[InputPosition] = None,
                           tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING): PreparedQuerySemantics = {

    val queryText = syntacticQuery.queryText

    val rewrittenSyntacticQuery = syntacticQuery.rewrite(rewriteProcedureCalls(context.procedureSignature, context.functionSignature))

    val procedureDeprecations = procedureDeprecationNotifications(rewrittenSyntacticQuery.statement)
    procedureDeprecations.foreach(notificationLogger.log)

    val mkException = new SyntaxExceptionCreator(queryText, offset)
    val postRewriteSemanticState = closing(tracer.beginPhase(SEMANTIC_CHECK)) {
      semanticChecker.check(syntacticQuery.queryText, rewrittenSyntacticQuery.statement, mkException)
    }

    val table = SemanticTable(types = postRewriteSemanticState.typeTable, recordedScopes = postRewriteSemanticState.recordedScopes)
    val result = rewrittenSyntacticQuery.withSemantics(table, postRewriteSemanticState.scopeTree)
    result
  }

  private def syntaxDeprecationNotifications(statement: Statement): Set[InternalNotification] =
    statement.treeFold(Set.empty[InternalNotification]) {
      case f@FunctionInvocation(_, FunctionName(name), _, _) if aliases.get(name).nonEmpty =>
        (seq) => (seq + DeprecatedFunctionNotification(f.position, name, aliases(name)), None)
    }

  private def procedureDeprecationNotifications(statement: Statement): Set[InternalNotification] =
    statement.treeFold(Set.empty[InternalNotification]) {
      case f@ResolvedCall(ProcedureSignature(name, _, _, Some(deprecatedBy), _, _), _, _, _, _) =>
        (seq) => (seq + DeprecatedProcedureNotification(f.position, name.toString, deprecatedBy), None)
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



