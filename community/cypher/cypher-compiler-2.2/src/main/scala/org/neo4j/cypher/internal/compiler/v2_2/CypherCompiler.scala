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
package org.neo4j.cypher.internal.compiler.v2_2

import org.neo4j.cypher.internal.compiler.v2_2.ast.Statement
import org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters.{normalizeReturnClauses, normalizeWithClauses}
import org.neo4j.cypher.internal.compiler.v2_2.executionplan._
import org.neo4j.cypher.internal.compiler.v2_2.helpers.LRUCache
import org.neo4j.cypher.internal.compiler.v2_2.parser.{CypherParser, ParserMonitor}
import org.neo4j.cypher.internal.compiler.v2_2.planner._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.rewriter.LogicalPlanRewriter
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{CachedMetricsFactory, DefaultQueryPlanner, SimpleMetricsFactory}
import org.neo4j.cypher.internal.compiler.v2_2.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_2.tracing.rewriters.RewriterStepSequencer
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.helpers.Clock

trait SemanticCheckMonitor {
  def startSemanticCheck(query: String)

  def finishSemanticCheckSuccess(query: String)

  def finishSemanticCheckError(query: String, errors: Seq[SemanticError])
}

trait AstRewritingMonitor {
  def startRewriting(queryText: String, statement: Statement)

  def abortedRewriting(obj: AnyRef)

  def finishRewriting(queryText: String, statement: Statement)
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

object CypherCompilerFactory {
  val monitorTag = "cypher2.2"

  def costBasedCompiler(graph: GraphDatabaseService, queryCacheSize: Int, statsDivergenceThreshold: Double,
                        queryPlanTTL: Long, clock: Clock, monitors: Monitors,
                        logger: InfoLogger, plannerName: Option[CostBasedPlannerName],
                        rewriterSequencer: (String) => RewriterStepSequencer): CypherCompiler = {
    val parser = new CypherParser(monitors.newMonitor[ParserMonitor[Statement]](monitorTag))
    val checker = new SemanticChecker(monitors.newMonitor[SemanticCheckMonitor](monitorTag))
    val rewriter = new ASTRewriter(rewriterSequencer, monitors.newMonitor[AstRewritingMonitor](monitorTag))
    val planBuilderMonitor = monitors.newMonitor[NewLogicalPlanSuccessRateMonitor](monitorTag)
    val planningMonitor = monitors.newMonitor[PlanningMonitor](monitorTag)
    val metricsFactory = CachedMetricsFactory(SimpleMetricsFactory)
    val queryPlanner = new DefaultQueryPlanner(LogicalPlanRewriter(rewriterSequencer))

    val pickedPlannerName = plannerName.getOrElse(ConservativePlannerName)
    val planner = CostBasedPipeBuilderFactory(monitors, metricsFactory, planningMonitor, clock, queryPlanner = queryPlanner, rewriterSequencer = rewriterSequencer, plannerName = pickedPlannerName, semanticChecker = checker)
    // falling back to legacy planner is allowed only when no cost-based planner is picked explicitly (e.g., COST, IDP)
    val pipeBuilder = pickedPlannerName match {
      case ConservativePlannerName =>
        new LegacyVsNewPipeBuilder(new LegacyPipeBuilder(monitors, rewriterSequencer), planner, planBuilderMonitor)
      case _ =>
        new ErrorReportingPipeBuilder(planner)
    }

    val execPlanBuilder = new ExecutionPlanBuilder(graph, statsDivergenceThreshold, queryPlanTTL, clock, pipeBuilder)
    val planCacheFactory = () => new LRUCache[Statement, ExecutionPlan](queryCacheSize)
    monitors.addMonitorListener(logStalePlanRemovalMonitor(logger), monitorTag)
    val cacheMonitor = monitors.newMonitor[AstCacheMonitor](monitorTag)
    val cache = new MonitoringCacheAccessor[Statement, ExecutionPlan](cacheMonitor)

    new CypherCompiler(parser, checker, execPlanBuilder, rewriter, cache, planCacheFactory, cacheMonitor, monitors)
  }

  def ruleBasedCompiler(graph: GraphDatabaseService, queryCacheSize: Int, statsDivergenceThreshold: Double,
                        queryPlanTTL: Long, clock: Clock, monitors: Monitors,
                        rewriterSequencer: (String) => RewriterStepSequencer): CypherCompiler = {
    val parser = new CypherParser(monitors.newMonitor[ParserMonitor[ast.Statement]](monitorTag))
    val checker = new SemanticChecker(monitors.newMonitor[SemanticCheckMonitor](monitorTag))
    val rewriter = new ASTRewriter(rewriterSequencer, monitors.newMonitor[AstRewritingMonitor](monitorTag))
    val pipeBuilder = new LegacyPipeBuilder(monitors, rewriterSequencer)

    val execPlanBuilder = new ExecutionPlanBuilder(graph, statsDivergenceThreshold, queryPlanTTL, clock, pipeBuilder)
    val planCacheFactory = () => new LRUCache[Statement, ExecutionPlan](queryCacheSize)
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

case class RawQuery(rawStatement: String, pos: InputPosition)

case class CypherCompiler(parser: CypherParser,
                          semanticChecker: SemanticChecker,
                          executionPlanBuilder: ExecutionPlanBuilder,
                          astRewriter: ASTRewriter,
                          cacheAccessor: CacheAccessor[Statement, ExecutionPlan],
                          planCacheFactory: () => LRUCache[Statement, ExecutionPlan],
                          cacheMonitor: CypherCacheFlushingMonitor[CacheAccessor[Statement, ExecutionPlan]],
                          monitors: Monitors) {

  def planQuery(queryText: String, context: PlanContext, rawQuery: Option[RawQuery] = None): (ExecutionPlan, Map[String, Any]) =
    planPreparedQuery(prepareQuery(queryText, rawQuery), context)

  def prepareQuery(preparsedQueryText: String, rawQuery: Option[RawQuery]): PreparedQuery = {
    val parsedStatement = parser.parse(preparsedQueryText, rawQuery)

    val mkException = new SyntaxExceptionCreator(rawQuery)
    val cleanedStatement: Statement = parsedStatement.endoRewrite(inSequence(normalizeReturnClauses(mkException), normalizeWithClauses(mkException)))
    val originalSemanticState = semanticChecker.check(preparsedQueryText, cleanedStatement, mkException)

    val (rewrittenStatement, extractedParams, postConditions) = astRewriter.rewrite(preparsedQueryText, cleanedStatement, originalSemanticState)
    val postRewriteSemanticState = semanticChecker.check(preparsedQueryText, rewrittenStatement, mkException)

    val table = SemanticTable(types = postRewriteSemanticState.typeTable, recordedScopes = postRewriteSemanticState.recordedScopes)
    PreparedQuery(rewrittenStatement, preparsedQueryText, extractedParams)(table, postConditions, postRewriteSemanticState.scopeTree)
  }

  def planPreparedQuery(parsedQuery: PreparedQuery, context: PlanContext): (ExecutionPlan, Map[String, Any]) = {
    val cache = provideCache(cacheAccessor, cacheMonitor, context)
    val (executionPlan, _) = cache.getOrElseUpdate(parsedQuery.statement,
      plan => plan.isStale(context.txIdProvider, context.statistics), {
        executionPlanBuilder.build(context, parsedQuery)
      }
    )
    (executionPlan, parsedQuery.extractedParams)
  }

  private def provideCache(cacheAccessor: CacheAccessor[Statement, ExecutionPlan],
                           monitor: CypherCacheFlushingMonitor[CacheAccessor[Statement, ExecutionPlan]],
                           context: PlanContext) = {
    context.getOrCreateFromSchemaState(cacheAccessor, {
      monitor.cacheFlushDetected(cacheAccessor)
      val lruCache: LRUCache[Statement, ExecutionPlan] = planCacheFactory()
      new QueryCache[Statement, ExecutionPlan](cacheAccessor, lruCache)
    })
  }
}
