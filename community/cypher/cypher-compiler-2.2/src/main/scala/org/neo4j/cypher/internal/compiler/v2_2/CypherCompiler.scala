/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v2_2.parser.{CypherParser, ParserMonitor}
import org.neo4j.cypher.internal.compiler.v2_2.planner._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{CachedMetricsFactory, SimpleMetricsFactory}
import org.neo4j.cypher.internal.compiler.v2_2.spi.PlanContext
import org.neo4j.cypher.internal.{LRUCache, PlanType}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.helpers.Clock
import org.neo4j.kernel.impl.util.StringLogger
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}

trait SemanticCheckMonitor {
  def startSemanticCheck(query: String)
  def finishSemanticCheckSuccess(query: String)
  def finishSemanticCheckError(query: String, errors: Seq[SemanticError])
}

trait AstRewritingMonitor {
  def startRewriting(queryText: String, statement: Statement)
  def finishRewriting(queryText: String, statement: Statement)
}

trait CypherCacheFlushingMonitor[T] {
  def cacheFlushDetected(justBeforeKey: T){}
}

trait CypherCacheHitMonitor[T] {
  def cacheHit(key: T){}
  def cacheMiss(key: T){}
  def cacheDiscard(key: T){}
}

trait CypherCacheMonitor[T, E] extends CypherCacheHitMonitor[T] with CypherCacheFlushingMonitor[E]

trait AstCacheMonitor extends CypherCacheMonitor[PreparedQuery, CacheAccessor[PreparedQuery, ExecutionPlan]]

object CypherCompilerFactory {
  val monitorTag = "cypher2.2"

  def ronjaCompiler(graph: GraphDatabaseService, queryCacheSize: Int, statsDivergenceThreshold: Double,
                    queryPlanTTL: Long, clock: Clock, kernelMonitors: KernelMonitors,
                    logger: StringLogger): CypherCompiler = {
    val monitors = new Monitors(kernelMonitors)
    val parser = new CypherParser(monitors.newMonitor[ParserMonitor[Statement]](monitorTag))
    val checker = new SemanticChecker(monitors.newMonitor[SemanticCheckMonitor](monitorTag))
    val rewriter = new ASTRewriter(monitors.newMonitor[AstRewritingMonitor](monitorTag))
    val planBuilderMonitor = monitors.newMonitor[NewLogicalPlanSuccessRateMonitor](monitorTag)
    val planningMonitor = monitors.newMonitor[PlanningMonitor](monitorTag)
    val metricsFactory = CachedMetricsFactory(SimpleMetricsFactory)
    val planner = new Planner(monitors, metricsFactory, planningMonitor, clock)
    val pipeBuilder = new LegacyVsNewPipeBuilder(new LegacyPipeBuilder(monitors), planner, planBuilderMonitor)
    val execPlanBuilder = new ExecutionPlanBuilder(graph, statsDivergenceThreshold, queryPlanTTL, clock, pipeBuilder)
    val planCacheFactory = () => new LRUCache[PreparedQuery, ExecutionPlan](queryCacheSize)
    val cacheMonitor = monitors.newMonitor[AstCacheMonitor](monitorTag)
    monitors.addMonitorListener(logStalePlanRemovalMonitor(logger), monitorTag)
    val cache = new MonitoringCacheAccessor[PreparedQuery, ExecutionPlan](cacheMonitor)

    new CypherCompiler(parser, checker, execPlanBuilder, rewriter, cache, planCacheFactory, cacheMonitor, monitors)
  }

  private def logStalePlanRemovalMonitor(logger: StringLogger) = new AstCacheMonitor {
    override def cacheDiscard(key: PreparedQuery) {
      logger.info(s"Discarded stale query from the query cache: ${key.queryText}")
    }
  }

  def legacyCompiler(graph: GraphDatabaseService, queryCacheSize: Int, statsDivergenceThreshold: Double,
                     queryPlanTTL: Long, clock: Clock, kernelMonitors: KernelMonitors): CypherCompiler = {
    val monitors = new Monitors(kernelMonitors)
    val parser = new CypherParser(monitors.newMonitor[ParserMonitor[ast.Statement]](monitorTag))
    val checker = new SemanticChecker(monitors.newMonitor[SemanticCheckMonitor](monitorTag))
    val rewriter = new ASTRewriter(monitors.newMonitor[AstRewritingMonitor](monitorTag))
    val pipeBuilder = new LegacyPipeBuilder(monitors)
    val execPlanBuilder = new ExecutionPlanBuilder(graph, statsDivergenceThreshold, queryPlanTTL, clock, pipeBuilder)
    val planCacheFactory = () => new LRUCache[PreparedQuery, ExecutionPlan](queryCacheSize)
    val cacheMonitor = monitors.newMonitor[AstCacheMonitor](monitorTag)
    val cache = new MonitoringCacheAccessor[PreparedQuery, ExecutionPlan](cacheMonitor)

    new CypherCompiler(parser, checker, execPlanBuilder, rewriter, cache, planCacheFactory, cacheMonitor, monitors)
  }
}

case class CypherCompiler(parser: CypherParser,
                          semanticChecker: SemanticChecker,
                          executionPlanBuilder: ExecutionPlanBuilder,
                          astRewriter: ASTRewriter,
                          cacheAccessor: CacheAccessor[PreparedQuery, ExecutionPlan],
                          planCacheFactory: () => LRUCache[PreparedQuery, ExecutionPlan],
                          cacheMonitor: CypherCacheFlushingMonitor[CacheAccessor[PreparedQuery, ExecutionPlan]],
                          monitors: Monitors) {

  def planQuery(queryText: String, context: PlanContext): (ExecutionPlan, Map[String, Any]) =
    planPreparedQuery(prepareQuery(queryText), context)

  def prepareQuery(queryText: String): PreparedQuery = {
    val parsedStatement = parser.parse(queryText)

    val cleanedStatement: Statement = parsedStatement.endoRewrite(inSequence(normalizeReturnClauses, normalizeWithClauses))
    val originalSemanticState = semanticChecker.check(queryText, cleanedStatement)

    val (rewrittenStatement, extractedParams) = astRewriter.rewrite(queryText, cleanedStatement, originalSemanticState)
    val postRewriteSemanticState = semanticChecker.check(queryText, rewrittenStatement)

    val table = SemanticTable(types = postRewriteSemanticState.typeTable)
    PreparedQuery(rewrittenStatement, queryText, extractedParams)(table, postRewriteSemanticState.scopeTree)
  }

  def planPreparedQuery(parsedQuery: PreparedQuery, context: PlanContext): (ExecutionPlan, Map[String, Any]) = {
    val cache = provideCache(cacheAccessor, cacheMonitor, context)
    var planned = false
    val plan = Iterator.continually {
      cacheAccessor.getOrElseUpdate(cache)(parsedQuery, {
        planned = true
        executionPlanBuilder.build(context, parsedQuery)
      })
    }.flatMap { plan =>
      if ( !planned && plan.isStale(context.txIdProvider, context.statistics) ) {
        cacheAccessor.remove(cache)(parsedQuery)
        None
      } else {
        Some(plan)
      }
    }.next()
    (plan, parsedQuery.extractedParams)
  }

  private def provideCache(cacheAccessor: CacheAccessor[PreparedQuery, ExecutionPlan],
                           monitor: CypherCacheFlushingMonitor[CacheAccessor[PreparedQuery, ExecutionPlan]],
                           context: PlanContext) =
    context.getOrCreateFromSchemaState(cacheAccessor, {
      monitor.cacheFlushDetected(cacheAccessor)
      planCacheFactory()
    })
}
