/**
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
package org.neo4j.cypher.internal.compiler.v2_1

import org.neo4j.cypher.internal.LRUCache
import org.neo4j.cypher.internal.compiler.v2_1.ast.Statement
import org.neo4j.cypher.internal.compiler.v2_1.ast.convert.StatementConverters._
import org.neo4j.cypher.internal.compiler.v2_1.commands.AbstractQuery
import org.neo4j.cypher.internal.compiler.v2_1.executionplan._
import org.neo4j.cypher.internal.compiler.v2_1.parser.{CypherParser, ParserMonitor}
import org.neo4j.cypher.internal.compiler.v2_1.planner.{Planner, PlanningMonitor}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{CachedMetricsFactory, SimpleMetricsFactory}
import org.neo4j.cypher.internal.compiler.v2_1.spi.PlanContext
import org.neo4j.graphdb.GraphDatabaseService
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
  def cacheFlushDetected(justBeforeKey: T)
}

trait CypherCacheHitMonitor[T] {
  def cacheHit(key: T)
  def cacheMiss(key: T)
}

trait CypherCacheMonitor[T, E] extends CypherCacheHitMonitor[T] with CypherCacheFlushingMonitor[E]

trait AstCacheMonitor extends CypherCacheMonitor[Statement, CacheAccessor[Statement, ExecutionPlan]]

object CypherCompilerFactory {
  val monitorTag = "cypher2.1"

  def ronjaCompiler(graph: GraphDatabaseService, queryCacheSize: Int, kernelMonitors: KernelMonitors): CypherCompiler = {
    val monitors = new Monitors(kernelMonitors)
    val parser = new CypherParser(monitors.newMonitor[ParserMonitor[ast.Statement]](monitorTag))
    val checker = new SemanticChecker(monitors.newMonitor[SemanticCheckMonitor](monitorTag))
    val rewriter = new ASTRewriter(monitors.newMonitor[AstRewritingMonitor](monitorTag))
    val planBuilderMonitor = monitors.newMonitor[NewQueryPlanSuccessRateMonitor](monitorTag)
    val planningMonitor = monitors.newMonitor[PlanningMonitor](monitorTag)
    val metricsFactory = CachedMetricsFactory(SimpleMetricsFactory)
    val planner = new Planner(monitors, metricsFactory, planningMonitor)
    val pipeBuilder = new LegacyVsNewPipeBuilder(new LegacyPipeBuilder(monitors), planner, planBuilderMonitor)
    val execPlanBuilder = new ExecutionPlanBuilder(graph, pipeBuilder)
    val planCacheFactory = () => new LRUCache[ast.Statement, ExecutionPlan](queryCacheSize)
    val cacheMonitor = monitors.newMonitor[AstCacheMonitor](monitorTag)
    val cache = new MonitoringCacheAccessor[ast.Statement, ExecutionPlan](cacheMonitor)

    new CypherCompiler(parser, checker, execPlanBuilder, rewriter, cache, planCacheFactory, cacheMonitor, monitors)
  }

  def legacyCompiler(graph: GraphDatabaseService, queryCacheSize: Int, kernelMonitors: KernelMonitors): CypherCompiler = {
    val monitors = new Monitors(kernelMonitors)
    val parser = new CypherParser(monitors.newMonitor[ParserMonitor[ast.Statement]](monitorTag))
    val checker = new SemanticChecker(monitors.newMonitor[SemanticCheckMonitor](monitorTag))
    val rewriter = new ASTRewriter(monitors.newMonitor[AstRewritingMonitor](monitorTag))
    val pipeBuilder = new LegacyPipeBuilder(monitors)
    val execPlanBuilder = new ExecutionPlanBuilder(graph, pipeBuilder)
    val planCacheFactory = () => new LRUCache[ast.Statement, ExecutionPlan](queryCacheSize)
    val cacheMonitor = monitors.newMonitor[AstCacheMonitor](monitorTag)
    val cache = new MonitoringCacheAccessor[ast.Statement, ExecutionPlan](cacheMonitor)

    new CypherCompiler(parser, checker, execPlanBuilder, rewriter, cache, planCacheFactory, cacheMonitor, monitors)
  }
}

case class CypherCompiler(parser: CypherParser,
                          semanticChecker: SemanticChecker,
                          executionPlanBuilder: ExecutionPlanBuilder,
                          astRewriter: ASTRewriter,
                          cacheAccessor: CacheAccessor[Statement, ExecutionPlan],
                          planCacheFactory: () => LRUCache[ast.Statement, ExecutionPlan],
                          cacheMonitor: CypherCacheFlushingMonitor[CacheAccessor[ast.Statement, ExecutionPlan]],
                          monitors: Monitors) {

  def planQuery(queryText: String, context: PlanContext): (ExecutionPlan, Map[String, Any]) =
    planPreparedQuery(prepareQuery(queryText), context)

  def prepareQuery(queryText: String): PreparedQuery = {
    val parsedStatement = parser.parse(queryText)
    semanticChecker.check(queryText, parsedStatement)
    val (rewrittenStatement, extractedParams) = astRewriter.rewrite(queryText, parsedStatement)
    val table = semanticChecker.check(queryText, parsedStatement)
    val query: AbstractQuery = rewrittenStatement.asQuery.setQueryText(queryText)
    PreparedQuery(rewrittenStatement, query, table, queryText, extractedParams)
  }

  def planPreparedQuery(parsedQuery: PreparedQuery, context: PlanContext): (ExecutionPlan, Map[String, Any]) = {
    val cache = provideCache(cacheAccessor, cacheMonitor, context)
    val plan = cacheAccessor.getOrElseUpdate(cache)(parsedQuery.statement, {
      executionPlanBuilder.build(context, parsedQuery)
    })
    (plan, parsedQuery.extractedParams)
  }

  private def provideCache(cacheAccessor: CacheAccessor[Statement, ExecutionPlan],
                           monitor: CypherCacheFlushingMonitor[CacheAccessor[Statement, ExecutionPlan]],
                           context: PlanContext) =
    context.getOrCreateFromSchemaState(cacheAccessor, {
      monitor.cacheFlushDetected(cacheAccessor)
      planCacheFactory()
    })
}
