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

import org.neo4j.cypher.internal.compiler.v3_1.CompilationPhaseTracer.CompilationPhase.{AST_REWRITE, PARSING, SEMANTIC_CHECK}
import org.neo4j.cypher.internal.compiler.v3_1.ast.rewriters.{normalizeReturnClauses, normalizeWithClauses}
import org.neo4j.cypher.internal.compiler.v3_1.executionplan._
import org.neo4j.cypher.internal.compiler.v3_1.helpers.closing
import org.neo4j.cypher.internal.compiler.v3_1.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_1.ast.Statement
import org.neo4j.cypher.internal.frontend.v3_1.notification.InternalNotification
import org.neo4j.cypher.internal.frontend.v3_1.parser.CypherParser
import org.neo4j.cypher.internal.frontend.v3_1.{InputPosition, SemanticTable, inSequence}

/*
 * Orchestrates all phases needed to go from query text to runnable plan
 */
case class CypherCompiler(parser: CypherParser,
                          semanticChecker: SemanticChecker,
                          executionPlanBuilder: ExecutionPlanBuilder,
                          astRewriter: ASTRewriter,
                          cacheAccessor: CacheAccessor[Statement, ExecutionPlan],
                          planCacheFactory: () => LRUCache[Statement, ExecutionPlan],
                          cacheMonitor: CypherCacheFlushingMonitor[CacheAccessor[Statement, ExecutionPlan]],
                          monitors: Monitors) extends CompilationOrchestrator {

  override def planQuery(queryText: String, context: PlanContext, notificationLogger: InternalNotificationLogger,
                         plannerName: String, offset: Option[InputPosition] = None): (ExecutionPlan, Map[String, Any]) = {
    val firstHalf = prepareSyntacticQuery(queryText, queryText, notificationLogger, plannerName, offset = offset, tracer = CompilationPhaseTracer.NO_TRACING)
    val secondHalf = planPreparedQuery(firstHalf, context, offset, CompilationPhaseTracer.NO_TRACING)

    secondHalf
  }

  override def planPreparedQuery(syntacticQuery: PreparedQuerySyntax,
                                 context: PlanContext,
                                 offset: Option[InputPosition] = None,
                                 tracer: CompilationPhaseTracer): (ExecutionPlan, Map[String, Any]) = {
    val semanticQuery = prepareSemanticQuery(syntacticQuery, context, offset, tracer)
    val cache = provideCache(cacheAccessor, cacheMonitor, context)
    val (executionPlan, _) = cache.getOrElseUpdate(syntacticQuery.statement, syntacticQuery.queryText,
      _.isStale(context.txIdProvider, context.statistics),
      executionPlanBuilder.build(context, semanticQuery, tracer)
    )
    (executionPlan, semanticQuery.extractedParams)
  }

  override def prepareSyntacticQuery(queryText: String, rawQueryText: String, notificationLogger: InternalNotificationLogger,
                                     plannerName: String, offset: Option[InputPosition] = None,
                                     tracer: CompilationPhaseTracer): PreparedQuerySyntax = {

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

    PreparedQuerySyntax(rewrittenStatement, queryText, offset, extractedParams)(notificationLogger, plannerName, postConditions)
  }

  def prepareSemanticQuery(syntacticQuery: PreparedQuerySyntax,
                           context: PlanContext,
                           offset: Option[InputPosition] = None,
                           tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING): PreparedQuerySemantics = {

    val queryText = syntacticQuery.queryText

    val rewrittenSyntacticQuery = syntacticQuery.rewrite(rewriteProcedureCalls(context.procedureSignature))

    val mkException = new SyntaxExceptionCreator(queryText, offset)
    val postRewriteSemanticState = closing(tracer.beginPhase(SEMANTIC_CHECK)) {
      semanticChecker.check(syntacticQuery.queryText, rewrittenSyntacticQuery.statement, mkException)
    }

    val table = SemanticTable(types = postRewriteSemanticState.typeTable, recordedScopes = postRewriteSemanticState.recordedScopes)
    val result = rewrittenSyntacticQuery.withSemantics(table, postRewriteSemanticState.scopeTree)
    result
  }

  private def syntaxDeprecationNotifications(statement: Statement) =
  // We don't have any deprecations in 3.1 yet
    Seq.empty[InternalNotification]

  private def provideCache(cacheAccessor: CacheAccessor[Statement, ExecutionPlan],
                           monitor: CypherCacheFlushingMonitor[CacheAccessor[Statement, ExecutionPlan]],
                           context: PlanContext) =
    context.getOrCreateFromSchemaState(cacheAccessor, {
      monitor.cacheFlushDetected(cacheAccessor)
      val lRUCache = planCacheFactory()
      new QueryCacheManager(cacheAccessor, lRUCache)
    })
}

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
