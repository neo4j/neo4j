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

import org.neo4j.cypher.internal.compiler.v3_2.CompilationPhaseTracer.CompilationPhase.SEMANTIC_CHECK
import org.neo4j.cypher.internal.compiler.v3_2.ast.ResolvedCall
import org.neo4j.cypher.internal.compiler.v3_2.executionplan._
import org.neo4j.cypher.internal.compiler.v3_2.helpers.closing
import org.neo4j.cypher.internal.compiler.v3_2.phases.PipeLineRunner
import org.neo4j.cypher.internal.compiler.v3_2.spi.{PlanContext, ProcedureSignature}
import org.neo4j.cypher.internal.compiler.v3_2.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_2.ast.Statement
import org.neo4j.cypher.internal.frontend.v3_2.notification.{DeprecatedProcedureNotification, InternalNotification}
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, SemanticTable}

case class CypherCompiler(semanticChecker: SemanticChecker,
                          executionPlanBuilder: ExecutionPlanBuilder,
                          astRewriter: ASTRewriter,
                          cacheAccessor: CacheAccessor[Statement, ExecutionPlan],
                          planCacheFactory: () => LFUCache[Statement, ExecutionPlan],
                          cacheMonitor: CypherCacheFlushingMonitor[CacheAccessor[Statement, ExecutionPlan]],
                          monitors: Monitors,
                          sequencer: String => RewriterStepSequencer) {

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
    val mkException = new SyntaxExceptionCreator(rawQueryText, offset)
    val pipeLine = PipeLineRunner(mkException,sequencer, tracer, notificationLogger)
    pipeLine.runIt(queryText, offset)(plannerName)
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
      semanticChecker.check(rewrittenSyntacticQuery.statement, mkException)
    }

    val table = SemanticTable(types = postRewriteSemanticState.typeTable, recordedScopes = postRewriteSemanticState.recordedScopes)
    val result = rewrittenSyntacticQuery.withSemantics(table, postRewriteSemanticState.scopeTree)
    result
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


case class CypherCompilerConfiguration(queryCacheSize: Int,
                                       statsDivergenceThreshold: Double,
                                       queryPlanTTL: Long,
                                       useErrorsOverWarnings: Boolean,
                                       idpMaxTableSize: Int,
                                       idpIterationDuration: Long,
                                       errorIfShortestPathFallbackUsedAtRuntime: Boolean,
                                       nonIndexedLabelWarningThreshold: Long)

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
