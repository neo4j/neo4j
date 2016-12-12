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

import org.neo4j.cypher.internal.compiler.v3_2.executionplan._
import org.neo4j.cypher.internal.compiler.v3_2.phases.CompilationState.{State1, State4}
import org.neo4j.cypher.internal.compiler.v3_2.phases._
import org.neo4j.cypher.internal.compiler.v3_2.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v3_2.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_2.ast.Statement
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, SemanticTable}

case class CypherCompiler(executionPlanBuilder: ExecutionPlanBuilder,
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
    val exceptionCreator = new SyntaxExceptionCreator(rawQueryText, offset)
    val startState = State1(queryText, offset)
    val context = Context(exceptionCreator, tracer, notificationLogger)
    val finalState: State4 = firstPipePline.transform(startState, context)

    val postConditions = finalState.postConditions
    val extractedParams = finalState.extractedParams
    PreparedQuerySyntax(finalState.statement, rawQueryText, offset, extractedParams)(plannerName, postConditions)
  }

  private val astRewriting = AstRewriting(sequencer)

  private val firstPipePline =
      Parsing andThen
      DeprecationWarnings andThen
      PreparatoryRewriting andThen
      SemanticAnalysis.Early andThen
      astRewriting

  def prepareSemanticQuery(syntacticQuery: PreparedQuerySyntax, notificationLogger: InternalNotificationLogger,
                           context: PlanContext,
                           offset: Option[InputPosition] = None,
                           tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING): PreparedQuerySemantics = {
    val rewriteProcedureCalls = new RewriteProcedureCalls(context.procedureSignature, context.functionSignature)

    val secondPipeLine =
      rewriteProcedureCalls andThen
      SemanticAnalysis.Late

    val exceptionCreator = new SyntaxExceptionCreator(syntacticQuery.queryText, offset)

    val input = State4(
      queryText = syntacticQuery.queryText,
      startPosition = syntacticQuery.offset,
      statement = syntacticQuery.statement,
      semantics = null,
      extractedParams = syntacticQuery.extractedParams,
      postConditions = syntacticQuery.conditions)

    val output = secondPipeLine.transform(input, Context(exceptionCreator, null, notificationLogger))

    val table = SemanticTable(types = output.semantics.typeTable, recordedScopes = output.semantics.recordedScopes)
    val copy1 = syntacticQuery.copy(statement = output.statement)(syntacticQuery.plannerName, syntacticQuery.conditions)
    val result = copy1.withSemantics(table, output.semantics.scopeTree)
    result
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
