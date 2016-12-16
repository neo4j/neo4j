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

import org.neo4j.cypher.internal.compiler.v3_2.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.compiler.v3_2.CompilationPhaseTracer.CompilationPhase._
import org.neo4j.cypher.internal.compiler.v3_2.ast.rewriters.{CNFNormalizer, Namespacer, rewriteEqualityToInPredicate}
import org.neo4j.cypher.internal.compiler.v3_2.executionplan._
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.procs.ProcedureOrSchemaCommandPlanBuilder
import org.neo4j.cypher.internal.compiler.v3_2.helpers.RuntimeTypeConverter
import org.neo4j.cypher.internal.compiler.v3_2.phases.CompilationState.{State1, State4, State5}
import org.neo4j.cypher.internal.compiler.v3_2.phases.OrElse._
import org.neo4j.cypher.internal.compiler.v3_2.phases._
import org.neo4j.cypher.internal.compiler.v3_2.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v3_2.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_2.InputPosition
import org.neo4j.cypher.internal.frontend.v3_2.ast.Statement

case class CypherCompiler(executionPlanBuilder: ExecutablePlanBuilder,
                          astRewriter: ASTRewriter,
                          cacheAccessor: CacheAccessor[Statement, ExecutionPlan],
                          planCacheFactory: () => LFUCache[Statement, ExecutionPlan],
                          cacheMonitor: CypherCacheFlushingMonitor[CacheAccessor[Statement, ExecutionPlan]],
                          monitors: Monitors,
                          sequencer: String => RewriterStepSequencer,
                          createFingerprintReference: Option[PlanFingerprint] => PlanFingerprintReference,
                          typeConverter: RuntimeTypeConverter) {


  def planQuery(queryText: String, context: PlanContext, notificationLogger: InternalNotificationLogger,
                plannerName: String = "",
                offset: Option[InputPosition] = None): (ExecutionPlan, Map[String, Any]) = {
    val step1: State4 = prepareSyntacticQuery(queryText, queryText, notificationLogger, plannerName)
    planPreparedQuery(step1, notificationLogger, context, offset, CompilationPhaseTracer.NO_TRACING)
  }

  def planPreparedQuery(input: State4,
                        notificationLogger: InternalNotificationLogger,
                        planContext: PlanContext,
                        offset: Option[InputPosition] = None,
                        tracer: CompilationPhaseTracer): (ExecutionPlan, Map[String, Any]) = {
    val semanticQuery: State5 = prepareSemanticQuery(input, notificationLogger, planContext)
    val cache = provideCache(cacheAccessor, cacheMonitor, planContext)
    val isStale = (plan: ExecutionPlan) => plan.isStale(planContext.txIdProvider, planContext.statistics)
    val (executionPlan, _) = cache.getOrElseUpdate(input.statement, input.queryText, isStale, {
      val context = createContext(tracer, notificationLogger, planContext, input.queryText, offset)
      thirdPipeLine.transform(semanticQuery, context)
    })
    (executionPlan, semanticQuery.extractedParams)
  }

  def prepareSyntacticQuery(queryText: String, rawQueryText: String, notificationLogger: InternalNotificationLogger,
                            plannerName: String = "",
                            offset: Option[InputPosition] = None,
                            tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING): State4 = {
    val startState = State1(queryText, offset, plannerName)
    val context = createContext(tracer, notificationLogger, null, queryText, offset) //TODO: short cut
    firstPipeline.transform(startState, context)
  }

  private val astRewriting = AstRewriting(sequencer)

  private val firstPipeline: Transformer[State1, State4] =
      Parsing andThen
      DeprecationWarnings andThen
      PreparatoryRewriting andThen
      SemanticAnalysis.Early andThen
      astRewriting

  private val secondPipeLine: Transformer[State4, State5] =
      RewriteProcedureCalls andThen
      SemanticAnalysis.Late andThen
      Namespacer

  private val thirdPipeLine =
    ProcedureOrSchemaCommandPlanBuilder orElse {
      rewriteEqualityToInPredicate andThen
      CNFNormalizer andThen
      RestOfPipeLine
    }

  def prepareSemanticQuery(in: State4,
                           notificationLogger: InternalNotificationLogger,
                           planContext: PlanContext,
                           tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING): State5 = {

    secondPipeLine.transform(in, createContext(tracer, devNullLogger, planContext, in.queryText, in.startPosition))
  }

  private def provideCache(cacheAccessor: CacheAccessor[Statement, ExecutionPlan],
                           monitor: CypherCacheFlushingMonitor[CacheAccessor[Statement, ExecutionPlan]],
                           context: PlanContext) =
    context.getOrCreateFromSchemaState(cacheAccessor, {
      monitor.cacheFlushDetected(cacheAccessor)
      val lRUCache = planCacheFactory()
      new QueryCache(cacheAccessor, lRUCache)
    })

  object RestOfPipeLine extends Phase[State5, ExecutionPlan] {
    override def phase: CompilationPhase = LOGICAL_PLANNING

    override def description: String = "place holder until pipe line has been cleaned up"

    override def transform(from: State5, context: Context): ExecutionPlan = {
      executionPlanBuilder.producePlan(from, context.planContext, context.tracer, context.createFingerprintReference)
    }
  }

  private def createContext(tracer: CompilationPhaseTracer, notificationLogger: InternalNotificationLogger, planContext: PlanContext, queryText: String, offset: Option[InputPosition]):Context = {
    val exceptionCreator = new SyntaxExceptionCreator(queryText, offset)
    val monitor = monitors.newMonitor[AstRewritingMonitor]()
    Context(exceptionCreator, tracer, notificationLogger, planContext, typeConverter, createFingerprintReference, monitor)
  }
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
