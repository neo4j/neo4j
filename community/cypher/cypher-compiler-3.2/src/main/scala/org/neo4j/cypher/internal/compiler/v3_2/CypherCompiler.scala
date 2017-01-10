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
package org.neo4j.cypher.internal.compiler.v3_2

import java.time.Clock

import org.neo4j.cypher.internal.compiler.v3_2.ast.rewriters.{CNFNormalizer, Namespacer, rewriteEqualityToInPredicate}
import org.neo4j.cypher.internal.compiler.v3_2.codegen.CodeGenConfiguration
import org.neo4j.cypher.internal.compiler.v3_2.codegen.spi.CodeStructure
import org.neo4j.cypher.internal.compiler.v3_2.executionplan._
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.procs.ProcedureCallOrSchemaCommandPlanBuilder
import org.neo4j.cypher.internal.compiler.v3_2.helpers.RuntimeTypeConverter
import org.neo4j.cypher.internal.compiler.v3_2.phases._
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical._
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.rewriter.PlanRewriter
import org.neo4j.cypher.internal.compiler.v3_2.planner.{CheckForUnresolvedTokens, ResolveTokens, UnionQuery}
import org.neo4j.cypher.internal.compiler.v3_2.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v3_2.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_2.ast.Statement
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, SemanticState}

case class CypherCompiler(createExecutionPlan: Transformer,
                          astRewriter: ASTRewriter,
                          cacheAccessor: CacheAccessor[Statement, ExecutionPlan],
                          planCacheFactory: () => LFUCache[Statement, ExecutionPlan],
                          cacheMonitor: CypherCacheFlushingMonitor[CacheAccessor[Statement, ExecutionPlan]],
                          monitors: Monitors,
                          sequencer: String => RewriterStepSequencer,
                          createFingerprintReference: Option[PlanFingerprint] => PlanFingerprintReference,
                          typeConverter: RuntimeTypeConverter,
                          metricsFactory: MetricsFactory,
                          queryGraphSolver: QueryGraphSolver,
                          config: CypherCompilerConfiguration,
                          updateStrategy: UpdateStrategy,
                          clock: Clock,
                          structure: CodeStructure[GeneratedQuery]) {

  def planQuery(queryText: String,
                context: PlanContext,
                notificationLogger: InternalNotificationLogger,
                plannerName: String = "",
                offset: Option[InputPosition] = None): (ExecutionPlan, Map[String, Any]) = {
    val step1: CompilationState = parseQuery(queryText, queryText, notificationLogger, plannerName, None, CompilationPhaseTracer.NO_TRACING)
    planPreparedQuery(step1, notificationLogger, context, offset, CompilationPhaseTracer.NO_TRACING)
  }

  def planPreparedQuery(input: CompilationState,
                        notificationLogger: InternalNotificationLogger,
                        planContext: PlanContext,
                        offset: Option[InputPosition] = None,
                        tracer: CompilationPhaseTracer): (ExecutionPlan, Map[String, Any]) = {
    val context = createContext(tracer, notificationLogger, planContext, input.queryText, input.startPosition, config.codeGenConfiguration)
    val preparedCompilationState = prepareForCaching.transform(input, context)
    val cache = provideCache(cacheAccessor, cacheMonitor, planContext)
    val isStale = (plan: ExecutionPlan) => plan.isStale(planContext.txIdProvider, planContext.statistics)
    val (executionPlan, _) = cache.getOrElseUpdate(input.statement, input.queryText, isStale, {
      val result: CompilationState = planAndCreateExecPlan.transform(preparedCompilationState, context)
      result.executionPlan
    })
    (executionPlan, preparedCompilationState.extractedParams)
  }

  def parseQuery(queryText: String,
                 rawQueryText: String,
                 notificationLogger: InternalNotificationLogger,
                 plannerNameText: String = IDPPlannerName.name,
                 offset: Option[InputPosition],
                 tracer: CompilationPhaseTracer): CompilationState = {
    val plannerName = PlannerName(plannerNameText)
    val startState = CompilationState(queryText, offset, plannerName)
    //TODO: these nulls are a short cut
    val context = createContext(tracer, notificationLogger, planContext = null, rawQueryText, offset, codeGenConfiguration = null)
    parsing.transform(startState, context)
  }

  val parsing: Transformer =
    Parsing.adds[Statement] andThen
    SyntaxDeprecationWarnings andThen
    PreparatoryRewriting andThen
    SemanticAnalysis(warn = true).adds[SemanticState] andThen
    AstRewriting(sequencer, shouldExtractParams = true)

  val prepareForCaching: Transformer =
    RewriteProcedureCalls andThen
    ProcedureDeprecationWarnings

  val costBasedPlanning: PipeLine =
    SemanticAnalysis(warn = false) andThen
    Namespacer andThen
    rewriteEqualityToInPredicate andThen
    CNFNormalizer andThen
    LateAstRewriting andThen
    ResolveTokens andThen
    CreatePlannerQuery.adds[UnionQuery] andThen
    OptionalMatchRemover andThen
    QueryPlanner().adds[LogicalPlan] andThen
    PlanRewriter(sequencer) andThen
    If(_.unionQuery.readOnly) (
      CheckForUnresolvedTokens
    )

  val planAndCreateExecPlan: Transformer =
    ProcedureCallOrSchemaCommandPlanBuilder andThen
    If(_.maybeExecutionPlan.isEmpty)(
      costBasedPlanning andThen
      createExecutionPlan.adds[ExecutionPlan]
    )

  private def provideCache(cacheAccessor: CacheAccessor[Statement, ExecutionPlan],
                           monitor: CypherCacheFlushingMonitor[CacheAccessor[Statement, ExecutionPlan]],
                           planContext: PlanContext): QueryCache[Statement, ExecutionPlan] =
    planContext.getOrCreateFromSchemaState(cacheAccessor, {
      monitor.cacheFlushDetected(cacheAccessor)
      val lRUCache = planCacheFactory()
      new QueryCache(cacheAccessor, lRUCache)
    })

  private def createContext(tracer: CompilationPhaseTracer,
                            notificationLogger: InternalNotificationLogger,
                            planContext: PlanContext,
                            queryText: String,
                            offset: Option[InputPosition],
                            codeGenConfiguration: CodeGenConfiguration): Context = {
    val exceptionCreator = new SyntaxExceptionCreator(queryText, offset)

    val metrics: Metrics = if (planContext == null)
      null
    else
      metricsFactory.newMetrics(planContext.statistics)

    Context(exceptionCreator, tracer, notificationLogger, planContext, typeConverter, createFingerprintReference,
      monitors, metrics, queryGraphSolver, config, updateStrategy, clock, structure, codeGenConfiguration)
  }
}


case class CypherCompilerConfiguration(queryCacheSize: Int,
                                       statsDivergenceThreshold: Double,
                                       queryPlanTTL: Long,
                                       useErrorsOverWarnings: Boolean,
                                       idpMaxTableSize: Int,
                                       idpIterationDuration: Long,
                                       errorIfShortestPathFallbackUsedAtRuntime: Boolean,
                                       nonIndexedLabelWarningThreshold: Long,
                                       codeGenConfiguration: CodeGenConfiguration)

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
