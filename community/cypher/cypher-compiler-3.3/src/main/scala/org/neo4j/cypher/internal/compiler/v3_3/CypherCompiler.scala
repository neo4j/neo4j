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
package org.neo4j.cypher.internal.compiler.v3_3

import java.time.Clock

import org.neo4j.cypher.internal.compiler.v3_3.executionplan._
import org.neo4j.cypher.internal.compiler.v3_3.executionplan.procs.ProcedureCallOrSchemaCommandPlanBuilder
import org.neo4j.cypher.internal.compiler.v3_3.helpers.RuntimeTypeConverter
import org.neo4j.cypher.internal.compiler.v3_3.phases.{CompilerContext, _}
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical._
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.rewriter.PlanRewriter
import org.neo4j.cypher.internal.compiler.v3_3.planner.{CheckForUnresolvedTokens, ResolveTokens}
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_3.InputPosition
import org.neo4j.cypher.internal.frontend.v3_3.ast.Statement
import org.neo4j.cypher.internal.frontend.v3_3.ast.rewriters.ASTRewriter
import org.neo4j.cypher.internal.frontend.v3_3.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_3.phases._
import org.neo4j.cypher.internal.ir.v3_2.UnionQuery

case class CypherCompiler[Context <: CompilerContext](createExecutionPlan: Transformer[Context, CompilationState, CompilationState],
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
                          contextCreation: ContextCreator[Context]) {

  def planQuery(queryText: String,
                context: PlanContext,
                notificationLogger: InternalNotificationLogger,
                plannerName: String = "",
                debugOptions: Set[String] = Set.empty,
                offset: Option[InputPosition] = None): (ExecutionPlan, Map[String, Any]) = {
    val state = parseQuery(queryText, queryText, notificationLogger, plannerName, debugOptions, None, CompilationPhaseTracer.NO_TRACING)
    planPreparedQuery(state, notificationLogger, context, debugOptions, offset, CompilationPhaseTracer.NO_TRACING)
  }

  def planPreparedQuery(state: BaseState,
                        notificationLogger: InternalNotificationLogger,
                        planContext: PlanContext,
                        debugOptions: Set[String],
                        offset: Option[InputPosition] = None,
                        tracer: CompilationPhaseTracer): (ExecutionPlan, Map[String, Any]) = {
    val context: Context = contextCreation.create(tracer, notificationLogger, planContext, state.queryText,
      debugOptions, state.startPosition, monitors, createFingerprintReference, typeConverter, metricsFactory,
      queryGraphSolver, config, updateStrategy, clock)
    val preparedCompilationState = prepareForCaching.transform(state, context)
    val cache = provideCache(cacheAccessor, cacheMonitor, planContext)
    val isStale = (plan: ExecutionPlan) => plan.isStale(planContext.txIdProvider, planContext.statistics)

    def createPlan(): ExecutionPlan = {
      val result: CompilationState = planAndCreateExecPlan.transform(preparedCompilationState, context)
      result.executionPlan
    }

    val executionPlan = if (debugOptions.isEmpty)
      cache.getOrElseUpdate(state.statement(), state.queryText, isStale, createPlan())._1
    else
      createPlan()

    (executionPlan, preparedCompilationState.extractedParams())
  }

  def parseQuery(queryText: String,
                 rawQueryText: String,
                 notificationLogger: InternalNotificationLogger,
                 plannerNameText: String = IDPPlannerName.name,
                 debugOptions: Set[String],
                 offset: Option[InputPosition],
                 tracer: CompilationPhaseTracer): BaseState = {
    val plannerName = PlannerNameFor(plannerNameText)
    val startState = CompilationState(queryText, offset, plannerName)
    //TODO: these nulls are a short cut
    val context = contextCreation.create(tracer, notificationLogger, planContext = null, rawQueryText, debugOptions,
      offset, monitors, createFingerprintReference, typeConverter, metricsFactory, queryGraphSolver, config,
      updateStrategy, clock)
    CompilationPhases.parsing(sequencer).transform(startState, context)
  }

  val prepareForCaching: Transformer[CompilerContext, BaseState, BaseState] =
    RewriteProcedureCalls andThen
    ProcedureDeprecationWarnings andThen
    ProcedureWarnings

  val irConstruction: Transformer[CompilerContext, BaseState, CompilationState] =
    ResolveTokens andThen
      CreatePlannerQuery.adds(CompilationContains[UnionQuery]) andThen
      OptionalMatchRemover

  val costBasedPlanning =
    QueryPlanner().adds(CompilationContains[LogicalPlan]) andThen
    PlanRewriter(sequencer) andThen
    If((s: CompilationState) => s.unionQuery.readOnly) (
      CheckForUnresolvedTokens
    )

  val standardPipeline: Transformer[Context, BaseState, CompilationState] =
    CompilationPhases.lateAstRewriting andThen
    irConstruction andThen
    costBasedPlanning andThen
    createExecutionPlan.adds(CompilationContains[ExecutionPlan])

  val planAndCreateExecPlan: Transformer[Context, BaseState, CompilationState] =
    ProcedureCallOrSchemaCommandPlanBuilder andThen
    If((s: CompilationState) => s.maybeExecutionPlan.isEmpty)(
      standardPipeline
    )

  private def provideCache(cacheAccessor: CacheAccessor[Statement, ExecutionPlan],
                           monitor: CypherCacheFlushingMonitor[CacheAccessor[Statement, ExecutionPlan]],
                           planContext: PlanContext): QueryCache[Statement, ExecutionPlan] =
    planContext.getOrCreateFromSchemaState(cacheAccessor, {
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
                                       errorIfShortestPathHasCommonNodesAtRuntime: Boolean,
                                       legacyCsvQuoteEscaping: Boolean,
                                       nonIndexedLabelWarningThreshold: Long)


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
