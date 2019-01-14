/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compatibility.v3_4

import java.time.Clock

import org.neo4j.cypher._
import org.neo4j.cypher.exceptionHandler.{RunSafely, runSafely}
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compatibility.v3_4.notifications.LogicalPlanNotifications
import org.neo4j.cypher.internal.compatibility.v3_4.runtime._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.{ExecutionPlan => ExecutionPlan_v3_4}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.helpers.simpleExpressionEvaluator
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compiler.v3_4
import org.neo4j.cypher.internal.compiler.v3_4._
import org.neo4j.cypher.internal.compiler.v3_4.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.{CachedMetricsFactory, SimpleMetricsFactory}
import org.neo4j.cypher.internal.frontend.v3_4.ast.Statement
import org.neo4j.cypher.internal.frontend.v3_4.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_4.phases._
import org.neo4j.cypher.internal.planner.v3_4.spi.{CostBasedPlannerName, DPPlannerName, IDPPlannerName, PlanContext}
import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.cypher.internal.util.v3_4.attribution.SequentialIdGen
import org.neo4j.cypher.internal.v3_4.expressions.Parameter
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log

import scala.util.Try

case class Compatibility[CONTEXT <: CommunityRuntimeContext,
                    T <: Transformer[CONTEXT, LogicalPlanState, CompilationState]](config: CypherCompilerConfiguration,
                                                                                   clock: Clock,
                                                                                   kernelMonitors: KernelMonitors,
                                                                                   log: Log,
                                                                                   planner: CypherPlanner,
                                                                                   runtime: CypherRuntime,
                                                                                   updateStrategy: CypherUpdateStrategy,
                                                                                   runtimeBuilder: RuntimeBuilder[T],
                                                                                   contextCreatorV3_4: ContextCreator[CONTEXT])
  extends LatestRuntimeVariablePlannerCompatibility[CONTEXT, T, Statement](config, clock, kernelMonitors, log, planner, runtime, updateStrategy, runtimeBuilder, contextCreatorV3_4) {

  val monitors: Monitors = WrappedMonitors(kernelMonitors)
  val cacheMonitor: AstCacheMonitor[Statement] = monitors.newMonitor[AstCacheMonitor[Statement]]("cypher3.4")
  monitors.addMonitorListener(logStalePlanRemovalMonitor(logger), "cypher3.4")

  val maybePlannerNameV3_4: Option[CostBasedPlannerName] = planner match {
    case CypherPlanner.default => None
    case CypherPlanner.cost | CypherPlanner.idp => Some(IDPPlannerName)
    case CypherPlanner.dp => Some(DPPlannerName)
    case _ => throw new IllegalArgumentException(s"unknown cost based planner: ${planner.name}")
  }
  val maybeUpdateStrategy: Option[UpdateStrategy] = updateStrategy match {
    case CypherUpdateStrategy.eager => Some(eagerUpdateStrategy)
    case _ => None
  }

  protected val rewriterSequencer: (String) => RewriterStepSequencer = {
    import RewriterStepSequencer._
    import org.neo4j.helpers.Assertion._

    if (assertionsEnabled()) newValidating else newPlain
  }

  protected val compiler: v3_4.CypherCompiler[CONTEXT] =
    new CypherCompilerFactory().costBasedCompiler(config, clock, monitors, rewriterSequencer,
      maybePlannerNameV3_4, maybeUpdateStrategy, contextCreatorV3_4)

  private def queryGraphSolver = LatestRuntimeVariablePlannerCompatibility.
    createQueryGraphSolver(maybePlannerNameV3_4.getOrElse(CostBasedPlannerName.default), monitors, config)

  def produceParsedQuery(preParsedQuery: PreParsedQuery, preparationTracer: CompilationPhaseTracer,
                         preParsingNotifications: Set[org.neo4j.graphdb.Notification]): ParsedQuery = {
    val notificationLogger = new RecordingNotificationLogger(Some(preParsedQuery.offset))

    // The "preparationTracer" can get closed, even if a ParsedQuery is cached and reused. It should not
    // be used inside ParsedQuery.plan. There, use the "planningTracer" instead
    val preparedSyntacticQueryForV_3_4 =
      Try(compiler.parseQuery(preParsedQuery.statement,
                              preParsedQuery.rawStatement,
                              notificationLogger, preParsedQuery.planner.name,
                              preParsedQuery.debugOptions,
                              Some(preParsedQuery.offset), preparationTracer))
    new ParsedQuery {
      override def plan(transactionalContext: TransactionalContextWrapper, planningTracer: CompilationPhaseTracer):
        (ExecutionPlan, Map[String, Any], Seq[String]) = runSafely {
        val syntacticQuery = preparedSyntacticQueryForV_3_4.get

        //Context used for db communication during planning
        val planContext = new ExceptionTranslatingPlanContext(TransactionBoundPlanContext(transactionalContext, notificationLogger))
        //Context used to create logical plans
        val logicalPlanIdGen = new SequentialIdGen()
        val context = contextCreatorV3_4.create(planningTracer, notificationLogger, planContext,
                                                        syntacticQuery.queryText, preParsedQuery.debugOptions,
                                                        Some(preParsedQuery.offset), monitors,
                                                        CachedMetricsFactory(SimpleMetricsFactory), queryGraphSolver,
                                                        config, maybeUpdateStrategy.getOrElse(defaultUpdateStrategy),
                                                        clock, logicalPlanIdGen, simpleExpressionEvaluator)
        //Prepare query for caching
        val preparedQuery = compiler.normalizeQuery(syntacticQuery, context)
        val queryParamNames: Seq[String] = preparedQuery.statement().findByAllClass[Parameter].map(x => x.name)
        val cache = provideCache(cacheAccessor, cacheMonitor, planContext, planCacheFactory)
        val isStale = (plan: ExecutionPlan_v3_4) => plan.checkPlanResusability(planContext.txIdProvider, planContext.statistics)

        //Just in the case the query is not in the cache do we want to do the full planning + creating executable plan
        val createPlan = new PlanProducer[ExecutionPlan_v3_4] {
          override def produceWithExistingTX: ExecutionPlan_v3_4 = {
            val logicalPlanState = compiler.planPreparedQuery(preparedQuery, context)
            LogicalPlanNotifications
              .checkForNotifications(logicalPlanState.maybeLogicalPlan.get, planContext, config)
              .foreach(notificationLogger.log)

            val result = createExecPlan.transform(logicalPlanState, context)
            result.maybeExecutionPlan.get
          }
        }
        val executionPlan = if (preParsedQuery.debugOptions.isEmpty)
          cache.getOrElseUpdate(syntacticQuery.statement(), syntacticQuery.queryText, isStale, createPlan)._1
        else
          createPlan.produceWithExistingTX

        (new ExecutionPlanWrapper(executionPlan, preParsingNotifications, preParsedQuery.offset), preparedQuery.extractedParams(), queryParamNames)
      }

      override protected val trier: Try[BaseState] = preparedSyntacticQueryForV_3_4
    }
  }

  private def provideCache(cacheAccessor: CacheAccessor[Statement, ExecutionPlan_v3_4],
                           monitor: CypherCacheFlushingMonitor[CacheAccessor[Statement, ExecutionPlan_v3_4]],
                           planContext: PlanContext,
                           planCacheFactory: () => LFUCache[Statement, ExecutionPlan_v3_4]): QueryCache[Statement, ExecutionPlan_v3_4] =
    planContext.getOrCreateFromSchemaState(cacheAccessor, {
      monitor.cacheFlushDetected(cacheAccessor)
      val lRUCache = planCacheFactory()
      new QueryCache(cacheAccessor, lRUCache)
    })

  override val runSafelyDuringPlanning: RunSafely = runSafely
  override val runSafelyDuringRuntime: RunSafely = runSafely
}
