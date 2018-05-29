/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v3_5

import java.time.Clock

import org.neo4j.cypher._
import org.neo4j.cypher.exceptionHandler.{RunSafely, runSafely}
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compatibility.v3_5.runtime._
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.{ExecutionPlan => ExecutionPlan_v3_5}
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.helpers.simpleExpressionEvaluator
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compiler.v3_5
import org.neo4j.cypher.internal.compiler.v3_5._
import org.neo4j.cypher.internal.compiler.v3_5.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.{CachedMetricsFactory, SimpleMetricsFactory}
import org.neo4j.cypher.internal.planner.v3_5.spi.{CostBasedPlannerName, DPPlannerName, IDPPlannerName, PlanContext}
import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log
import org.opencypher.v9_0.ast.Statement
import org.opencypher.v9_0.expressions.Parameter
import org.opencypher.v9_0.frontend.phases._
import org.opencypher.v9_0.rewriting.RewriterStepSequencer
import org.opencypher.v9_0.util.InputPosition
import org.opencypher.v9_0.util.attribution.SequentialIdGen

case class Cypher35Compiler[CONTEXT <: CommunityRuntimeContext,
                    T <: Transformer[CONTEXT, LogicalPlanState, CompilationState]](config: CypherPlannerConfiguration,
                                                                                   clock: Clock,
                                                                                   kernelMonitors: KernelMonitors,
                                                                                   log: Log,
                                                                                   planner: CypherPlannerOption,
                                                                                   updateStrategy: CypherUpdateStrategy,
                                                                                   runtime: CypherRuntime[CONTEXT],
                                                                                   contextCreatorv3_5: ContextCreator[CONTEXT],
                                                                                   txIdProvider: () => Long)
  extends LatestRuntimeVariablePlannerCompatibility[CONTEXT, T, Statement](config,
                                                                           clock,
                                                                           kernelMonitors,
                                                                           log,
                                                                           planner,
                                                                           updateStrategy,
                                                                           runtime,
                                                                           contextCreatorv3_5,
                                                                           txIdProvider) with CachingCompiler[BaseState]  {

  val monitors: Monitors = WrappedMonitors(kernelMonitors)
  monitors.addMonitorListener(logStalePlanRemovalMonitor(logger), "cypher3.4")

  val maybePlannerNamev3_5: Option[CostBasedPlannerName] = planner match {
    case CypherPlannerOption.default => None
    case CypherPlannerOption.cost | CypherPlannerOption.idp => Some(IDPPlannerName)
    case CypherPlannerOption.dp => Some(DPPlannerName)
    case _ => throw new IllegalArgumentException(s"unknown cost based planner: ${planner.name}")
  }
  val maybeUpdateStrategy: Option[UpdateStrategy] = updateStrategy match {
    case CypherUpdateStrategy.eager => Some(eagerUpdateStrategy)
    case _ => None
  }

  override def parserCacheSize: Int = config.queryCacheSize
  override def plannerCacheSize: Int = config.queryCacheSize

  protected val rewriterSequencer: (String) => RewriterStepSequencer = {
    import RewriterStepSequencer._
    import org.neo4j.helpers.Assertion._

    if (assertionsEnabled()) newValidating else newPlain
  }

  protected val compiler: v3_5.CypherPlanner[CONTEXT] =
    new CypherPlannerFactory().costBasedCompiler(config, clock, monitors, rewriterSequencer,
      maybePlannerNamev3_5, maybeUpdateStrategy, contextCreatorv3_5)

  private def queryGraphSolver = LatestRuntimeVariablePlannerCompatibility.
    createQueryGraphSolver(maybePlannerNamev3_5.getOrElse(CostBasedPlannerName.default), monitors, config)

  private def checkForSchemaChanges(planContext: PlanContext): Unit =
    planContext.getOrCreateFromSchemaState(this, planCache.clear())

  override val runSafelyDuringPlanning: RunSafely = runSafely
  override val runSafelyDuringRuntime: RunSafely = runSafely

  override def clearCaches(): Long = {
    Math.max(super.clearCaches(), planCache.clear())
  }

  override def compile(preParsedQuery: PreParsedQuery,
                       tracer: CompilationPhaseTracer,
                       preParsingNotifications: Set[org.neo4j.graphdb.Notification],
                       transactionalContext: TransactionalContext
                      ): CacheableExecutableQuery = {

    val notificationLogger = new RecordingNotificationLogger(Some(preParsedQuery.offset))

    runSafely {
      val syntacticQuery =
        getOrParse(preParsedQuery, new Parser3_5(compiler, notificationLogger, preParsedQuery.offset, tracer))

      // Context used for db communication during planning
      val planContext = new ExceptionTranslatingPlanContext(TransactionBoundPlanContext(
        TransactionalContextWrapper(transactionalContext), notificationLogger))

      // Context used to create logical plans
      val logicalPlanIdGen = new SequentialIdGen()
      val context = contextCreatorv3_5.create(tracer,
                                              notificationLogger,
                                              planContext,
                                              syntacticQuery.queryText,
                                              preParsedQuery.debugOptions,
                                              Some(preParsedQuery.offset),
                                              monitors,
                                              CachedMetricsFactory(SimpleMetricsFactory),
                                              queryGraphSolver,
                                              config,
                                              maybeUpdateStrategy.getOrElse(defaultUpdateStrategy),
                                              clock,
                                              logicalPlanIdGen,
                                              simpleExpressionEvaluator)

      // Prepare query for caching
      val preparedQuery = compiler.normalizeQuery(syntacticQuery, context)
      val queryParamNames: Seq[String] = preparedQuery.statement().findByAllClass[Parameter].map(x => x.name)

      checkForSchemaChanges(planContext)

      // If the query is not cached we want to do the full planning + creating executable plan
      def createPlan(): ExecutionPlan_v3_5 = {
        val logicalPlanState = compiler.planPreparedQuery(preparedQuery, context)
          notification.LogicalPlanNotifications
          .checkForNotifications(logicalPlanState.maybeLogicalPlan.get, planContext, config)
          .foreach(notificationLogger.log)
       runtime.compileToExecutable(logicalPlanState, context)
      }

      val executionPlan3_5 =
        if (preParsedQuery.debugOptions.isEmpty)
          planCache.computeIfAbsentOrStale(syntacticQuery.statement(),
                                           transactionalContext,
                                           createPlan,
                                           syntacticQuery.queryText).executableQuery
        else
          createPlan()

      val executionPlan = new ExecutionPlanWrapper(executionPlan3_5, preParsingNotifications)
      CacheableExecutableQuery(executionPlan, queryParamNames, ValueConversion.asValues(preparedQuery.extractedParams()))
    }
  }
}

class Parser3_5[CONTEXT3_5 <: v3_5.phases.PlannerContext](compiler: v3_5.CypherPlanner[CONTEXT3_5],
                                                           notificationLogger: RecordingNotificationLogger,
                                                           offset: InputPosition,
                                                           tracer: CompilationPhaseTracer
                                                          ) extends Parser[BaseState] {

  override def parse(preParsedQuery: PreParsedQuery): BaseState = {
    compiler.parseQuery(preParsedQuery.statement,
                        preParsedQuery.rawStatement,
                        notificationLogger,
                        preParsedQuery.planner.name,
                        preParsedQuery.debugOptions,
                        Some(offset),
                        tracer)
  }
}
