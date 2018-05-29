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
package org.neo4j.cypher.internal.compatibility.v3_3

import java.time.Clock

import org.neo4j.cypher.exceptionHandler.{RunSafely, runSafely => runtimeRunSafely}
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compatibility.v3_3.helpers.as3_3
import org.neo4j.cypher.internal.compatibility.v3_5.notification.LogicalPlanNotifications
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.{ExecutionPlan => ExecutionPlan_v3_5}
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.helpers.simpleExpressionEvaluator
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.{CommunityRuntimeContext => CommunityRuntimeContextv3_5, _}
import org.neo4j.cypher.internal.compatibility.v3_5.{ExceptionTranslatingPlanContext => ExceptionTranslatingPlanContextv3_5}
import org.neo4j.cypher.internal.compiler.v3_3
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.{idp => idpV3_3}
import org.neo4j.cypher.internal.compiler.v3_3.planner.{logical => logicalV3_3}
import org.neo4j.cypher.internal.compiler.v3_3.{CypherCompilerFactory, DPPlannerName => DPPlannerNameV3_3, IDPPlannerName => IDPPlannerNameV3_3}
import org.neo4j.cypher.internal.compiler.v3_5._
import org.neo4j.cypher.internal.compiler.v3_5.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.{CachedMetricsFactory, SimpleMetricsFactory}
import org.neo4j.cypher.internal.frontend.v3_3.ast.{Expression, Parameter, Statement => StatementV3_3}
import org.neo4j.cypher.internal.frontend.v3_3.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_3.phases
import org.neo4j.cypher.internal.frontend.v3_3.phases.{BaseState, Monitors => MonitorsV3_3, RecordingNotificationLogger => RecordingNotificationLoggerV3_3}
import org.neo4j.cypher.internal.planner.v3_5.spi.{CostBasedPlannerName, PlanContext, InstrumentedGraphStatistics => InstrumentedGraphStatisticsv3_5, MutableGraphStatisticsSnapshot => MutableGraphStatisticsSnapshotv3_5}
import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.cypher.internal.spi.v3_3.{ExceptionTranslatingPlanContext => ExceptionTranslatingPlanContextV3_3, TransactionBoundGraphStatistics => TransactionBoundGraphStatisticsV3_3, TransactionBoundPlanContext => TransactionBoundPlanContextV3_3}
import org.neo4j.cypher.{CypherPlannerOption, CypherUpdateStrategy}
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log
import org.opencypher.v9_0.frontend.phases.{CompilationPhaseTracer, Transformer, RecordingNotificationLogger => RecordingNotificationLoggerv3_5}
import org.opencypher.v9_0.util.attribution.SequentialIdGen

case class Cypher33Compiler[CONTEXT3_3 <: v3_3.phases.CompilerContext,
CONTEXT3_5 <: CommunityRuntimeContextv3_5,
T <: Transformer[CONTEXT3_5, LogicalPlanState, CompilationState]](configv3_5: CypherPlannerConfiguration,
                                                                  clock: Clock,
                                                                  kernelMonitors: KernelMonitors,
                                                                  log: Log,
                                                                  planner: CypherPlannerOption,
                                                                  updateStrategy: CypherUpdateStrategy,
                                                                  runtime: CypherRuntime[CONTEXT3_5],
                                                                  contextCreatorV3_3: v3_3.ContextCreator[CONTEXT3_3],
                                                                  contextCreatorV3_5: ContextCreator[CONTEXT3_5],
                                                                  txIdProvider: () => Long)
extends LatestRuntimeVariablePlannerCompatibility[CONTEXT3_5, T, StatementV3_3](configv3_5,
                                                                                clock,
                                                                                kernelMonitors,
                                                                                log,
                                                                                planner,
                                                                                updateStrategy,
                                                                                runtime,
                                                                                contextCreatorV3_5,
                                                                                txIdProvider) with CachingCompiler[BaseState] {

  val monitorsV3_3: MonitorsV3_3 = WrappedMonitors(kernelMonitors)
  monitorsV3_3.addMonitorListener(logStalePlanRemovalMonitor(logger), "cypher3.3")

  val configV3_3: v3_3.CypherCompilerConfiguration = helpers.as3_3(configv3_5)
  val maybePlannerName: Option[v3_3.CostBasedPlannerName] = planner match {
    case CypherPlannerOption.default => None
    case CypherPlannerOption.cost | CypherPlannerOption.idp => Some(IDPPlannerNameV3_3)
    case CypherPlannerOption.dp => Some(DPPlannerNameV3_3)
    case _ => throw new IllegalArgumentException(s"unknown cost based planner: ${planner.name}")
  }
  override val maybePlannerNamev3_5: Option[CostBasedPlannerName] = maybePlannerName.map(x => helpers.as3_5(x).asInstanceOf[CostBasedPlannerName])
  val maybeUpdateStrategy: Option[v3_3.UpdateStrategy] = updateStrategy match {
    case CypherUpdateStrategy.eager => Some(v3_3.eagerUpdateStrategy)
    case _ => None
  }

  override def parserCacheSize: Int = configv3_5.queryCacheSize
  override def plannerCacheSize: Int = configv3_5.queryCacheSize

  val rewriterSequencer: (String) => RewriterStepSequencer = {
    import RewriterStepSequencer._
    import org.neo4j.helpers.Assertion._

    if (assertionsEnabled()) newValidating else newPlain
  }
  val compiler: v3_3.CypherCompiler[CONTEXT3_3] =
    new CypherCompilerFactory().costBasedCompiler(configV3_3, clock, monitorsV3_3, rewriterSequencer,
      maybePlannerName, maybeUpdateStrategy, contextCreatorV3_3)


  private def queryGraphSolverV3_3 = Cypher33Compiler.
    createQueryGraphSolver(maybePlannerName.getOrElse(v3_3.CostBasedPlannerName.default), monitorsV3_3, configV3_3)

  private def checkForSchemaChanges(planContext: PlanContext): Unit =
    planContext.getOrCreateFromSchemaState(this, planCache.clear())

  override val runSafelyDuringPlanning : RunSafely = runSafely
  override val runSafelyDuringRuntime : RunSafely = runtimeRunSafely

  override def clearCaches(): Long = {
    Math.max(super.clearCaches(), planCache.clear())
  }

  override def compile(preParsedQuery: PreParsedQuery,
                       tracer: CompilationPhaseTracer,
                       preParsingNotifications: Set[org.neo4j.graphdb.Notification],
                       transactionalContext: TransactionalContext
                      ): CacheableExecutableQuery = {

    val inputPositionV3_3 = as3_3(preParsedQuery.offset)
    val inputPositionv3_5 = preParsedQuery.offset
    val notificationLoggerV3_3 = new RecordingNotificationLoggerV3_3(Some(inputPositionV3_3))
    val notificationLoggerv3_5 = new RecordingNotificationLoggerv3_5(Some(inputPositionv3_5))

    runSafely {
      val syntacticQuery =
        getOrParse(preParsedQuery, new Parser3_3(compiler, notificationLoggerV3_3, inputPositionV3_3, as3_3(tracer)))

      // Context used for db communication during planning
      val tcv3_5 = TransactionalContextWrapper(transactionalContext)

      // Create graph-statistics to be shared between 3.3 logical planning and 3.5 physical planning
      val graphStatisticsSnapshotv3_5 = new MutableGraphStatisticsSnapshotv3_5()
      val graphStatisticsV3_3 = new WrappedInstrumentedGraphStatistics(
        TransactionBoundGraphStatisticsV3_3(tcv3_5.dataRead, tcv3_5.schemaRead),
        graphStatisticsSnapshotv3_5)

      val planContextV3_3 = new ExceptionTranslatingPlanContextV3_3(
        new TransactionBoundPlanContextV3_3(() => transactionalContext.kernelTransaction,
                                            notificationLoggerV3_3, graphStatisticsV3_3))

      val graphStatisticsv3_5 = InstrumentedGraphStatisticsv3_5(
        TransactionBoundGraphStatistics(tcv3_5.dataRead, tcv3_5.schemaRead),
        graphStatisticsSnapshotv3_5)

      val planContextv3_5 = new ExceptionTranslatingPlanContextv3_5(
        new TransactionBoundPlanContext(tcv3_5, notificationLoggerv3_5, graphStatisticsv3_5))

      // Only used during planning
      def simpleExpressionEvaluatorV3_3 = new logicalV3_3.ExpressionEvaluator {
        override def evaluateExpression(expr: Expression): Option[Any] = None
      }

      // Context used to create logical plans
      val contextV3_3: CONTEXT3_3 =
        contextCreatorV3_3.create(as3_3(tracer),
                                  notificationLoggerV3_3,
                                  planContextV3_3,
                                  syntacticQuery.queryText,
                                  preParsedQuery.debugOptions,
                                  Some(inputPositionV3_3),
                                  monitorsV3_3,
                                  logicalV3_3.CachedMetricsFactory(logicalV3_3.SimpleMetricsFactory),
                                  queryGraphSolverV3_3,
                                  configV3_3,
                                  maybeUpdateStrategy.getOrElse(v3_3.defaultUpdateStrategy),
                                  clock,
                                  simpleExpressionEvaluatorV3_3)

      val logicalPlanIdGen = new SequentialIdGen()
      val contextv3_5: CONTEXT3_5 =
        contextCreatorV3_5.create(tracer,
                                  notificationLoggerv3_5,
                                  planContextv3_5,
                                  syntacticQuery.queryText,
                                  preParsedQuery.debugOptions,
                                  Some(inputPositionv3_5),
                                  monitorsv3_5,
                                  CachedMetricsFactory(SimpleMetricsFactory),
                                  queryGraphSolverv3_5,
                                  configv3_5,
                                  maybeUpdateStrategy.map(helpers.as3_5).getOrElse(defaultUpdateStrategy),
                                  clock,
                                  logicalPlanIdGen,
                                  simpleExpressionEvaluator)

      // Prepare query for caching
      val preparedQuery = compiler.normalizeQuery(syntacticQuery, contextV3_3)
      val queryParamNames: Seq[String] = preparedQuery.statement().findByAllClass[Parameter].map(x => x.name)
      checkForSchemaChanges(planContextv3_5)

      // If the query is not cached we do full planning + creating of executable plan
      def createPlan(): ExecutionPlan_v3_5 = {
        val logicalPlanStateV3_3 = compiler.planPreparedQuery(preparedQuery, contextV3_3)
        val logicalPlanStatev3_5 = helpers.as3_5(logicalPlanStateV3_3) // Here we switch from 3.3 to 3.5
        LogicalPlanNotifications
          .checkForNotifications(logicalPlanStatev3_5.maybeLogicalPlan.get, planContextv3_5, configv3_5)
          .foreach(notificationLoggerv3_5.log)
        runtime.compileToExecutable(logicalPlanStatev3_5, contextv3_5)
      }

      val executionPlan3_5 =
        if (preParsedQuery.debugOptions.isEmpty)
          planCache.computeIfAbsentOrStale(syntacticQuery.statement(),
                                           transactionalContext,
                                           createPlan,
                                           syntacticQuery.queryText).executableQuery
        else
          createPlan()

      // Log notifications/warnings from planning
      notificationLoggerV3_3.notifications.map(helpers.as3_5).foreach(notificationLoggerv3_5.log)
      val executionPlan = new ExecutionPlanWrapper(executionPlan3_5, preParsingNotifications)
      CacheableExecutableQuery(executionPlan, queryParamNames, ValueConversion.asValues(preparedQuery.extractedParams()))
    }
  }
}

class Parser3_3[CONTEXT3_3 <: v3_3.phases.CompilerContext](compiler: v3_3.CypherCompiler[CONTEXT3_3],
                                                           notificationLogger: RecordingNotificationLoggerV3_3,
                                                           offset: org.neo4j.cypher.internal.frontend.v3_3.InputPosition,
                                                           tracer: phases.CompilationPhaseTracer
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

object Cypher33Compiler {
  def createQueryGraphSolver(n: v3_3.CostBasedPlannerName, monitors: MonitorsV3_3,
                             config: v3_3.CypherCompilerConfiguration): logicalV3_3.QueryGraphSolver = n match {
    case v3_3.IDPPlannerName =>
      val monitor = monitors.newMonitor[idpV3_3.IDPQueryGraphSolverMonitor]()
      val solverConfig = new idpV3_3.ConfigurableIDPSolverConfig(
        maxTableSize = config.idpMaxTableSize,
        iterationDurationLimit = config.idpIterationDuration
      )
      val singleComponentPlanner = idpV3_3.SingleComponentPlanner(monitor, solverConfig)
      idpV3_3.IDPQueryGraphSolver(singleComponentPlanner, idpV3_3.cartesianProductsOrValueJoins, monitor)

    case v3_3.DPPlannerName =>
      val monitor = monitors.newMonitor[idpV3_3.IDPQueryGraphSolverMonitor]()
      val singleComponentPlanner = idpV3_3.SingleComponentPlanner(monitor, idpV3_3.DPSolverConfig)
      idpV3_3.IDPQueryGraphSolver(singleComponentPlanner, idpV3_3.cartesianProductsOrValueJoins, monitor)
  }
}
