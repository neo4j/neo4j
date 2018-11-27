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
package org.neo4j.cypher.internal.compatibility.v3_4

import java.time.Clock

import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compatibility.v3_4.helpers.{as3_4, as4_0}
import org.neo4j.cypher.internal.compatibility.v4_0.notification.LogicalPlanNotifications
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.helpers.simpleExpressionEvaluator
import org.neo4j.cypher.internal.compiler.v3_4
import org.neo4j.cypher.internal.compiler.v3_4.CypherCompilerFactory
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.{idp => idpV3_4}
import org.neo4j.cypher.internal.compiler.v3_4.planner.{logical => logicalV3_4}
import org.neo4j.cypher.internal.compiler.v4_0.phases.{PlannerContext, PlannerContextCreator}
import org.neo4j.cypher.internal.compiler.v4_0.planner.logical.{CachedMetricsFactory, SimpleMetricsFactory}
import org.neo4j.cypher.internal.compiler.v4_0.{CypherPlannerConfiguration, defaultUpdateStrategy}
import org.neo4j.cypher.internal.frontend.v3_4.ast.{Statement => StatementV3_4}
import org.neo4j.cypher.internal.frontend.v3_4.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_4.phases
import org.neo4j.cypher.internal.frontend.v3_4.phases.{BaseState, Monitors => MonitorsV3_4, RecordingNotificationLogger => RecordingNotificationLoggerV3_4}
import org.neo4j.cypher.internal.planner.v3_4.spi.{DPPlannerName => DPPlannerNameV3_4, IDPPlannerName => IDPPlannerNameV3_4}
import org.neo4j.cypher.internal.planner.v3_4.{spi => spiV3_4}
import org.neo4j.cypher.internal.planner.v4_0.spi.{PlanContext, PlannerNameWithVersion, InstrumentedGraphStatistics => InstrumentedGraphStatisticsv4_0, MutableGraphStatisticsSnapshot => MutableGraphStatisticsSnapshotv4_0}
import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.cypher.internal.spi.v3_4.{ExceptionTranslatingPlanContext => ExceptionTranslatingPlanContextV3_4, TransactionBoundGraphStatistics => TransactionBoundGraphStatisticsV3_4, TransactionBoundPlanContext => TransactionBoundPlanContextV3_4}
import org.neo4j.cypher.internal.spi.v4_0.{ExceptionTranslatingPlanContext => ExceptionTranslatingPlanContextV4_0, TransactionBoundGraphStatistics => TransactionBoundGraphStatisticsV4_0, TransactionBoundPlanContext => TransactionBoundPlanContextV4_0}
import org.neo4j.cypher.internal.util.{v3_4 => utilV3_4}
import org.neo4j.cypher.internal.v3_4.expressions.{Expression, Parameter}
import org.neo4j.cypher.{CypherPlannerOption, CypherUpdateStrategy, CypherVersion}
import org.neo4j.helpers.collection.Pair
import org.neo4j.kernel.impl.api.SchemaStateKey
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log
import org.neo4j.values.virtual.MapValue
import org.neo4j.cypher.internal.v3_5.frontend.PlannerName
import org.neo4j.cypher.internal.v3_5.frontend.phases.{CompilationPhaseTracer, RecordingNotificationLogger, InternalNotificationLogger => InternalNotificationLoggerv4_0}
import org.neo4j.cypher.internal.v3_5.util.attribution.SequentialIdGen

case class Cypher3_4Planner(configv4_0: CypherPlannerConfiguration,
                            clock: Clock,
                            kernelMonitors: KernelMonitors,
                            log: Log,
                            plannerOption: CypherPlannerOption,
                            updateStrategy: CypherUpdateStrategy,
                            txIdProvider: () => Long)
  extends BasePlanner[StatementV3_4, BaseState](configv4_0, clock, kernelMonitors, log, txIdProvider) with CypherPlanner {

  val monitorsV3_4: MonitorsV3_4 = WrappedMonitors(kernelMonitors)
  monitorsV3_4.addMonitorListener(logStalePlanRemovalMonitor(logger), "cypher3.4")

  val configV3_4: v3_4.CypherCompilerConfiguration = helpers.as3_4(configv4_0)
  val plannerName: spiV3_4.CostBasedPlannerName = plannerOption match {
    case CypherPlannerOption.default => spiV3_4.CostBasedPlannerName.default
    case CypherPlannerOption.cost | CypherPlannerOption.idp => IDPPlannerNameV3_4
    case CypherPlannerOption.dp => DPPlannerNameV3_4
    case _ => throw new IllegalArgumentException(s"unknown cost based planner: ${plannerOption.name}")
  }

  val maybeUpdateStrategy: Option[v3_4.UpdateStrategy] = updateStrategy match {
    case CypherUpdateStrategy.eager => Some(v3_4.eagerUpdateStrategy)
    case _ => None
  }

  override def parserCacheSize: Int = configv4_0.queryCacheSize

  val rewriterSequencer: (String) => RewriterStepSequencer = {
    import RewriterStepSequencer._
    import org.neo4j.helpers.Assertion._

    if (assertionsEnabled()) newValidating else newPlain
  }

  private val contextCreatorV3_4 = CommunityRuntimeContextCreator
  private val compiler: v3_4.CypherCompiler[CommunityRuntimeContext] =
    new CypherCompilerFactory().costBasedCompiler(configV3_4, clock, monitorsV3_4, rewriterSequencer,
      Some(plannerName), maybeUpdateStrategy, contextCreatorV3_4)

  private def createQueryGraphSolverV3_4() =
    plannerName match {
      case IDPPlannerNameV3_4 =>
        val monitor = monitors.newMonitor[idpV3_4.IDPQueryGraphSolverMonitor]()
        val solverConfig = new idpV3_4.ConfigurableIDPSolverConfig(
          maxTableSize = configV3_4.idpMaxTableSize,
          iterationDurationLimit = configV3_4.idpIterationDuration
        )
        val singleComponentPlanner = idpV3_4.SingleComponentPlanner(monitor, solverConfig)
        idpV3_4.IDPQueryGraphSolver(singleComponentPlanner, idpV3_4.cartesianProductsOrValueJoins, monitor)

      case DPPlannerNameV3_4 =>
        val monitor = monitors.newMonitor[idpV3_4.IDPQueryGraphSolverMonitor]()
        val singleComponentPlanner = idpV3_4.SingleComponentPlanner(monitor, idpV3_4.DPSolverConfig)
        idpV3_4.IDPQueryGraphSolver(singleComponentPlanner, idpV3_4.cartesianProductsOrValueJoins, monitor)
    }

  private val schemaStateKey = SchemaStateKey.newKey()
  private def checkForSchemaChanges(tcw: TransactionalContextWrapper): Unit =
    tcw.getOrCreateFromSchemaState(schemaStateKey, planCache.clear())

  override def parseAndPlan(preParsedQuery: PreParsedQuery,
                            tracer: CompilationPhaseTracer,
                            transactionalContext: TransactionalContext,
                            params: MapValue
                           ): LogicalPlanResult = {

    val inputPositionV3_4 = as3_4(preParsedQuery.offset)
    val inputPositionv4_0 = preParsedQuery.offset
    val notificationLoggerV3_4 = new RecordingNotificationLoggerV3_4(Some(inputPositionV3_4))
    val notificationLoggerv4_0 = new RecordingNotificationLogger(Some(inputPositionv4_0))

    runSafely {
      val syntacticQuery =
        getOrParse(preParsedQuery, new Parser3_4(compiler, notificationLoggerV3_4, inputPositionV3_4, as3_4(tracer)))

      // Context used for db communication during planning
      val tcv4_0 = TransactionalContextWrapper(transactionalContext)

      // Create graph-statistics to be shared between 3.3 logical planning and 4.0 physical planning
      val graphStatisticsSnapshotv4_0 = new MutableGraphStatisticsSnapshotv4_0()
      val graphStatisticsV3_4 = new WrappedInstrumentedGraphStatistics(
        TransactionBoundGraphStatisticsV3_4(tcv4_0.dataRead, tcv4_0.schemaRead),
        graphStatisticsSnapshotv4_0)

      val planContextV3_4 = new ExceptionTranslatingPlanContextV3_4(
        new TransactionBoundPlanContextV3_4(() => transactionalContext.kernelTransaction,
                                            notificationLoggerV3_4, graphStatisticsV3_4))

      val graphStatisticsv4_0 = InstrumentedGraphStatisticsv4_0(
        TransactionBoundGraphStatisticsV4_0(tcv4_0.dataRead, tcv4_0.schemaRead),
        graphStatisticsSnapshotv4_0)

      val planContextv4_0 = new ExceptionTranslatingPlanContextV4_0(
        new TransactionBoundPlanContextV4_0(tcv4_0, notificationLoggerv4_0, graphStatisticsv4_0))

      // Only used during planning
      def simpleExpressionEvaluatorV3_4 = new logicalV3_4.ExpressionEvaluator {
        override def evaluateExpression(expr: Expression): Option[Any] = None
      }

      // Context used to create logical plans
      val logicalPlanIdGenv3_4 = new utilV3_4.attribution.SequentialIdGen()
      val contextV3_4: CommunityRuntimeContext =
        contextCreatorV3_4.create(as3_4(tracer),
                                  notificationLoggerV3_4,
                                  planContextV3_4,
                                  syntacticQuery.queryText,
                                  preParsedQuery.debugOptions,
                                  Some(inputPositionV3_4),
                                  monitorsV3_4,
                                  logicalV3_4.CachedMetricsFactory(logicalV3_4.SimpleMetricsFactory),
                                  createQueryGraphSolverV3_4(),
                                  configV3_4,
                                  maybeUpdateStrategy.getOrElse(v3_4.defaultUpdateStrategy),
                                  clock,
                                  logicalPlanIdGenv3_4,
                                  simpleExpressionEvaluatorV3_4)

      val logicalPlanIdGenv4_0 = new SequentialIdGen()
      val contextv4_0: PlannerContext =
        PlannerContextCreator.create(tracer,
                                     notificationLoggerv4_0,
                                     planContextv4_0,
                                     syntacticQuery.queryText,
                                     preParsedQuery.debugOptions,
                                     Some(inputPositionv4_0),
                                     monitors,
                                     CachedMetricsFactory(SimpleMetricsFactory),
                                     null,
                                     configv4_0,
                                     maybeUpdateStrategy.map(helpers.as4_0).getOrElse(defaultUpdateStrategy),
                                     clock,
                                     logicalPlanIdGenv4_0,
                                     simpleExpressionEvaluator)

      // Prepare query for caching
      val preparedQuery = compiler.normalizeQuery(syntacticQuery, contextV3_4)
      val queryParamNames: Seq[String] = preparedQuery.statement().findByAllClass[Parameter].map(x => x.name)
      checkForSchemaChanges(tcv4_0)

      // If the query is not cached we do full planning + creating of executable plan
      def createPlan(): CacheableLogicalPlan = {
        val logicalPlanStateV3_4 = compiler.planPreparedQuery(preparedQuery, contextV3_4)
        val logicalPlanStatev4_0 = helpers.as4_0(logicalPlanStateV3_4) // Here we switch from 3.4 to 4.0
        LogicalPlanNotifications
          .checkForNotifications(logicalPlanStatev4_0.maybeLogicalPlan.get, planContextv4_0, configv4_0)
          .foreach(notificationLoggerv4_0.log)

        val reusabilityState = createReusabilityState(logicalPlanStatev4_0, planContextv4_0)
        // Log notifications/warnings from planning
        notificationLoggerV3_4.notifications.map(helpers.as4_0).foreach(notificationLoggerv4_0.log)

        CacheableLogicalPlan(logicalPlanStatev4_0, reusabilityState, notificationLoggerv4_0.notifications, shouldBeCached = true)
      }

      // 3.4 does not produce different plans for different parameter types.
      // Therefore, we always use an empty map as ParameterTypeMap and all plans get cached

      val cacheableLogicalPlan =
        if (preParsedQuery.debugOptions.isEmpty)
          planCache.computeIfAbsentOrStale(Pair.of(syntacticQuery.statement(), Map.empty),
                                           transactionalContext,
                                           createPlan,
                                           _ => None,
                                           syntacticQuery.queryText).executableQuery
        else
          createPlan()

      LogicalPlanResult(
        cacheableLogicalPlan.logicalPlanState,
        queryParamNames,
        ValueConversion.asValues(preparedQuery.extractedParams()),
        cacheableLogicalPlan.reusability,
        contextv4_0,
        cacheableLogicalPlan.notifications,
        cacheableLogicalPlan.shouldBeCached)
    }
  }

  override val name: PlannerName = PlannerNameWithVersion(as4_0(plannerName), CypherVersion.v3_4.name)
}

private[v3_4] class Parser3_4(compiler: v3_4.CypherCompiler[CommunityRuntimeContext],
                              notificationLogger: RecordingNotificationLoggerV3_4,
                              offset: org.neo4j.cypher.internal.util.v3_4.InputPosition,
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
