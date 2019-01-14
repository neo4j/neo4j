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
package org.neo4j.cypher.internal.compatibility.v3_5

import java.time.Clock

import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compatibility.v3_5.helpers.as3_5
import org.neo4j.cypher.internal.compatibility.v3_5.helpers.as4_0
import org.neo4j.cypher.internal.compatibility.v4_0.notification.LogicalPlanNotifications
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.helpers.simpleExpressionEvaluator
import org.neo4j.cypher.internal.compiler.v3_5
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.{idp => idpV3_5}
import org.neo4j.cypher.internal.compiler.v3_5.planner.{logical => logicalV3_5}
import org.neo4j.cypher.internal.compiler.v4_0.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.v4_0.phases.PlannerContextCreator
import org.neo4j.cypher.internal.compiler.v4_0.planner.logical.CachedMetricsFactory
import org.neo4j.cypher.internal.compiler.v4_0.planner.logical.SimpleMetricsFactory
import org.neo4j.cypher.internal.compiler.v4_0.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.v4_0.defaultUpdateStrategy
import org.neo4j.cypher.internal.planner.v3_5.spi.{DPPlannerName => DPPlannerNameV3_5}
import org.neo4j.cypher.internal.planner.v3_5.spi.{IDPPlannerName => IDPPlannerNameV3_5}
import org.neo4j.cypher.internal.planner.v3_5.{spi => spiV3_5}
import org.neo4j.cypher.internal.planner.v4_0.spi.PlannerNameWithVersion
import org.neo4j.cypher.internal.planner.v4_0.spi.{InstrumentedGraphStatistics => InstrumentedGraphStatisticsv4_0}
import org.neo4j.cypher.internal.planner.v4_0.spi.{MutableGraphStatisticsSnapshot => MutableGraphStatisticsSnapshotv4_0}
import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.cypher.internal.spi.v3_5.{TransactionBoundGraphStatistics => TransactionBoundGraphStatisticsV3_5}
import org.neo4j.cypher.internal.spi.v3_5.{ExceptionTranslatingPlanContext => ExceptionTranslatingPlanContextV3_5}
import org.neo4j.cypher.internal.spi.v3_5.{TransactionBoundPlanContext => TransactionBoundPlanContextV3_5}
import org.neo4j.cypher.internal.spi.v4_0.{TransactionBoundGraphStatistics => TransactionBoundGraphStatisticsV4_0}
import org.neo4j.cypher.internal.spi.v4_0.{ExceptionTranslatingPlanContext => ExceptionTranslatingPlanContextV4_0}
import org.neo4j.cypher.internal.spi.v4_0.{TransactionBoundPlanContext => TransactionBoundPlanContextV4_0}
import org.neo4j.cypher.internal.v4_0.frontend.PlannerName
import org.neo4j.cypher.internal.v4_0.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.v4_0.frontend.phases.RecordingNotificationLogger
import org.neo4j.cypher.internal.v4_0.util.attribution.SequentialIdGen
import org.neo4j.cypher.CypherPlannerOption
import org.neo4j.cypher.CypherUpdateStrategy
import org.neo4j.cypher.CypherVersion
import org.neo4j.helpers.collection.Pair
import org.neo4j.kernel.impl.api.SchemaStateKey
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log
import org.neo4j.values.virtual.MapValue
import org.neo4j.cypher.internal.v3_5.ast.{Statement => StatementV3_5}
import org.neo4j.cypher.internal.v3_5.expressions.Expression
import org.neo4j.cypher.internal.v3_5.expressions.Parameter
import org.neo4j.cypher.internal.v3_5.frontend.phases
import org.neo4j.cypher.internal.v3_5.frontend.phases.BaseState
import org.neo4j.cypher.internal.v3_5.frontend.phases.{RecordingNotificationLogger => RecordingNotificationLoggerV3_5}
import org.neo4j.cypher.internal.v3_5.frontend.phases.{Monitors => MonitorsV3_5}
import org.neo4j.cypher.internal.v3_5.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.v3_5.{util => utilV3_5}

case class Cypher3_5Planner(configv4_0: CypherPlannerConfiguration,
                            clock: Clock,
                            kernelMonitors: KernelMonitors,
                            log: Log,
                            plannerOption: CypherPlannerOption,
                            updateStrategy: CypherUpdateStrategy,
                            txIdProvider: () => Long)
  extends BasePlanner[StatementV3_5, BaseState](configv4_0, clock, kernelMonitors, log, txIdProvider) with CypherPlanner {

  val monitorsV3_5: MonitorsV3_5 = WrappedMonitors(kernelMonitors)
  monitorsV3_5.addMonitorListener(logStalePlanRemovalMonitor(logger), "cypher3.5")

  val configV3_5: v3_5.CypherPlannerConfiguration = helpers.as3_5(configv4_0)
  val plannerName: spiV3_5.CostBasedPlannerName = plannerOption match {
    case CypherPlannerOption.default => spiV3_5.CostBasedPlannerName.default
    case CypherPlannerOption.cost | CypherPlannerOption.idp => IDPPlannerNameV3_5
    case CypherPlannerOption.dp => DPPlannerNameV3_5
    case _ => throw new IllegalArgumentException(s"unknown cost based planner: ${plannerOption.name}")
  }

  val maybeUpdateStrategy: Option[v3_5.UpdateStrategy] = updateStrategy match {
    case CypherUpdateStrategy.eager => Some(v3_5.eagerUpdateStrategy)
    case _ => None
  }

  override def parserCacheSize: Int = configv4_0.queryCacheSize

  val rewriterSequencer: (String) => RewriterStepSequencer = {
    import RewriterStepSequencer._
    import org.neo4j.helpers.Assertion._

    if (assertionsEnabled()) newValidating else newPlain
  }

  private val contextCreatorV3_5 = CommunityRuntimeContextCreator
  private val compiler: v3_5.CypherPlanner[CommunityRuntimeContext] =
    new v3_5.CypherPlannerFactory().costBasedCompiler(configV3_5, clock, monitorsV3_5, rewriterSequencer,
      maybeUpdateStrategy, contextCreatorV3_5)

  private def createQueryGraphSolverV3_5() =
    plannerName match {
      case IDPPlannerNameV3_5 =>
        val monitor = monitors.newMonitor[idpV3_5.IDPQueryGraphSolverMonitor]()
        val solverConfig = new idpV3_5.ConfigurableIDPSolverConfig(
          maxTableSize = configV3_5.idpMaxTableSize,
          iterationDurationLimit = configV3_5.idpIterationDuration
        )
        val singleComponentPlanner = idpV3_5.SingleComponentPlanner(monitor, solverConfig)
        idpV3_5.IDPQueryGraphSolver(singleComponentPlanner, idpV3_5.cartesianProductsOrValueJoins, monitor)

      case DPPlannerNameV3_5 =>
        val monitor = monitors.newMonitor[idpV3_5.IDPQueryGraphSolverMonitor]()
        val singleComponentPlanner = idpV3_5.SingleComponentPlanner(monitor, idpV3_5.DPSolverConfig)
        idpV3_5.IDPQueryGraphSolver(singleComponentPlanner, idpV3_5.cartesianProductsOrValueJoins, monitor)
    }

  private val schemaStateKey = SchemaStateKey.newKey()
  private def checkForSchemaChanges(tcw: TransactionalContextWrapper): Unit =
    tcw.getOrCreateFromSchemaState(schemaStateKey, planCache.clear())

  override def parseAndPlan(preParsedQuery: PreParsedQuery,
                            tracer: CompilationPhaseTracer,
                            transactionalContext: TransactionalContext,
                            params: MapValue
                           ): LogicalPlanResult = {

    val inputPositionV3_5 = as3_5(preParsedQuery.offset)
    val inputPositionv4_0 = preParsedQuery.offset
    val notificationLoggerV3_5 = new RecordingNotificationLoggerV3_5(Some(inputPositionV3_5))
    val notificationLoggerv4_0 = new RecordingNotificationLogger(Some(inputPositionv4_0))

    runSafely {
      val syntacticQuery =
        getOrParse(preParsedQuery, new Parser3_5(compiler, notificationLoggerV3_5, inputPositionV3_5, as3_5(tracer)))

      // Context used for db communication during planning
      val tcv4_0 = TransactionalContextWrapper(transactionalContext)

      // Create graph-statistics to be shared between 3.3 logical planning and 4.0 physical planning
      val graphStatisticsSnapshotv4_0 = new MutableGraphStatisticsSnapshotv4_0()
      val graphStatisticsV3_5 = new WrappedInstrumentedGraphStatistics(
        TransactionBoundGraphStatisticsV3_5(tcv4_0.dataRead, tcv4_0.schemaRead),
        graphStatisticsSnapshotv4_0)

      val planContextV3_5 = new ExceptionTranslatingPlanContextV3_5(
        new TransactionBoundPlanContextV3_5(() => transactionalContext.kernelTransaction,
                                            notificationLoggerV3_5, graphStatisticsV3_5))

      val graphStatisticsv4_0 = InstrumentedGraphStatisticsv4_0(
        TransactionBoundGraphStatisticsV4_0(tcv4_0.dataRead, tcv4_0.schemaRead),
        graphStatisticsSnapshotv4_0)

      val planContextv4_0 = new ExceptionTranslatingPlanContextV4_0(
        new TransactionBoundPlanContextV4_0(tcv4_0, notificationLoggerv4_0, graphStatisticsv4_0))

      // Only used during planning
      def simpleExpressionEvaluatorV3_5 = new logicalV3_5.ExpressionEvaluator {
        override def evaluateExpression(expr: Expression): Option[Any] = None
      }

      // Context used to create logical plans
      val logicalPlanIdGenv3_5 = new utilV3_5.attribution.SequentialIdGen()
      val contextV3_5: CommunityRuntimeContext =
        contextCreatorV3_5.create(as3_5(tracer),
                                  notificationLoggerV3_5,
                                  planContextV3_5,
                                  syntacticQuery.queryText,
                                  preParsedQuery.debugOptions,
                                  Some(inputPositionV3_5),
                                  monitorsV3_5,
                                  logicalV3_5.CachedMetricsFactory(logicalV3_5.SimpleMetricsFactory),
                                  createQueryGraphSolverV3_5(),
                                  configV3_5,
                                  maybeUpdateStrategy.getOrElse(v3_5.defaultUpdateStrategy),
                                  clock,
                                  logicalPlanIdGenv3_5,
                                  simpleExpressionEvaluatorV3_5)

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
      val preparedQuery = compiler.normalizeQuery(syntacticQuery, contextV3_5)
      val queryParamNames: Seq[String] = preparedQuery.statement().findByAllClass[Parameter].map(x => x.name)
      checkForSchemaChanges(tcv4_0)

      // If the query is not cached we do full planning + creating of executable plan
      def createPlan(): CacheableLogicalPlan = {
        val logicalPlanStateV3_5 = compiler.planPreparedQuery(preparedQuery, contextV3_5)
        val logicalPlanStatev4_0 = helpers.as4_0(logicalPlanStateV3_5) // Here we switch from 3.5 to 4.0
        LogicalPlanNotifications
          .checkForNotifications(logicalPlanStatev4_0.maybeLogicalPlan.get, planContextv4_0, configv4_0)
          .foreach(notificationLoggerv4_0.log)

        val reusabilityState = createReusabilityState(logicalPlanStatev4_0, planContextv4_0)
        // Log notifications/warnings from planning
        notificationLoggerV3_5.notifications.map(helpers.as4_0).foreach(notificationLoggerv4_0.log)

        CacheableLogicalPlan(logicalPlanStatev4_0, reusabilityState, notificationLoggerv4_0.notifications, shouldBeCached = true)
      }

      // 3.5 does not produce different plans for different parameter types.
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

  override val name: PlannerName = PlannerNameWithVersion(as4_0(plannerName), CypherVersion.v3_5.name)
}

private[v3_5] class Parser3_5(compiler: v3_5.CypherPlanner[CommunityRuntimeContext],
                              notificationLogger: RecordingNotificationLoggerV3_5,
                              offset: org.neo4j.cypher.internal.v3_5.util.InputPosition,
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
