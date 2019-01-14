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

import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compatibility.v3_4.helpers.{as3_4, as3_5}
import org.neo4j.cypher.internal.compatibility.v3_5.notification.LogicalPlanNotifications
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.helpers.simpleExpressionEvaluator
import org.neo4j.cypher.internal.compatibility.v3_5.{ExceptionTranslatingPlanContext => ExceptionTranslatingPlanContextv3_5}
import org.neo4j.cypher.internal.compiler.v3_4
import org.neo4j.cypher.internal.compiler.v3_4.CypherCompilerFactory
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.{idp => idpV3_4}
import org.neo4j.cypher.internal.compiler.v3_4.planner.{logical => logicalV3_4}
import org.neo4j.cypher.internal.compiler.v3_5.phases.{PlannerContext, PlannerContextCreator}
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.{CachedMetricsFactory, SimpleMetricsFactory}
import org.neo4j.cypher.internal.compiler.v3_5.{CypherPlannerConfiguration, defaultUpdateStrategy}
import org.neo4j.cypher.internal.frontend.v3_4.ast.{Statement => StatementV3_4}
import org.neo4j.cypher.internal.frontend.v3_4.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_4.phases
import org.neo4j.cypher.internal.frontend.v3_4.phases.{BaseState, Monitors => MonitorsV3_4, RecordingNotificationLogger => RecordingNotificationLoggerV3_4}
import org.neo4j.cypher.internal.planner.v3_4.spi.{DPPlannerName => DPPlannerNameV3_4, IDPPlannerName => IDPPlannerNameV3_4}
import org.neo4j.cypher.internal.planner.v3_4.{spi => spiV3_4}
import org.neo4j.cypher.internal.planner.v3_5.spi.{PlanContext, PlannerNameWithVersion, InstrumentedGraphStatistics => InstrumentedGraphStatisticsv3_5, MutableGraphStatisticsSnapshot => MutableGraphStatisticsSnapshotv3_5}
import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.cypher.internal.spi.v3_4.{ExceptionTranslatingPlanContext => ExceptionTranslatingPlanContextV3_4, TransactionBoundGraphStatistics => TransactionBoundGraphStatisticsV3_4, TransactionBoundPlanContext => TransactionBoundPlanContextV3_4}
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
import org.neo4j.cypher.internal.v3_5.frontend.phases.{CompilationPhaseTracer, RecordingNotificationLogger, InternalNotificationLogger => InternalNotificationLoggerv3_5}
import org.neo4j.cypher.internal.v3_5.util.attribution.SequentialIdGen

case class Cypher34Planner(configv3_5: CypherPlannerConfiguration,
                           clock: Clock,
                           kernelMonitors: KernelMonitors,
                           log: Log,
                           plannerOption: CypherPlannerOption,
                           updateStrategy: CypherUpdateStrategy,
                           txIdProvider: () => Long)
  extends BasePlanner[StatementV3_4, BaseState](configv3_5, clock, kernelMonitors, log, txIdProvider) with CypherPlanner {

  val monitorsV3_4: MonitorsV3_4 = WrappedMonitors(kernelMonitors)
  monitorsV3_4.addMonitorListener(logStalePlanRemovalMonitor(logger), "cypher3.4")

  val configV3_4: v3_4.CypherCompilerConfiguration = helpers.as3_4(configv3_5)
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

  override def parserCacheSize: Int = configv3_5.queryCacheSize

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
    val inputPositionv3_5 = preParsedQuery.offset
    val notificationLoggerV3_4 = new RecordingNotificationLoggerV3_4(Some(inputPositionV3_4))
    val notificationLoggerv3_5 = new RecordingNotificationLogger(Some(inputPositionv3_5))

    runSafely {
      val syntacticQuery =
        getOrParse(preParsedQuery, new Parser3_4(compiler, notificationLoggerV3_4, inputPositionV3_4, as3_4(tracer)))

      // Context used for db communication during planning
      val tcv3_5 = TransactionalContextWrapper(transactionalContext)

      // Create graph-statistics to be shared between 3.3 logical planning and 3.5 physical planning
      val graphStatisticsSnapshotv3_5 = new MutableGraphStatisticsSnapshotv3_5()
      val graphStatisticsV3_4 = new WrappedInstrumentedGraphStatistics(
        TransactionBoundGraphStatisticsV3_4(tcv3_5.dataRead, tcv3_5.schemaRead),
        graphStatisticsSnapshotv3_5)

      val planContextV3_4 = new ExceptionTranslatingPlanContextV3_4(
        new TransactionBoundPlanContextV3_4(() => transactionalContext.kernelTransaction,
                                            notificationLoggerV3_4, graphStatisticsV3_4))

      val graphStatisticsv3_5 = InstrumentedGraphStatisticsv3_5(
        TransactionBoundGraphStatistics(tcv3_5.dataRead, tcv3_5.schemaRead),
        graphStatisticsSnapshotv3_5)

      val planContextv3_5 = new ExceptionTranslatingPlanContextv3_5(
        new TransactionBoundPlanContext(tcv3_5, notificationLoggerv3_5, graphStatisticsv3_5))

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

      val logicalPlanIdGenv3_5 = new SequentialIdGen()
      val contextv3_5: PlannerContext =
        PlannerContextCreator.create(tracer,
                                     notificationLoggerv3_5,
                                     planContextv3_5,
                                     syntacticQuery.queryText,
                                     preParsedQuery.debugOptions,
                                     Some(inputPositionv3_5),
                                     monitors,
                                     CachedMetricsFactory(SimpleMetricsFactory),
                                     null,
                                     configv3_5,
                                     maybeUpdateStrategy.map(helpers.as3_5).getOrElse(defaultUpdateStrategy),
                                     clock,
                                     logicalPlanIdGenv3_5,
                                     simpleExpressionEvaluator(PlanningQueryContext(transactionalContext)))

      // Prepare query for caching
      val preparedQuery = compiler.normalizeQuery(syntacticQuery, contextV3_4)
      val queryParamNames: Seq[String] = preparedQuery.statement().findByAllClass[Parameter].map(x => x.name)
      checkForSchemaChanges(tcv3_5)

      // If the query is not cached we do full planning + creating of executable plan
      def createPlan(): CacheableLogicalPlan = {
        val logicalPlanStateV3_4 = compiler.planPreparedQuery(preparedQuery, contextV3_4)
        val logicalPlanStatev3_5 = helpers.as3_5(logicalPlanStateV3_4) // Here we switch from 3.4 to 3.5
        LogicalPlanNotifications
          .checkForNotifications(logicalPlanStatev3_5.maybeLogicalPlan.get, planContextv3_5, configv3_5)
          .foreach(notificationLoggerv3_5.log)

        val reusabilityState = createReusabilityState(logicalPlanStatev3_5, planContextv3_5)
        // Log notifications/warnings from planning
        notificationLoggerV3_4.notifications.map(helpers.as3_5).foreach(notificationLoggerv3_5.log)

        CacheableLogicalPlan(logicalPlanStatev3_5, reusabilityState, notificationLoggerv3_5.notifications)
      }

      // 3.4 does not produce different plans for different parameter types.
      // Therefore, we always use an empty map as ParameterTypeMap

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
        contextv3_5,
        cacheableLogicalPlan.notifications)
    }
  }

  override val name: PlannerName = PlannerNameWithVersion(as3_5(plannerName), CypherVersion.v3_4.name)
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
