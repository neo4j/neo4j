/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import java.util.function.BiFunction

import org.neo4j.cypher._
import org.neo4j.cypher.exceptionHandler.runSafely
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.helpers.simpleExpressionEvaluator
import org.neo4j.cypher.internal.compatibility.{CypherPlanner, _}
import org.neo4j.cypher.internal.compiler.v3_5
import org.neo4j.cypher.internal.compiler.v3_5._
import org.neo4j.cypher.internal.compiler.v3_5.phases.{PlannerContext, PlannerContextCreator}
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.idp._
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.{CachedMetricsFactory, SimpleMetricsFactory}
import org.neo4j.cypher.internal.planner.v3_5.spi.{CostBasedPlannerName, DPPlannerName, IDPPlannerName, PlanContext}
import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.cypher.internal.v3_5.ast.Statement
import org.neo4j.cypher.internal.v3_5.expressions.Parameter
import org.neo4j.cypher.internal.v3_5.frontend.PlannerName
import org.neo4j.cypher.internal.v3_5.frontend.phases._
import org.neo4j.cypher.internal.v3_5.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.v3_5.util.InputPosition
import org.neo4j.cypher.internal.v3_5.util.attribution.SequentialIdGen
import org.neo4j.helpers.collection.Pair
import org.neo4j.kernel.impl.api.SchemaStateKey
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

object Cypher35Planner {
  /**
    * This back-door is intended for quick handling of bugs and support cases
    * where we need to inject some specific indexes and statistics.
    */
  var customPlanContextCreator: Option[(TransactionalContextWrapper, InternalNotificationLogger) => PlanContext] = None
}

case class Cypher35Planner(config: CypherPlannerConfiguration,
                           clock: Clock,
                           kernelMonitors: KernelMonitors,
                           log: Log,
                           plannerOption: CypherPlannerOption,
                           updateStrategy: CypherUpdateStrategy,
                           txIdProvider: () => Long)
  extends BasePlanner[Statement, BaseState](config, clock, kernelMonitors, txIdProvider) with CypherPlanner {

  monitors.addMonitorListener(logStalePlanRemovalMonitor(log), "cypher3.5")

  val plannerName: CostBasedPlannerName =
    plannerOption match {
      case CypherPlannerOption.default => CostBasedPlannerName.default
      case CypherPlannerOption.cost | CypherPlannerOption.idp => IDPPlannerName
      case CypherPlannerOption.dp => DPPlannerName
      case _ => throw new IllegalArgumentException(s"unknown cost based planner: ${plannerOption.name}")
    }

  private val maybeUpdateStrategy: Option[UpdateStrategy] = updateStrategy match {
    case CypherUpdateStrategy.eager => Some(eagerUpdateStrategy)
    case _ => None
  }

  protected val rewriterSequencer: String => RewriterStepSequencer = {
    import RewriterStepSequencer._
    import org.neo4j.helpers.Assertion._

    if (assertionsEnabled()) newValidating else newPlain
  }

  private val contextCreator = PlannerContextCreator

  protected val planner: v3_5.CypherPlanner[PlannerContext] =
    new CypherPlannerFactory().costBasedCompiler(config, clock, monitors, rewriterSequencer,
      maybeUpdateStrategy, contextCreator)

  private def createQueryGraphSolver(): IDPQueryGraphSolver =
    plannerName match {
      case IDPPlannerName =>
        val monitor = monitors.newMonitor[IDPQueryGraphSolverMonitor]()
        val solverConfig = new ConfigurableIDPSolverConfig(
          maxTableSize = config.idpMaxTableSize,
          iterationDurationLimit = config.idpIterationDuration
        )
        val singleComponentPlanner = SingleComponentPlanner(monitor, solverConfig)
        IDPQueryGraphSolver(singleComponentPlanner, cartesianProductsOrValueJoins, monitor)

      case DPPlannerName =>
        val monitor = monitors.newMonitor[IDPQueryGraphSolverMonitor]()
        val singleComponentPlanner = SingleComponentPlanner(monitor, DPSolverConfig)
        IDPQueryGraphSolver(singleComponentPlanner, cartesianProductsOrValueJoins, monitor)
    }

  private val schemaStateKey = SchemaStateKey.newKey()
  private def checkForSchemaChanges(tcw: TransactionalContextWrapper): Unit =
    tcw.getOrCreateFromSchemaState(schemaStateKey, planCache.clear())

  override def parseAndPlan(preParsedQuery: PreParsedQuery,
                            tracer: CompilationPhaseTracer,
                            transactionalContext: TransactionalContext,
                            params: MapValue
                           ): LogicalPlanResult = {

    runSafely {
      val notificationLogger = new RecordingNotificationLogger(Some(preParsedQuery.offset))
      val syntacticQuery =
        getOrParse(preParsedQuery, new Parser3_5(planner, notificationLogger, preParsedQuery.offset, tracer))

      // The parser populates the notificationLogger as a side-effect of its work, therefore
      // in the case of a cached query the notificationLogger will not be properly filled
      syntacticQuery.maybeSemantics.map(_.notifications).getOrElse(Set.empty).foreach(notificationLogger.log)

      val transactionalContextWrapper = TransactionalContextWrapper(transactionalContext)
      // Context used for db communication during planning
      val createPlanContext = Cypher35Planner.customPlanContextCreator.getOrElse(TransactionBoundPlanContext.apply _)
      val planContext =
        new ExceptionTranslatingPlanContext(
          createPlanContext(transactionalContextWrapper, notificationLogger)
        )

      // Context used to create logical plans
      val logicalPlanIdGen = new SequentialIdGen()
      val context = contextCreator.create(tracer,
                                          notificationLogger,
                                          planContext,
                                          syntacticQuery.queryText,
                                          preParsedQuery.debugOptions,
                                          Some(preParsedQuery.offset),
                                          monitors,
                                          CachedMetricsFactory(SimpleMetricsFactory),
                                          createQueryGraphSolver(),
                                          config,
                                          maybeUpdateStrategy.getOrElse(defaultUpdateStrategy),
                                          clock,
                                          logicalPlanIdGen,
                                          simpleExpressionEvaluator(PlanningQueryContext(transactionalContext)))

      // Prepare query for caching
      val preparedQuery = planner.normalizeQuery(syntacticQuery, context)
      val queryParamNames: Seq[String] = preparedQuery.statement().findByAllClass[Parameter].map(x => x.name)

      checkForSchemaChanges(transactionalContextWrapper)

      // If the query is not cached we want to do the full planning
      def createPlan(): CacheableLogicalPlan = {
        val logicalPlanState = planner.planPreparedQuery(preparedQuery, context)
        notification.LogicalPlanNotifications
          .checkForNotifications(logicalPlanState.maybeLogicalPlan.get, planContext, config)
          .foreach(notificationLogger.log)

        val reusabilityState = createReusabilityState(logicalPlanState, planContext)
        CacheableLogicalPlan(logicalPlanState, reusabilityState, notificationLogger.notifications)
      }

      // Filter the parameters to retain only those that are actually used in the query
      val filteredParams = params.filter(new BiFunction[String, AnyValue, java.lang.Boolean] {
        override def apply(name: String, value: AnyValue): java.lang.Boolean = queryParamNames.contains(name)
      })

      val cacheableLogicalPlan =
        if (preParsedQuery.debugOptions.isEmpty)
          planCache.computeIfAbsentOrStale(Pair.of(syntacticQuery.statement(), QueryCache.extractParameterTypeMap(filteredParams)),
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
        context,
        cacheableLogicalPlan.notifications)
    }
  }

  override val name: PlannerName = plannerName
}

private[v3_5] class Parser3_5(planner: v3_5.CypherPlanner[PlannerContext],
                              notificationLogger: InternalNotificationLogger,
                              offset: InputPosition,
                              tracer: CompilationPhaseTracer
                             ) extends Parser[BaseState] {

  override def parse(preParsedQuery: PreParsedQuery): BaseState = {
    planner.parseQuery(preParsedQuery.statement,
                       preParsedQuery.rawStatement,
                       notificationLogger,
                       preParsedQuery.planner.name,
                       preParsedQuery.debugOptions,
                       Some(offset),
                       tracer)
  }
}
