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
import org.neo4j.graphdb.Notification
import org.neo4j.helpers.collection.Pair
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log
import org.opencypher.v9_0.ast.Statement
import org.opencypher.v9_0.expressions.Parameter
import org.opencypher.v9_0.frontend.PlannerName
import org.opencypher.v9_0.frontend.phases._
import org.opencypher.v9_0.rewriting.RewriterStepSequencer
import org.opencypher.v9_0.util.InputPosition
import org.opencypher.v9_0.util.attribution.SequentialIdGen

case class Cypher35Planner(config: CypherPlannerConfiguration,
                           clock: Clock,
                           kernelMonitors: KernelMonitors,
                           log: Log,
                           plannerOption: CypherPlannerOption,
                           updateStrategy: CypherUpdateStrategy,
                           txIdProvider: () => Long)
  extends BasePlanner[Statement, BaseState](config, clock, kernelMonitors, log, txIdProvider) with CypherPlanner {

  monitors.addMonitorListener(logStalePlanRemovalMonitor(logger), "cypher3.5")

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

  private def checkForSchemaChanges(planContext: PlanContext): Unit =
    planContext.getOrCreateFromSchemaState(this, planCache.clear())

  override def parseAndPlan(preParsedQuery: PreParsedQuery,
                            tracer: CompilationPhaseTracer,
                            preParsingNotifications: Set[Notification],
                            transactionalContext: TransactionalContext
                           ): LogicalPlanResult = {

    val notificationLogger = new RecordingNotificationLogger(Some(preParsedQuery.offset))

    runSafely {
      val syntacticQuery =
        getOrParse(preParsedQuery, new Parser3_5(planner, notificationLogger, preParsedQuery.offset, tracer))

      // Context used for db communication during planning
      val planContext = new ExceptionTranslatingPlanContext(TransactionBoundPlanContext(
        TransactionalContextWrapper(transactionalContext), notificationLogger))

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
                                          createQueryGraphSolver,
                                          config,
                                          maybeUpdateStrategy.getOrElse(defaultUpdateStrategy),
                                          clock,
                                          logicalPlanIdGen,
                                          simpleExpressionEvaluator)

      // Prepare query for caching
      val preparedQuery = planner.normalizeQuery(syntacticQuery, context)
      val queryParamNames: Seq[String] = preparedQuery.statement().findByAllClass[Parameter].map(x => x.name)

      checkForSchemaChanges(planContext)

      // If the query is not cached we want to do the full planning + creating executable plan
      def createPlan(): CacheableLogicalPlan = {
        val logicalPlanState = planner.planPreparedQuery(preparedQuery, context)
        notification.LogicalPlanNotifications
          .checkForNotifications(logicalPlanState.maybeLogicalPlan.get, planContext, config)
          .foreach(notificationLogger.log)

        val reusabilityState = createReusabilityState(logicalPlanState, planContext)
        CacheableLogicalPlan(logicalPlanState, reusabilityState)
      }

      val params= ValueConversion.asValues(preparedQuery.extractedParams())
      val cacheableLogicalPlan =
        if (preParsedQuery.debugOptions.isEmpty)
          planCache.computeIfAbsentOrStale(Pair.of(syntacticQuery.statement(), params),
                                           transactionalContext,
                                           createPlan,
                                           syntacticQuery.queryText).executableQuery
        else
          createPlan()

      LogicalPlanResult(
        cacheableLogicalPlan.logicalPlanState,
        queryParamNames,
        params,
        cacheableLogicalPlan.reusability,
        context)
    }
  }

  override val name: PlannerName = plannerName
}

private[v3_5] class Parser3_5(planner: v3_5.CypherPlanner[PlannerContext],
                              notificationLogger: RecordingNotificationLogger,
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
