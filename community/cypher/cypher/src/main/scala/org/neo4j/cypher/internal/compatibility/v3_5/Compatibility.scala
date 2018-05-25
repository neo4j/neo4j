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
import org.opencypher.v9_0.ast.Statement
import org.opencypher.v9_0.frontend.phases._
import org.neo4j.cypher.internal.planner.v3_5.spi.{CostBasedPlannerName, DPPlannerName, IDPPlannerName, PlanContext}
import org.neo4j.cypher.internal.runtime.interpreted._
import org.opencypher.v9_0.util.attribution.SequentialIdGen
import org.opencypher.v9_0.expressions.Parameter
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log
import org.opencypher.v9_0.rewriting.RewriterStepSequencer

import scala.util.Try

case class Compatibility[CONTEXT <: CommunityRuntimeContext,
                    T <: Transformer[CONTEXT, LogicalPlanState, CompilationState]](config: CypherPlannerConfiguration,
                                                                                   clock: Clock,
                                                                                   kernelMonitors: KernelMonitors,
                                                                                   log: Log,
                                                                                   planner: CypherPlannerOption,
                                                                                   runtime: CypherRuntimeOption,
                                                                                   updateStrategy: CypherUpdateStrategy,
                                                                                   runtimeBuilder: RuntimeBuilder[T],
                                                                                   contextCreatorv3_5: ContextCreator[CONTEXT],
                                                                                   txIdProvider: () => Long)
  extends LatestRuntimeVariablePlannerCompatibility[CONTEXT, T, Statement](config,
                                                                           clock,
                                                                           kernelMonitors,
                                                                           log,
                                                                           planner,
                                                                           runtime,
                                                                           updateStrategy,
                                                                           runtimeBuilder,
                                                                           contextCreatorv3_5,
                                                                           txIdProvider) {

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
      override def plan(transactionalContext: TransactionalContext, planningTracer: CompilationPhaseTracer):
        (ExecutionPlan, Map[String, Any], Seq[String]) = runSafely {
        val syntacticQuery = preparedSyntacticQueryForV_3_4.get

        //Context used for db communication during planning
        val planContext = new ExceptionTranslatingPlanContext(TransactionBoundPlanContext(
                                    TransactionalContextWrapper(transactionalContext), notificationLogger))
        //Context used to create logical plans
        val logicalPlanIdGen = new SequentialIdGen()
        val context = contextCreatorv3_5.create(planningTracer, notificationLogger, planContext,
                                                        syntacticQuery.queryText, preParsedQuery.debugOptions,
                                                        Some(preParsedQuery.offset), monitors,
                                                        CachedMetricsFactory(SimpleMetricsFactory), queryGraphSolver,
                                                        config, maybeUpdateStrategy.getOrElse(defaultUpdateStrategy),
                                                        clock, logicalPlanIdGen, simpleExpressionEvaluator)
        //Prepare query for caching
        val preparedQuery = compiler.normalizeQuery(syntacticQuery, context)
        val queryParamNames: Seq[String] = preparedQuery.statement().findByAllClass[Parameter].map(x => x.name)

        checkForSchemaChanges(planContext)

        //Just in the case the query is not in the cache do we want to do the full planning + creating executable plan
        def createPlan(): ExecutionPlan_v3_5 = {
          val logicalPlanState = compiler.planPreparedQuery(preparedQuery, context)
          notification.LogicalPlanNotifications
            .checkForNotifications(logicalPlanState.maybeLogicalPlan.get, planContext, config)
            .foreach(notificationLogger.log)

          val result = createExecPlan.transform(logicalPlanState, context)
          result.maybeExecutionPlan.get
        }

        val executionPlan =
          if (preParsedQuery.debugOptions.isEmpty)
            planCache.computeIfAbsentOrStale(syntacticQuery.statement(),
                                             transactionalContext,
                                             createPlan,
                                             syntacticQuery.queryText).executableQuery
          else
            createPlan()

        (new ExecutionPlanWrapper(executionPlan, preParsingNotifications), preparedQuery.extractedParams(), queryParamNames)
      }

      override protected val trier: Try[BaseState] = preparedSyntacticQueryForV_3_4
    }
  }

  private def checkForSchemaChanges(planContext: PlanContext): Unit =
    planContext.getOrCreateFromSchemaState(this, planCache.clear())

  override val runSafelyDuringPlanning: RunSafely = runSafely
  override val runSafelyDuringRuntime: RunSafely = runSafely
}
