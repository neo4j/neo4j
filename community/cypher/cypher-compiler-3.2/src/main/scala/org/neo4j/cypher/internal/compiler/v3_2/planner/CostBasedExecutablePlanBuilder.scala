/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.planner

import org.neo4j.cypher.internal.compiler.v3_2.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.{ExecutablePlanBuilder, NewRuntimeSuccessRateMonitor, PlanFingerprint, PlanFingerprintReference}
import org.neo4j.cypher.internal.compiler.v3_2.helpers.closing
import org.neo4j.cypher.internal.compiler.v3_2.phases.CompilationState
import org.neo4j.cypher.internal.compiler.v3_2.planner.execution.PipeExecutionBuilderContext
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical._
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.v3_2.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v3_2.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.{InternalException, SemanticTable}
import org.neo4j.cypher.internal.ir.v3_2.PeriodicCommit

/* This class is responsible for taking a query from an AST object to a runnable object.  */
case class CostBasedExecutablePlanBuilder(monitors: Monitors,
                                          metricsFactory: MetricsFactory,
                                          queryPlanner: QueryPlanner,
                                          queryGraphSolver: QueryGraphSolver,
                                          rewriterSequencer: (String) => RewriterStepSequencer,
                                          plannerName: CostBasedPlannerName,
                                          runtimeBuilder: RuntimeBuilder,
                                          updateStrategy: UpdateStrategy,
                                          config: CypherCompilerConfiguration,
                                          publicTypeConverter: Any => Any)
  extends ExecutablePlanBuilder {

  override def producePlan(input: CompilationState,
                           planContext: PlanContext,
                           tracer: CompilationPhaseTracer,
                           createFingerprintReference: (Option[PlanFingerprint]) => PlanFingerprintReference) = {
    //monitor success of compilation
    val planBuilderMonitor = monitors.newMonitor[NewRuntimeSuccessRateMonitor](CypherCompilerFactory.monitorTag)

    input.statement match {
      case ast: Query =>
        val (periodicCommit, logicalPlan, pipeBuildContext) = closing(tracer.beginPhase(LOGICAL_PLANNING)) {
          produceLogicalPlan(ast, input.unionQuery, input.semanticTable)(planContext, planContext.notificationLogger())
        }
          runtimeBuilder(periodicCommit, logicalPlan, pipeBuildContext, planContext, tracer, input.semanticTable,
                         planBuilderMonitor, plannerName, input, createFingerprintReference, config)
      case x =>
        throw new InternalException(s"Can't plan a $x query with the cost planner")
    }
  }

  private def produceLogicalPlan(ast: Query, original: UnionQuery, semanticTable: SemanticTable)
                        (planContext: PlanContext, notificationLogger: InternalNotificationLogger):
  (Option[PeriodicCommit], LogicalPlan, PipeExecutionBuilderContext) = {

    val metrics = metricsFactory.newMetrics(planContext.statistics)
    val logicalPlanProducer = LogicalPlanProducer(metrics.cardinality)

    val context = LogicalPlanningContext(planContext, logicalPlanProducer, metrics, semanticTable,
      queryGraphSolver, notificationLogger = notificationLogger, useErrorsOverWarnings = config.useErrorsOverWarnings,
      errorIfShortestPathFallbackUsedAtRuntime = config.errorIfShortestPathFallbackUsedAtRuntime,
      config = QueryPlannerConfiguration.default.withUpdateStrategy(updateStrategy))

    val (periodicCommit, plan) = queryPlanner.plan(original)(context)

    val pipeBuildContext = PipeExecutionBuilderContext(metrics.cardinality, semanticTable, plannerName)

    //Check for unresolved tokens for read-only queries
    if (plan.solved.all(_.queryGraph.readOnly)) checkForUnresolvedTokens(ast, semanticTable).foreach(notificationLogger.log)

    (periodicCommit, plan, pipeBuildContext)
  }
}