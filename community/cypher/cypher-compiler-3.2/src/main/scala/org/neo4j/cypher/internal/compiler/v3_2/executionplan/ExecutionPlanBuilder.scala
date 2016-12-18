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
package org.neo4j.cypher.internal.compiler.v3_2.executionplan

import org.neo4j.cypher.internal.compiler.v3_2.codegen.QueryExecutionTracer
import org.neo4j.cypher.internal.compiler.v3_2.codegen.profiling.ProfilingTracer
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.ExecutionPlanBuilder.DescriptionProvider
import org.neo4j.cypher.internal.compiler.v3_2.helpers.RuntimeTypeConverter
import org.neo4j.cypher.internal.compiler.v3_2.phases.CompilationState
import org.neo4j.cypher.internal.compiler.v3_2.pipes._
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.{Id, InternalPlanDescription}
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_2.planner.{CantCompileQueryException, CantHandleQueryException}
import org.neo4j.cypher.internal.compiler.v3_2.profiler.Profiler
import org.neo4j.cypher.internal.compiler.v3_2.spi._
import org.neo4j.cypher.internal.compiler.v3_2.{ExecutionMode, ProfileMode, _}
import org.neo4j.cypher.internal.frontend.v3_2.PeriodicCommitInOpenTransactionException
import org.neo4j.cypher.internal.frontend.v3_2.ast.Statement
import org.neo4j.cypher.internal.frontend.v3_2.notification.InternalNotification

trait RunnablePlan {
  def apply(queryContext: QueryContext,
            execMode: ExecutionMode,
            descriptionProvider: DescriptionProvider,
            params: Map[String, Any],
            closer: TaskCloser): InternalExecutionResult
}

case class CompiledPlan(updating: Boolean,
                        periodicCommit: Option[PeriodicCommitInfo] = None,
                        fingerprint: Option[PlanFingerprint] = None,
                        plannerUsed: PlannerName,
                        planDescription: InternalPlanDescription,
                        columns: Seq[String],
                        executionResultBuilder: RunnablePlan )

case class PipeInfo(pipe: Pipe,
                    updating: Boolean,
                    periodicCommit: Option[PeriodicCommitInfo] = None,
                    fingerprint: Option[PlanFingerprint] = None,
                    plannerUsed: PlannerName)

case class PeriodicCommitInfo(size: Option[Long]) {
  def batchRowCount = size.getOrElse(/* defaultSize */ 1000L)
}

trait NewLogicalPlanSuccessRateMonitor {
  def newQuerySeen(queryText: String, ast:Statement)
  def unableToHandleQuery(queryText: String, ast:Statement, origin: CantHandleQueryException)
}

trait NewRuntimeSuccessRateMonitor {
  def newPlanSeen(plan: LogicalPlan)
  def unableToHandlePlan(plan: LogicalPlan, origin: CantCompileQueryException)
}

trait ExecutablePlanBuilder {

  def producePlan(inputQuery: CompilationState, planContext: PlanContext,
                  tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING,
                  createFingerprintReference: (Option[PlanFingerprint]) => PlanFingerprintReference): ExecutionPlan
}

object InterpretedExecutionPlanBuilder {
  def interpretedToExecutionPlan(pipeInfo: PipeInfo,
                                 planContext: PlanContext,
                                 inputQuery: CompilationState,
                                 createFingerprintReference: Option[PlanFingerprint] => PlanFingerprintReference,
                                 config: CypherCompilerConfiguration,
                                 typeConverter: RuntimeTypeConverter,
                                 logicalPlan: LogicalPlan,
                                 idMap: Map[LogicalPlan, Id]) = {
    val PipeInfo(pipe, updating, periodicCommitInfo, fp, planner) = pipeInfo
    val columns = inputQuery.statement.returnColumns
    val resultBuilderFactory = DefaultExecutionResultBuilderFactory(pipeInfo, columns, typeConverter, logicalPlan, idMap)
    val func = getExecutionPlanFunction(periodicCommitInfo, inputQuery.queryText, updating, resultBuilderFactory, planContext.notificationLogger())
    new ExecutionPlan {
      private val fingerprint = createFingerprintReference(fp)

      override def run(queryContext: QueryContext, planType: ExecutionMode, params: Map[String, Any]) =
        func(queryContext, planType, params)

      override def isPeriodicCommit = periodicCommitInfo.isDefined
      override def plannerUsed = planner
      override def isStale(lastTxId: () => Long, statistics: GraphStatistics) = fingerprint.isStale(lastTxId, statistics)

      override def runtimeUsed = InterpretedRuntimeName

      override def notifications(planContext: PlanContext) = checkForNotifications(pipe, planContext, config)
    }
  }

  private def checkForNotifications(pipe: Pipe, planContext: PlanContext, config: CypherCompilerConfiguration): Seq[InternalNotification] = {
    val notificationCheckers = Seq(checkForEagerLoadCsv,
      CheckForLoadCsvAndMatchOnLargeLabel(planContext, config.nonIndexedLabelWarningThreshold))

    notificationCheckers.flatMap(_(pipe))
  }

  private def getExecutionPlanFunction(periodicCommit: Option[PeriodicCommitInfo],
                                       queryId: AnyRef,
                                       updating: Boolean,
                                       resultBuilderFactory: ExecutionResultBuilderFactory,
                                       notificationLogger: InternalNotificationLogger):
  (QueryContext, ExecutionMode, Map[String, Any]) => InternalExecutionResult =
    (queryContext: QueryContext, planType: ExecutionMode, params: Map[String, Any]) => {
      val builder = resultBuilderFactory.create()

      val profiling = planType == ProfileMode
      val builderContext = if (updating || profiling) new UpdateCountingQueryContext(queryContext) else queryContext
      builder.setQueryContext(builderContext)

      if (periodicCommit.isDefined) {
        if (!builderContext.transactionalContext.isTopLevelTx)
          throw new PeriodicCommitInOpenTransactionException()
        builder.setLoadCsvPeriodicCommitObserver(periodicCommit.get.batchRowCount)
      }

      if (profiling)
        builder.setPipeDecorator(new Profiler())

      builder.build(queryId, planType, params, notificationLogger)
    }
}

object ExecutionPlanBuilder {
  type DescriptionProvider =
        (InternalPlanDescription => (Provider[InternalPlanDescription], Option[QueryExecutionTracer]))

  def tracer( mode: ExecutionMode ) : DescriptionProvider = mode match {
    case ProfileMode =>
      val tracer = new ProfilingTracer()
      (description: InternalPlanDescription) => (new Provider[InternalPlanDescription] {

        override def get(): InternalPlanDescription = description.map {
          plan: InternalPlanDescription =>
            val data = tracer.get(plan.id)
            plan.
              addArgument(Arguments.DbHits(data.dbHits())).
              addArgument(Arguments.Rows(data.rows())).
              addArgument(Arguments.Time(data.time()))
        }
      }, Some(tracer))
    case _ => (description: InternalPlanDescription) => (new Provider[InternalPlanDescription] {
      override def get(): InternalPlanDescription = description
    }, None)
  }
}
