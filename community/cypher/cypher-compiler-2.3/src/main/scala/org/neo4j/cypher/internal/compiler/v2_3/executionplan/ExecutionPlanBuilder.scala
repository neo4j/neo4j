/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan

import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders._
import org.neo4j.cypher.internal.compiler.v2_3.pipes._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_3.planner.{CantCompileQueryException, CantHandleQueryException}
import org.neo4j.cypher.internal.compiler.v2_3.profiler.Profiler
import org.neo4j.cypher.internal.compiler.v2_3.spi._
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v2_3.{ExecutionMode, ProfileMode, _}
import org.neo4j.cypher.internal.frontend.v2_3.PeriodicCommitInOpenTransactionException
import org.neo4j.cypher.internal.frontend.v2_3.ast.Statement
import org.neo4j.cypher.internal.frontend.v2_3.notification.InternalNotification
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.helpers.Clock
import org.neo4j.kernel.api.{Statement => KernelStatement}

import scala.annotation.tailrec


trait RunnablePlan {
  def apply(statement: KernelStatement,
            nodeManager: EntityAccessor,
            execMode: ExecutionMode,
            params: Map[String, Any],
            closer: TaskCloser): InternalExecutionResult
}

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

object ExecutablePlanBuilder {

  def create(plannerName: Option[PlannerName], rulePlanProducer: ExecutablePlanBuilder,
             costPlanProducer: ExecutablePlanBuilder, planBuilderMonitor: NewLogicalPlanSuccessRateMonitor,
             useErrorsOverWarnings: Boolean) = plannerName match {
    case None => new SilentFallbackPlanBuilder(rulePlanProducer, costPlanProducer, planBuilderMonitor)
    case Some(_) if useErrorsOverWarnings => new ErrorReportingExecutablePlanBuilder(costPlanProducer)
    case Some(_) => new WarningFallbackPlanBuilder(rulePlanProducer, costPlanProducer, planBuilderMonitor)
  }
}

trait ExecutablePlanBuilder {
  def producePlan(inputQuery: PreparedQuery, planContext: PlanContext, tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING): PipeInfo
}

class ExecutionPlanBuilder(graph: GraphDatabaseService, entityAccessor: EntityAccessor,
                           config: CypherCompilerConfiguration, clock: Clock, pipeBuilder: ExecutablePlanBuilder)
  extends PatternGraphBuilder {

  def build(planContext: PlanContext, inputQuery: PreparedQuery, tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING): ExecutionPlan = {
    val executablePlan = pipeBuilder.producePlan(inputQuery, planContext, tracer)
    buildInterpreted(executablePlan, inputQuery)
  }

  private def checkForNotifications(pipe: Pipe, planContext: PlanContext): Seq[InternalNotification] = {
    val notificationCheckers = Seq(checkForEagerLoadCsv,
      CheckForLoadCsvAndMatchOnLargeLabel(planContext, config.nonIndexedLabelWarningThreshold))

    notificationCheckers.flatMap(_(pipe))
  }

  private def buildInterpreted(pipeInfo: PipeInfo, inputQuery: PreparedQuery) = {
    val abstractQuery = inputQuery.abstractQuery
    val PipeInfo(pipe, updating, periodicCommitInfo, fp, planner) = pipeInfo
    val columns = getQueryResultColumns(abstractQuery, pipe.symbols)
    val resultBuilderFactory = new DefaultExecutionResultBuilderFactory(pipeInfo, columns)
    val func = getExecutionPlanFunction(periodicCommitInfo, abstractQuery.getQueryText, updating, resultBuilderFactory, inputQuery.notificationLogger)
    new ExecutionPlan {
      private val fingerprint = new PlanFingerprintReference(clock, config.queryPlanTTL, config.statsDivergenceThreshold, fp)

     override def run(queryContext: QueryContext, ignored: KernelStatement, planType: ExecutionMode,
                      params: Map[String, Any]) = func(queryContext, planType, params)

      override def isPeriodicCommit = periodicCommitInfo.isDefined
      override def plannerUsed = planner
      override def isStale(lastTxId: () => Long, statistics: GraphStatistics) = fingerprint.isStale(lastTxId, statistics)

      override def runtimeUsed = InterpretedRuntimeName

      override def notifications(planContext: PlanContext) = checkForNotifications(pipe, planContext)
    }

  }

  @tailrec
  private def getQueryResultColumns(q: AbstractQuery, currentSymbols: SymbolTable): List[String] = q match {
    case in: PeriodicCommitQuery =>
      getQueryResultColumns(in.query, currentSymbols)

    case in: Query =>
      // Find the last query part
      var query = in
      while (query.tail.isDefined) {
        query = query.tail.get
      }

      query.returns.columns.flatMap {
        case "*" => currentSymbols.identifiers.keys
        case x => Seq(x)
      }

    case union: Union =>
      getQueryResultColumns(union.queries.head, currentSymbols)

    case _ =>
      List.empty
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
        if (!builderContext.isTopLevelTx)
          throw new PeriodicCommitInOpenTransactionException()
        builder.setLoadCsvPeriodicCommitObserver(periodicCommit.get.batchRowCount)
      }

      if (profiling)
        builder.setPipeDecorator(new Profiler())

      builder.build(queryId, planType, params, notificationLogger)
    }
}

