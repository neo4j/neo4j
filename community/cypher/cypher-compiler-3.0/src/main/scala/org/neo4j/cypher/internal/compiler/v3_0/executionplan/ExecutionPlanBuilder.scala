/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.executionplan

import org.neo4j.cypher.internal.compiler.v3_0.codegen.QueryExecutionTracer
import org.neo4j.cypher.internal.compiler.v3_0.codegen.profiling.ProfilingTracer
import org.neo4j.cypher.internal.compiler.v3_0.commands._
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.ExecutionPlanBuilder.{DescriptionProvider, tracer}
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.builders._
import org.neo4j.cypher.internal.compiler.v3_0.pipes._
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.Cardinality
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_0.planner.{CantCompileQueryException, CantHandleQueryException}
import org.neo4j.cypher.internal.compiler.v3_0.profiler.Profiler
import org.neo4j.cypher.internal.compiler.v3_0.spi._
import org.neo4j.cypher.internal.compiler.v3_0.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v3_0.{ExecutionMode, ProfileMode, _}
import org.neo4j.cypher.internal.frontend.v3_0.{LabelId, PeriodicCommitInOpenTransactionException}
import org.neo4j.cypher.internal.frontend.v3_0.ast.Statement
import org.neo4j.cypher.internal.frontend.v3_0.notification.{LargeLabelWithLoadCsvNotification, EagerLoadCsvNotification, InternalNotification}
import org.neo4j.function.Supplier
import org.neo4j.function.Suppliers.singleton
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.QueryExecutionType.QueryType
import org.neo4j.helpers.Clock
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.api.{Statement => KernelStatement}
import org.neo4j.kernel.impl.core.NodeManager


trait RunnablePlan {
  def apply(statement: KernelStatement,
            nodeManager: NodeManager,
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
  def producePlan(inputQuery: PreparedQuery, planContext: PlanContext, tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING): Either[CompiledPlan, PipeInfo]
}

class ExecutionPlanBuilder(graph: GraphDatabaseService, config: CypherCompilerConfiguration,
                           clock: Clock, pipeBuilder: ExecutablePlanBuilder) extends PatternGraphBuilder {
  val nodeManager = {
    val gdapi = graph.asInstanceOf[GraphDatabaseAPI]
    gdapi.getDependencyResolver.resolveDependency(classOf[NodeManager])
  }

  def build(planContext: PlanContext, inputQuery: PreparedQuery, tracer: CompilationPhaseTracer=CompilationPhaseTracer.NO_TRACING): ExecutionPlan = {
    val executablePlan = pipeBuilder.producePlan(inputQuery, planContext, tracer)
    executablePlan match {
      case Left(compiledPlan) => buildCompiled(compiledPlan, planContext, inputQuery)
      case Right(pipeInfo) => buildInterpreted(pipeInfo, planContext, inputQuery)
    }
  }

  private def buildCompiled(compiledPlan: CompiledPlan, planContext: PlanContext, inputQuery: PreparedQuery) = {
    new ExecutionPlan {
      val fingerprint = PlanFingerprintReference(clock, config.queryPlanTTL, config.statsDivergenceThreshold, compiledPlan.fingerprint)

      def isStale(lastTxId: () => Long, statistics: GraphStatistics) = fingerprint.isStale(lastTxId, statistics)

      def run(queryContext: QueryContext, kernelStatement: KernelStatement,
              executionMode: ExecutionMode, params: Map[String, Any]): InternalExecutionResult = {
        val taskCloser = new TaskCloser
        taskCloser.addTask(queryContext.close)
        try {
          if (executionMode == ExplainMode) {
            //close all statements
            taskCloser.close(success = true)
            new ExplainExecutionResult(compiledPlan.columns.toList,
              compiledPlan.planDescription, QueryType.READ_ONLY, inputQuery.notificationLogger.notifications)
          } else
            compiledPlan.executionResultBuilder(kernelStatement, nodeManager, executionMode, tracer(executionMode), params, taskCloser)
        } catch {
          case (t: Throwable) =>
            taskCloser.close(success = false)
            throw t
        }
      }

      def plannerUsed: PlannerName = compiledPlan.plannerUsed

      def isPeriodicCommit: Boolean = compiledPlan.periodicCommit.isDefined

      def runtimeUsed = CompiledRuntimeName

      def notifications = Seq.empty
    }
  }



  private def checkForNotifications(pipe: Pipe, planContext: PlanContext): Seq[InternalNotification] = {
    val notificationCheckers = Seq(checkForEagerLoadCsv,
      CheckForLoadCsvAndMatchOnLargeLabel(planContext, config.nonIndexedLabelWarningThreshold))

    notificationCheckers.flatMap(_(pipe))
  }


  private def buildInterpreted(pipeInfo: PipeInfo, planContext: PlanContext, inputQuery: PreparedQuery) = {
    val abstractQuery = inputQuery.abstractQuery
    val PipeInfo(pipe, updating, periodicCommitInfo, fp, planner) = pipeInfo
    val columns = getQueryResultColumns(abstractQuery, pipe.symbols)
    val resultBuilderFactory = new DefaultExecutionResultBuilderFactory(pipeInfo, columns)
    val func = getExecutionPlanFunction(periodicCommitInfo, abstractQuery.getQueryText, updating, resultBuilderFactory, inputQuery.notificationLogger)
    new ExecutionPlan {
      private val fingerprint = PlanFingerprintReference(clock, config.queryPlanTTL, config.statsDivergenceThreshold, fp)

      def run(queryContext: QueryContext, ignored: KernelStatement, planType: ExecutionMode, params: Map[String, Any]) =
        func(queryContext, planType, params)

      def isPeriodicCommit = periodicCommitInfo.isDefined
      def plannerUsed = planner
      def isStale(lastTxId: () => Long, statistics: GraphStatistics) = fingerprint.isStale(lastTxId, statistics)

      def runtimeUsed = InterpretedRuntimeName

      def notifications = checkForNotifications(pipe, planContext)
    }

  }

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

object ExecutionPlanBuilder {
  type DescriptionProvider = (InternalPlanDescription => (Supplier[InternalPlanDescription], Option[QueryExecutionTracer]))

  def tracer( mode: ExecutionMode ) : DescriptionProvider = mode match {
    case ProfileMode =>
      val tracer = new ProfilingTracer()
      (description: InternalPlanDescription) => (new Supplier[InternalPlanDescription] {

        override def get(): InternalPlanDescription = description.map {
          plan: InternalPlanDescription =>
            val data = tracer.get(plan.id)
            plan.
              addArgument(Arguments.DbHits(data.dbHits())).
              addArgument(Arguments.Rows(data.rows())).
              addArgument(Arguments.Time(data.time()))
        }
      }, Some(tracer))
    case _ => (description: InternalPlanDescription) => (singleton(description), None)
  }
}
