/**
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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.ast.Statement
import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders._
import org.neo4j.cypher.internal.compiler.v2_3.pipes._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_3.planner.{CantCompileQueryException, CantHandleQueryException}
import org.neo4j.cypher.internal.compiler.v2_3.profiler.Profiler
import org.neo4j.cypher.internal.compiler.v2_3.spi._
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.{ExecutionMode, ProfileMode}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.helpers.Clock
import org.neo4j.kernel.api.{Statement => KernelStatement}


case class CompiledPlan(updating: Boolean,
                        periodicCommit: Option[PeriodicCommitInfo] = None,
                        fingerprint: Option[PlanFingerprint] = None,
                        plannerUsed: PlannerName,
                        executionResultBuilder: (KernelStatement, GraphDatabaseService, ExecutionMode, Map[String, Any]) => InternalExecutionResult)

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
  def producePlan(inputQuery: PreparedQuery, planContext: PlanContext): Either[CompiledPlan, PipeInfo]
}

class ExecutionPlanBuilder(graph: GraphDatabaseService, statsDivergenceThreshold: Double, queryPlanTTL: Long,
                           clock: Clock, pipeBuilder: ExecutablePlanBuilder) extends PatternGraphBuilder {
  def build(planContext: PlanContext, inputQuery: PreparedQuery): ExecutionPlan = {
    val executablePlan = pipeBuilder.producePlan(inputQuery, planContext)
    executablePlan match {
      case Left(compiledPlan) => buildCompiled(compiledPlan, planContext, inputQuery)
      case Right(pipeInfo) => buildInterpreted(pipeInfo, planContext, inputQuery)
    }
  }

  private def buildCompiled(compiledPlan: CompiledPlan, planContext: PlanContext, inputQuery: PreparedQuery) = {
    new ExecutionPlan {
      val fingerprint = PlanFingerprintReference(clock, queryPlanTTL, statsDivergenceThreshold, compiledPlan.fingerprint)

      def isStale(lastTxId: () => Long, statistics: GraphStatistics) = fingerprint.isStale(lastTxId, statistics)

      def run(queryContext: QueryContext, kernelStatement: KernelStatement,
                       planType: ExecutionMode, params: Map[String, Any]): InternalExecutionResult =
        compiledPlan.executionResultBuilder(kernelStatement, graph, planType, params)

      def plannerUsed: PlannerName = CostPlannerName

      def isPeriodicCommit: Boolean = compiledPlan.periodicCommit.isDefined

      def runtimeUsed = CompiledRuntimeName
    }
  }

  private def buildInterpreted(pipeInfo: PipeInfo, planContext: PlanContext, inputQuery: PreparedQuery) = {
    val abstractQuery = inputQuery.abstractQuery
    val PipeInfo(pipe, updating, periodicCommitInfo, fp, planner) = pipeInfo

    val columns = getQueryResultColumns(abstractQuery, pipe.symbols)
    val resultBuilderFactory = new DefaultExecutionResultBuilderFactory(pipeInfo, columns)
    val func = getExecutionPlanFunction(periodicCommitInfo, abstractQuery.getQueryText, updating, resultBuilderFactory, inputQuery.notificationLogger)

    new ExecutionPlan {
      private val fingerprint = PlanFingerprintReference(clock, queryPlanTTL, statsDivergenceThreshold, fp)

      def run(queryContext: QueryContext, ignored: KernelStatement, planType: ExecutionMode, params: Map[String, Any]) =
        func(queryContext, planType, params)

      def isPeriodicCommit = periodicCommitInfo.isDefined
      def plannerUsed = planner
      def isStale(lastTxId: () => Long, statistics: GraphStatistics) = fingerprint.isStale(lastTxId, statistics)

      def runtimeUsed = InterpretedRuntimeName
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

      builder.build(graph, queryId, planType, params, notificationLogger)
    }
}
