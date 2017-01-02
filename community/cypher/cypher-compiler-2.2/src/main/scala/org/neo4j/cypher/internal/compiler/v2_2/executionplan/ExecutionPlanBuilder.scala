/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.executionplan

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.ast.Statement
import org.neo4j.cypher.internal.compiler.v2_2.commands._
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.builders._
import org.neo4j.cypher.internal.compiler.v2_2.pipes._
import org.neo4j.cypher.internal.compiler.v2_2.planner.CantHandleQueryException
import org.neo4j.cypher.internal.compiler.v2_2.profiler.Profiler
import org.neo4j.cypher.internal.compiler.v2_2.spi._
import org.neo4j.cypher.internal.compiler.v2_2.symbols.SymbolTable
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.helpers.Clock

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

trait PipeBuilder {
  def producePlan(inputQuery: PreparedQuery, planContext: PlanContext): PipeInfo
}

class ExecutionPlanBuilder(graph: GraphDatabaseService, statsDivergenceThreshold: Double, queryPlanTTL: Long,
                           clock: Clock, pipeBuilder: PipeBuilder) extends PatternGraphBuilder {
  def build(planContext: PlanContext, inputQuery: PreparedQuery): ExecutionPlan = {
    val abstractQuery = inputQuery.abstractQuery

    val pipeInfo = pipeBuilder.producePlan(inputQuery, planContext)
    val PipeInfo(pipe, updating, periodicCommitInfo, fp, planner) = pipeInfo

    val columns = getQueryResultColumns(abstractQuery, pipe.symbols)
    val resultBuilderFactory = new DefaultExecutionResultBuilderFactory(pipeInfo, columns)
    val func = getExecutionPlanFunction(periodicCommitInfo, abstractQuery.getQueryText, updating, resultBuilderFactory)

    new ExecutionPlan {
      private val fingerprint = PlanFingerprintReference(clock, queryPlanTTL, statsDivergenceThreshold, fp)

      def run(queryContext: QueryContext, planType: ExecutionMode, params: Map[String, Any]) =
        func(queryContext, planType, params)

      def isPeriodicCommit = periodicCommitInfo.isDefined
      def plannerUsed = planner
      def isStale(lastCommittedTxId: () => Long, statistics: GraphStatistics) = fingerprint.isStale(lastCommittedTxId, statistics)
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
                                       resultBuilderFactory: ExecutionResultBuilderFactory):
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

      builder.build(graph, queryId, planType, params)
    }
}
