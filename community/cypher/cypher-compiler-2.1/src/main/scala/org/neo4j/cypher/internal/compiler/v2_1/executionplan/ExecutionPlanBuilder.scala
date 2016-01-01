/**
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
package org.neo4j.cypher.internal.compiler.v2_1.executionplan

import builders._
import org.neo4j.cypher.internal.compiler.v2_1._
import commands._
import pipes._
import profiler.Profiler
import org.neo4j.cypher.{CypherException, PeriodicCommitInOpenTransactionException}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.cypher.internal.compiler.v2_1.ast.Statement
import org.neo4j.cypher.internal.compiler.v2_1.spi.{UpdateCountingQueryContext, CSVResources, PlanContext}
import org.neo4j.cypher.internal.compiler.v2_1.commands.PeriodicCommitQuery
import org.neo4j.cypher.internal.compiler.v2_1.commands.Union
import org.neo4j.cypher.internal.compiler.v2_1.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v2_1.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_1.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v2_1.helpers.{EagerMappingBuilder, MappingBuilder}
import org.neo4j.cypher.internal.compiler.v2_1.planner.CantHandleQueryException

case class PipeInfo(pipe: Pipe,
                    updating: Boolean,
                    periodicCommit: Option[PeriodicCommitInfo] = None)

case class PeriodicCommitInfo(size: Option[Long]) {
  def batchRowCount = size.getOrElse(/* defaultSize */ 1000L)
}

trait NewQueryPlanSuccessRateMonitor {
  def newQuerySeen(queryText: String, ast:Statement)
  def unableToHandleQuery(queryText: String, ast:Statement, origin: CantHandleQueryException)
}

trait PipeBuilder {
  def producePlan(inputQuery: PreparedQuery, planContext: PlanContext): PipeInfo
}

class ExecutionPlanBuilder(graph: GraphDatabaseService,
                           pipeBuilder: PipeBuilder) extends PatternGraphBuilder {

  def build(planContext: PlanContext, inputQuery: PreparedQuery): ExecutionPlan = {
    val abstractQuery = inputQuery.abstractQuery

    val PipeInfo(pipe, isUpdating, periodicCommitInfo) = pipeBuilder.producePlan(inputQuery, planContext)

    val columns = getQueryResultColumns(abstractQuery, pipe.symbols)
    val func = getExecutionPlanFunction(pipe, columns, periodicCommitInfo, isUpdating, abstractQuery.getQueryText)

    new ExecutionPlan {
      def execute(queryContext: QueryContext, params: Map[String, Any]) = func(queryContext, params, false)

      def profile(queryContext: QueryContext, params: Map[String, Any]) = func(new UpdateCountingQueryContext(queryContext), params, true)
      def isPeriodicCommit = periodicCommitInfo.isDefined
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

  private def getExecutionPlanFunction(pipe: Pipe,
                                       columns: List[String],
                                       periodicCommit: Option[PeriodicCommitInfo],
                                       updating: Boolean,
                                       queryId: AnyRef) =
    (queryContext: QueryContext, params: Map[String, Any], profile: Boolean) => {

      val builder = new ExecutionWorkflowBuilder(queryContext)

      if (periodicCommit.isDefined) {
        if (!queryContext.isTopLevelTx)
          throw new PeriodicCommitInOpenTransactionException()
        builder.setLoadCsvPeriodicCommitObserver(periodicCommit.get.batchRowCount)
      }

      builder.transformQueryContext(new UpdateCountingQueryContext(_))

      if (profile)
        builder.setPipeDecorator(new Profiler())

      builder.runWithQueryState(graph, queryId, params) {
        state =>
          val results = pipe.createResults(state)
          val closingIterator = builder.buildClosingIterator(results)
          val descriptor = builder.buildDescriptor(pipe, closingIterator.isEmpty)

          if (updating)
            new EagerPipeExecutionResult(closingIterator, columns, state, descriptor)
          else
            new PipeExecutionResult(closingIterator, columns, state, descriptor)
      }
    }
}

class ExecutionWorkflowBuilder(initialQueryContext: QueryContext) {
  private val taskCloser = new TaskCloser
  private var externalResource: ExternalResource = new CSVResources(taskCloser)
  private val queryContextBuilder: MappingBuilder[QueryContext] = new EagerMappingBuilder(initialQueryContext)
  private var pipeDecorator: PipeDecorator = NullPipeDecorator
  private var exceptionDecorator: CypherException => CypherException = identity

  def transformQueryContext(f: QueryContext => QueryContext) {
    queryContextBuilder += f
  }

  def setLoadCsvPeriodicCommitObserver(batchRowCount: Long) {
    val observer = new LoadCsvPeriodicCommitObserver(batchRowCount, externalResource, queryContext)
    externalResource = observer
    setExceptionDecorator(observer)
  }

  def setPipeDecorator(newDecorator: PipeDecorator) {
    pipeDecorator = newDecorator
  }

  def setExceptionDecorator(newDecorator: CypherException => CypherException) {
    exceptionDecorator = newDecorator
  }

  def buildClosingIterator(results: Iterator[ExecutionContext]) =
    new ClosingIterator(results, taskCloser, exceptionDecorator)

  def buildDescriptor(pipe: Pipe, isProfileReady: => Boolean) =
    () => pipeDecorator.decorate(pipe.planDescription, isProfileReady)

  def runWithQueryState[T](graph: GraphDatabaseService, queryId: AnyRef, params: Map[String, Any])(f: QueryState => T) = {
    taskCloser.addTask(queryContext.close)
    val state = new QueryState(graph, queryContext, externalResource, params, pipeDecorator, queryId = queryId)
    try {
      try {
        f(state)
      }
      catch {
        case e: CypherException =>
          throw exceptionDecorator(e)
      }
    }
    catch {
      case (t: Throwable) =>
        taskCloser.close(success = false)
        throw t
    }
  }

  private def queryContext = queryContextBuilder.result()
}
