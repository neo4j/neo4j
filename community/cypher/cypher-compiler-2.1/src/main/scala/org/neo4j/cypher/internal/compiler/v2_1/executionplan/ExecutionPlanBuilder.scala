/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import symbols.SymbolTable
import org.neo4j.cypher.{PeriodicCommitInOpenTransactionException, ExecutionResult}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.cypher.internal.compiler.v2_1.planner.{CantHandleQueryException, Planner}
import org.neo4j.cypher.internal.compiler.v2_1.ast.Statement
import org.neo4j.cypher.internal.compiler.v2_1.spi.{CSVResources, QueryContext, PlanContext}
import org.neo4j.cypher.internal.compiler.v2_1.spi.{UpdateCountingQueryContext, CSVResources, QueryContext, PlanContext}

case class PipeInfo(pipe: Pipe, updating: Boolean, periodicCommit: Option[PeriodicCommitInfo] = None)

case class PeriodicCommitInfo(size: Option[Long])

class ExecutionPlanBuilder(graph: GraphDatabaseService, pipeBuilder: PipeBuilder = new PipeBuilder, execPlanBuilder: Planner = new Planner()) extends PatternGraphBuilder {

  def build(planContext: PlanContext, inputQuery: AbstractQuery, ast: Statement): ExecutionPlan = {

    val PipeInfo(p, isUpdating, periodicCommitInfo) = try {
      execPlanBuilder.producePlan(ast)
    } catch {
      case _: CantHandleQueryException => pipeBuilder.buildPipes(planContext, inputQuery)
    }

    val columns = getQueryResultColumns(inputQuery, p.symbols)
    val func = getExecutionPlanFunction(p, columns, periodicCommitInfo, isUpdating)

    new ExecutionPlan {
      def execute(queryContext: QueryContext, params: Map[String, Any]) = func(queryContext, params, false)

      def profile(queryContext: QueryContext, params: Map[String, Any]) = func(new UpdateCountingQueryContext(queryContext), params, true)
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
                                       updating: Boolean):
  (QueryContext, Map[String, Any], Boolean) => ExecutionResult = {
    val func = (queryContext: QueryContext, params: Map[String, Any], profile: Boolean) => {

      val builder = new ExecutionResultBuilder
      builder.addQueryContextBuilder(_ => queryContext)
      builder.addCloserBuilder(_ => new TaskCloser)
      builder.addExternalResourceBuilder(_ => new CSVResources(builder.closer))

      periodicCommit match {

        case Some(info) if !queryContext.isTopLevelTx => throw new PeriodicCommitInOpenTransactionException()

        case Some(info) if !pipe.exists(_.isInstanceOf[LoadCSVPipe]) =>
          val defaultSize = 10000L
          val size = info.size.getOrElse(defaultSize)
          builder.addObserverBuilder(_ => new PeriodicCommitObserver(size, queryContext))
          builder.addQueryContextBuilder(inner => new UpdateObservableQueryContext(builder.observer, inner))

        case Some(info) =>
          val defaultSize = 10000L
          val size = info.size.getOrElse(defaultSize)
          builder.addObserverBuilder(_ => new LoadCsvPeriodicCommitObserver(size, builder.externalResource, queryContext))
          builder.addQueryContextBuilder(inner => new UpdateObservableQueryContext(builder.observer, inner))

        case _ =>
      }

      builder.addQueryContextBuilder(inner => new UpdateCountingQueryContext(inner))
      val newQueryContext = builder.queryContext

      val decorator = if (profile) new Profiler() else NullDecorator
      val taskCloser =  builder.closer
      taskCloser.addTask(newQueryContext.close)
      val state = new QueryState(graph, newQueryContext, builder.externalResource, params, decorator)

      try {
        val results: Iterator[collection.Map[String, Any]] = pipe.createResults(state)

        val closingIterator = new ClosingIterator(results, taskCloser)
        val descriptor = {
          () =>
            val result = decorator.decorate(pipe.executionPlanDescription, closingIterator.isEmpty)
            result
        }
        if (updating)
          new EagerPipeExecutionResult(closingIterator, columns, state, descriptor)
        else
          new PipeExecutionResult(closingIterator, columns, state, descriptor)

      }
      catch {
        case (t: Throwable) =>
          taskCloser.close(success = false)
          throw t
      }
    }

    func
  }

  class ExecutionResultBuilder {
    var queryContextBuilders: QueryContext => QueryContext = (_) => null

    def addQueryContextBuilder(f: QueryContext => QueryContext) =
      queryContextBuilders = queryContextBuilders andThen f

    var externalResourceBuilder: ExternalResource => ExternalResource = (_) => null

    def addExternalResourceBuilder(f: ExternalResource => ExternalResource) =
      externalResourceBuilder = externalResourceBuilder andThen f

    var observerBuild: UpdateObserver => UpdateObserver = (_) => null

    def addObserverBuilder(f: UpdateObserver => UpdateObserver) =
      observerBuild = observerBuild andThen f

    var closerBuilder: TaskCloser => TaskCloser = (_) => null

    def addCloserBuilder(f: TaskCloser => TaskCloser) =
      closerBuilder = closerBuilder andThen f

    lazy val queryContext: QueryContext = queryContextBuilders(null)
    lazy val observer: UpdateObserver = observerBuild(null)
    lazy val externalResource: ExternalResource = externalResourceBuilder(null)
    lazy val closer: TaskCloser = closerBuilder(null)
  }
}
