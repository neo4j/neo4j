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
package org.neo4j.cypher.internal.compiler.v2_2.executionplan

import org.neo4j.cypher.internal.{Explained, PlanType}
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.PlanDescription
import org.neo4j.cypher.{ExecutionResult, CypherException}
import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.pipes._
import org.neo4j.cypher.internal.compiler.v2_2.spi.{QueryContext, CSVResources}
import org.neo4j.graphdb.GraphDatabaseService

case class DefaultExecutionResultBuilderFactory(pipeInfo: PipeInfo, columns: List[String], planType: PlanType) extends ExecutionResultBuilderFactory {
  def create(): ExecutionResultBuilder =
    ExecutionWorkflowBuilder()

  case class ExecutionWorkflowBuilder() extends ExecutionResultBuilder {
    private val taskCloser = new TaskCloser
    private var externalResource: ExternalResource = new CSVResources(taskCloser)
    private var maybeQueryContext: Option[QueryContext] = None
    private var pipeDecorator: PipeDecorator = NullPipeDecorator
    private var exceptionDecorator: CypherException => CypherException = identity

    def setQueryContext(context: QueryContext) {
      maybeQueryContext = Some(context)
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

    def build(graph: GraphDatabaseService, queryId: AnyRef, params: Map[String, Any]): InternalExecutionResult = {
      taskCloser.addTask(queryContext.close)
      val state = new QueryState(graph, queryContext, externalResource, params, pipeDecorator, queryId = queryId)
      try {
        try {
          createResults(state)
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

    private def createResults(state: QueryState): InternalExecutionResult =
      if (planType == Explained) {
        new ExplainExecutionResult(columns, pipeInfo.pipe.planDescription)
      } else {
        val results = pipeInfo.pipe.createResults(state)
        val closingIterator = buildClosingIterator(results)
        val descriptor = buildDescriptor(pipeInfo.pipe, closingIterator.isEmpty)

        if (pipeInfo.updating)
          new EagerPipeExecutionResult(closingIterator, columns, state, descriptor, planType)
        else
          new PipeExecutionResult(closingIterator, columns, state, descriptor, planType)
      }

    private def queryContext = maybeQueryContext.get

    private def buildClosingIterator(results: Iterator[ExecutionContext]): ClosingIterator =
      new ClosingIterator(results, taskCloser, exceptionDecorator)

    private def buildDescriptor(pipe: Pipe, isProfileReady: => Boolean): () => PlanDescription =
      () => pipeDecorator.decorate(pipe.planDescription, isProfileReady)
  }
}
