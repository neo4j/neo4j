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
package org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan

import org.neo4j.cypher.internal.compatibility.v3_5.runtime._
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.interpreted.{CSVResources, ExecutionContext}
import org.neo4j.cypher.internal.v3_5.logical.plans.LogicalPlan
import org.neo4j.cypher.result.{QueryProfile, RuntimeResult}
import org.neo4j.values.virtual.MapValue
import org.opencypher.v9_0.frontend.phases.InternalNotificationLogger
import org.opencypher.v9_0.util.{CypherException, TaskCloser}

import scala.collection.mutable

abstract class BaseExecutionResultBuilderFactory(pipe: Pipe,
                                                 readOnly: Boolean,
                                                 columns: List[String],
                                                 logicalPlan: LogicalPlan) extends ExecutionResultBuilderFactory {
  abstract class BaseExecutionWorkflowBuilder() extends ExecutionResultBuilder {
    protected val taskCloser = new TaskCloser
    protected var externalResource: ExternalCSVResource = new CSVResources(taskCloser)
    protected var maybeQueryContext: Option[QueryContext] = None
    protected var pipeDecorator: PipeDecorator = NullPipeDecorator
    protected var exceptionDecorator: CypherException => CypherException = identity

    protected def createQueryState(params: MapValue): QueryState

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

    override def build(params: MapValue,
                       notificationLogger: InternalNotificationLogger,
                       readOnly: Boolean,
                       queryProfile: QueryProfile): RuntimeResult = {
      taskCloser.addTask(queryContext.transactionalContext.close)
      taskCloser.addTask(queryContext.resources.close)
      val state = createQueryState(params)
      try {
        createResults(state, notificationLogger, readOnly, queryProfile)
      }
      catch {
        case e: CypherException =>
          taskCloser.close(success = false)
          throw exceptionDecorator(e)
        case t: Throwable =>
          taskCloser.close(success = false)
          throw t
      }
    }

    private def createResults(state: QueryState,
                              notificationLogger: InternalNotificationLogger,
                              readOnly: Boolean,
                              queryProfile: QueryProfile): RuntimeResult = {
      val results = pipe.createResults(state)
      val resultIterator = buildResultIterator(results, readOnly)
      new PipeExecutionResult(resultIterator, columns.toArray, state, queryProfile)
    }

    protected def queryContext: QueryContext = maybeQueryContext.get

    protected def buildResultIterator(results: Iterator[ExecutionContext], readOnly: Boolean): IteratorBasedResult
  }
}

case class InterpretedExecutionResultBuilderFactory(pipe: Pipe,
                                                    readOnly: Boolean,
                                                    columns: List[String],
                                                    logicalPlan: LogicalPlan,
                                                    lenientCreateRelationship: Boolean)
  extends BaseExecutionResultBuilderFactory(pipe, readOnly, columns, logicalPlan) {

  override def create(): ExecutionResultBuilder = InterpretedExecutionWorkflowBuilder()

  case class InterpretedExecutionWorkflowBuilder() extends BaseExecutionWorkflowBuilder {
    override def createQueryState(params: MapValue): QueryState = {
      new QueryState(queryContext,
                     externalResource,
                     params,
                     pipeDecorator,
                     triadicState = mutable.Map.empty,
                     repeatableReads = mutable.Map.empty,
                     lenientCreateRelationship = lenientCreateRelationship)
    }

    override def buildResultIterator(results: Iterator[ExecutionContext], readOnly: Boolean): IteratorBasedResult = {
      IteratorBasedResult(results)
    }
  }
}
