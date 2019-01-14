/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan

import org.neo4j.cypher.internal.compatibility.v3_4.runtime._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.helpers.InternalWrapping._
import org.neo4j.cypher.internal.frontend.v3_4.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, ReadOnlies}
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.interpreted.{CSVResources, ExecutionContext}
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments.{Runtime, RuntimeImpl}
import org.neo4j.cypher.internal.runtime.planDescription.{InternalPlanDescription, LogicalPlan2PlanDescription}
import org.neo4j.cypher.internal.runtime.{InternalExecutionResult, _}
import org.neo4j.cypher.internal.util.v3_4.{CypherException, ProfilerStatisticsNotReadyException, TaskCloser}
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan
import org.neo4j.values.virtual.MapValue

import scala.collection.mutable

abstract class BaseExecutionResultBuilderFactory(pipeInfo: PipeInfo,
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

    override def build(planType: ExecutionMode,
                       params: MapValue,
                       notificationLogger: InternalNotificationLogger,
                       runtimeName: RuntimeName,
                       readOnlies: ReadOnlies,
                       cardinalities: Cardinalities): InternalExecutionResult = {
      taskCloser.addTask(queryContext.transactionalContext.close)
      taskCloser.addTask(queryContext.resources.close)
      val state = createQueryState(params)
      try {
        createResults(state, planType, notificationLogger, runtimeName, readOnlies, cardinalities)
      }
      catch {
        case e: CypherException =>
          taskCloser.close(success = false)
          throw exceptionDecorator(e)
        case (t: Throwable) =>
          taskCloser.close(success = false)
          throw t
      }
    }

    private def createResults(state: QueryState, planType: ExecutionMode,
                              notificationLogger: InternalNotificationLogger,
                              runtimeName: RuntimeName,
                              readOnlies: ReadOnlies,
                              cardinalities: Cardinalities): InternalExecutionResult = {
      val queryType: InternalQueryType = getQueryType
      val planDescription =
        () => LogicalPlan2PlanDescription(logicalPlan, pipeInfo.plannerUsed, readOnlies, cardinalities)
          .addArgument(Runtime(runtimeName.toTextOutput))
          .addArgument(RuntimeImpl(runtimeName.name))
      if (planType == ExplainMode) {
        //close all statements
        taskCloser.close(success = true)
        ExplainExecutionResult(columns.toArray, planDescription(), queryType,
                               notificationLogger.notifications.map(asKernelNotification(notificationLogger.offset)))
      } else {
        val results = pipeInfo.pipe.createResults(state)
        val resultIterator = buildResultIterator(results, pipeInfo.updating)
        val verifyProfileReady = () => {
          val isResultReady = resultIterator.wasMaterialized
          if (!isResultReady) {
            taskCloser.close(success = false)
            throw new ProfilerStatisticsNotReadyException()
          }
        }
        val descriptor = buildDescriptor(planDescription, verifyProfileReady)
        new PipeExecutionResult(resultIterator, columns.toArray, state, descriptor, planType, queryType)
      }
    }

    protected def queryContext = maybeQueryContext.get

    protected def buildResultIterator(results: Iterator[ExecutionContext], isUpdating: Boolean): ResultIterator

    private def buildDescriptor(planDescription: () => InternalPlanDescription, verifyProfileReady: () => Unit): () => InternalPlanDescription =
      pipeDecorator.decorate(planDescription, verifyProfileReady)
  }

  private def getQueryType = {
    val queryType =
      if (pipeInfo.pipe.isInstanceOf[IndexOperationPipe] || pipeInfo.pipe.isInstanceOf[ConstraintOperationPipe])
        SCHEMA_WRITE
      else if (pipeInfo.updating) {
        if (columns.isEmpty)
          WRITE
        else
          READ_WRITE
      } else
        READ_ONLY
    queryType
  }
}

case class InterpretedExecutionResultBuilderFactory(pipeInfo: PipeInfo,
                                                    columns: List[String],
                                                    logicalPlan: LogicalPlan,
                                                    lenientCreateRelationship: Boolean)
  extends BaseExecutionResultBuilderFactory(pipeInfo, columns, logicalPlan) {

  override def create(): ExecutionResultBuilder =
    new InterpretedExecutionWorkflowBuilder()

  case class InterpretedExecutionWorkflowBuilder() extends BaseExecutionWorkflowBuilder {
    override def createQueryState(params: MapValue) = {
      new QueryState(queryContext,
                     externalResource,
                     params,
                     pipeDecorator,
                     triadicState = mutable.Map.empty,
                     repeatableReads = mutable.Map.empty,
                     lenientCreateRelationship = lenientCreateRelationship)
    }

    override def buildResultIterator(results: Iterator[ExecutionContext], isUpdating: Boolean): ResultIterator = {
      val closingIterator = new ClosingIterator(results, taskCloser, exceptionDecorator)
      val resultIterator = if (isUpdating) closingIterator.toEager else closingIterator
      resultIterator
    }
  }
}
