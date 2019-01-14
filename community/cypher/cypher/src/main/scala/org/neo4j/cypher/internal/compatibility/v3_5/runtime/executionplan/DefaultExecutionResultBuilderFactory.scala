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
package org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan

import org.neo4j.cypher.internal.compatibility.v3_5.runtime._
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.interpreted.{CSVResources, ExecutionContext}
import org.neo4j.cypher.internal.v3_5.logical.plans.LogicalPlan
import org.neo4j.cypher.result.{QueryProfile, RuntimeResult}
import org.neo4j.values.virtual.MapValue
import org.neo4j.cypher.internal.v3_5.frontend.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.v3_5.util.CypherException

import scala.collection.mutable

abstract class BaseExecutionResultBuilderFactory(pipe: Pipe,
                                                 readOnly: Boolean,
                                                 columns: List[String],
                                                 logicalPlan: LogicalPlan) extends ExecutionResultBuilderFactory {
  abstract class BaseExecutionWorkflowBuilder() extends ExecutionResultBuilder {
    protected var externalResource: ExternalCSVResource = new CSVResources(queryContext.resources)
    protected var pipeDecorator: PipeDecorator = NullPipeDecorator
    protected var exceptionDecorator: CypherException => CypherException = identity

    protected def createQueryState(params: MapValue): QueryState

    def queryContext: QueryContext

    def setLoadCsvPeriodicCommitObserver(batchRowCount: Long): Unit = {
      val observer = new LoadCsvPeriodicCommitObserver(batchRowCount, externalResource, queryContext)
      externalResource = observer
      exceptionDecorator = observer
    }

    def setPipeDecorator(newDecorator: PipeDecorator): Unit =
      pipeDecorator = newDecorator

    override def build(params: MapValue,
                       readOnly: Boolean,
                       queryProfile: QueryProfile): RuntimeResult = {
      val state = createQueryState(params)
      try {
        val results = pipe.createResults(state)
        val resultIterator = buildResultIterator(results, readOnly)
        new PipeExecutionResult(resultIterator, columns.toArray, state, queryProfile)
      } catch {
        case e: CypherException =>
          throw exceptionDecorator(e)
      }
    }

    protected def buildResultIterator(results: Iterator[ExecutionContext], readOnly: Boolean): IteratorBasedResult
  }
}

case class InterpretedExecutionResultBuilderFactory(pipe: Pipe,
                                                    readOnly: Boolean,
                                                    columns: List[String],
                                                    logicalPlan: LogicalPlan,
                                                    lenientCreateRelationship: Boolean)
  extends BaseExecutionResultBuilderFactory(pipe, readOnly, columns, logicalPlan) {

  override def create(queryContext: QueryContext): ExecutionResultBuilder = InterpretedExecutionWorkflowBuilder(queryContext: QueryContext)

  case class InterpretedExecutionWorkflowBuilder(queryContext: QueryContext) extends BaseExecutionWorkflowBuilder {
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
