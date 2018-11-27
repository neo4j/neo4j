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
package org.neo4j.cypher.internal.compatibility.v4_0.runtime.executionplan

import org.neo4j.cypher.internal.compatibility.v4_0.runtime._
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.interpreted.CSVResources
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.v4_0.logical.plans.LoadCSV
import org.neo4j.cypher.internal.v4_0.logical.plans.LogicalPlan
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.values.virtual.MapValue

abstract class BaseExecutionResultBuilderFactory(pipe: Pipe,
                                                 readOnly: Boolean,
                                                 columns: List[String],
                                                 logicalPlan: LogicalPlan) extends ExecutionResultBuilderFactory {

  abstract class BaseExecutionResultBuilder() extends ExecutionResultBuilder {
    protected var externalResource: ExternalCSVResource = new CSVResources(queryContext.resources)
    protected var pipeDecorator: PipeDecorator = if (logicalPlan.treeFind[LogicalPlan] {
      case _: LoadCSV => true
    }.isEmpty) NullPipeDecorator else new LinenumberPipeDecorator()

    protected def createQueryState(params: MapValue, prePopulateResults: Boolean): QueryState

    def queryContext: QueryContext

    def setLoadCsvPeriodicCommitObserver(batchRowCount: Long): Unit = {
      externalResource = new LoadCsvPeriodicCommitObserver(batchRowCount, externalResource, queryContext)
    }

    def addProfileDecorator(profileDecorator: PipeDecorator): Unit = pipeDecorator match {
      case decorator: LinenumberPipeDecorator => decorator.setInnerDecorator(profileDecorator)
      case _ => pipeDecorator = profileDecorator
    }

    override def build(params: MapValue,
                       readOnly: Boolean,
                       queryProfile: QueryProfile,
                       prePopulateResults: Boolean): RuntimeResult = {
      val state = createQueryState(params, prePopulateResults)
      val results = pipe.createResults(state)
      val resultIterator = buildResultIterator(results, readOnly)
      new PipeExecutionResult(resultIterator, columns.toArray, state, queryProfile)
    }

    protected def buildResultIterator(results: Iterator[ExecutionContext], readOnly: Boolean): IteratorBasedResult
  }

}

case class InterpretedExecutionResultBuilderFactory(pipe: Pipe,
                                                    queryIndexes: QueryIndexes,
                                                    readOnly: Boolean,
                                                    columns: List[String],
                                                    logicalPlan: LogicalPlan,
                                                    lenientCreateRelationship: Boolean)
  extends BaseExecutionResultBuilderFactory(pipe, readOnly, columns, logicalPlan) {

  override def create(queryContext: QueryContext): ExecutionResultBuilder = InterpretedExecutionResultBuilder(queryContext: QueryContext)

  case class InterpretedExecutionResultBuilder(queryContext: QueryContext) extends BaseExecutionResultBuilder {
    override def createQueryState(params: MapValue, prePopulateResults: Boolean): QueryState = {
      val cursors = new ExpressionCursors(queryContext.transactionalContext.cursors)

      new QueryState(queryContext,
                     externalResource,
                     params,
                     cursors,
                     queryIndexes.indexes.map(index => queryContext.transactionalContext.dataRead.indexReadSession(index)),
                     pipeDecorator,
                     lenientCreateRelationship = lenientCreateRelationship,
                     prePopulateResults = prePopulateResults)
    }

    override def buildResultIterator(results: Iterator[ExecutionContext], readOnly: Boolean): IteratorBasedResult = {
      IteratorBasedResult(results)
    }
  }

}
