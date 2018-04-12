/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{ClosingQueryResultRecordIterator, ResultIterator}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.{BaseExecutionResultBuilderFactory, ExecutionResultBuilder, PipeInfo}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan
import org.neo4j.values.virtual.MapValue

import scala.collection.mutable

class SlottedExecutionResultBuilderFactory(pipeInfo: PipeInfo,
                                           columns: List[String],
                                           logicalPlan: LogicalPlan,
                                           pipelines: SlotConfigurations)
  extends BaseExecutionResultBuilderFactory(pipeInfo, columns, logicalPlan) {

  override def create(): ExecutionResultBuilder =
    new SlottedExecutionWorkflowBuilder()

  class SlottedExecutionWorkflowBuilder() extends BaseExecutionWorkflowBuilder {
    override protected def createQueryState(params: MapValue) = {
      new SlottedQueryState(queryContext, externalResource, params, pipeDecorator,
        triadicState = mutable.Map.empty, repeatableReads = mutable.Map.empty)
    }

    override def buildResultIterator(results: Iterator[ExecutionContext], isUpdating: Boolean): ResultIterator = {
      val closingIterator = new ClosingQueryResultRecordIterator(results, taskCloser, exceptionDecorator)
      val resultIterator = if (isUpdating) closingIterator.toEager else closingIterator
      resultIterator
    }
  }
}
