/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
                                           pipelines: SlotConfigurations,
                                           lenientCreateRelationship: Boolean)
  extends BaseExecutionResultBuilderFactory(pipeInfo, columns, logicalPlan) {

  override def create(): ExecutionResultBuilder =
    new SlottedExecutionWorkflowBuilder()

  class SlottedExecutionWorkflowBuilder() extends BaseExecutionWorkflowBuilder {
    override protected def createQueryState(params: MapValue) = {
      new SlottedQueryState(queryContext, externalResource, params, pipeDecorator,
        triadicState = mutable.Map.empty, repeatableReads = mutable.Map.empty,
        lenientCreateRelationship = lenientCreateRelationship)
    }

    override def buildResultIterator(results: Iterator[ExecutionContext], isUpdating: Boolean): ResultIterator = {
      val closingIterator = new ClosingQueryResultRecordIterator(results, taskCloser, exceptionDecorator)
      val resultIterator = if (isUpdating) closingIterator.toEager else closingIterator
      resultIterator
    }
  }
}
