/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.predicates.Predicate
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.PrimitiveLongHelper
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.PrimitiveExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.helpers.NullChecker.nodeIsNull
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionContext, PipelineInformation}
import org.neo4j.cypher.internal.v3_3.logical.plans.LogicalPlanId
import org.neo4j.cypher.internal.frontend.v3_3.{InternalException, SemanticDirection}
import org.neo4j.kernel.impl.api.RelationshipVisitor
import org.neo4j.kernel.impl.api.store.RelationshipIterator

case class OptionalExpandAllSlottedPipe(source: Pipe,
                                        fromOffset: Int,
                                        relOffset: Int,
                                        toOffset: Int,
                                        dir: SemanticDirection,
                                        types: LazyTypes,
                                        predicate: Predicate,
                                        pipelineInformation: PipelineInformation)
                                       (val id: LogicalPlanId = LogicalPlanId.DEFAULT) extends PipeWithSource(source) with Pipe {

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.flatMap {
      (inputRow: ExecutionContext) =>
        val fromNode = inputRow.getLongAt(fromOffset)

        if (nodeIsNull(fromNode)) {
          Iterator(withNulls(inputRow))
        } else {
          val relationships: RelationshipIterator = state.query.getRelationshipsForIdsPrimitive(fromNode, dir, types.types(state.query))
          var otherSide: Long = 0

          val relVisitor = new RelationshipVisitor[InternalException] {
            override def visit(relationshipId: Long, typeId: Int, startNodeId: Long, endNodeId: Long): Unit =
              if (fromNode == startNodeId)
                otherSide = endNodeId
              else
                otherSide = startNodeId
          }

          val matchIterator = PrimitiveLongHelper.map(relationships, relId => {
            relationships.relationshipVisit(relId, relVisitor)
            val outputRow = PrimitiveExecutionContext(pipelineInformation)
            outputRow.copyFrom(inputRow, pipelineInformation.initialNumberOfLongs, pipelineInformation.initialNumberOfReferences)
            outputRow.setLongAt(relOffset, relId)
            outputRow.setLongAt(toOffset, otherSide)
            outputRow
          }).filter(ctx => predicate.isTrue(ctx, state))

          if (matchIterator.isEmpty)
            Iterator(withNulls(inputRow))
          else
            matchIterator
        }
    }
  }

  private def withNulls(inputRow: ExecutionContext) = {
    val outputRow = PrimitiveExecutionContext(pipelineInformation)
    outputRow.copyFrom(inputRow, pipelineInformation.initialNumberOfLongs, pipelineInformation.initialNumberOfReferences)
    outputRow.setLongAt(relOffset, -1)
    outputRow.setLongAt(toOffset, -1)
    outputRow
  }

}
