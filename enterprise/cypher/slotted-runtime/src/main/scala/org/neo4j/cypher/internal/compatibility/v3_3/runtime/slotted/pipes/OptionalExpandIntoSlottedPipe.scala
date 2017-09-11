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

import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.predicates.Predicate
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.PrimitiveLongHelper
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.PrimitiveExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.helpers.NullChecker.nodeIsNull
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionContext, PipelineInformation}
import org.neo4j.cypher.internal.v3_3.logical.plans.LogicalPlanId
import org.neo4j.cypher.internal.frontend.v3_3.SemanticDirection

case class OptionalExpandIntoSlottedPipe(source: Pipe,
                                         fromOffset: Int,
                                         relOffset: Int,
                                         toOffset: Int,
                                         dir: SemanticDirection,
                                         lazyTypes: LazyTypes,
                                         predicate: Predicate,
                                         pipelineInformation: PipelineInformation)
                                        (val id: LogicalPlanId = LogicalPlanId.DEFAULT)
  extends PipeWithSource(source) with PrimitiveCachingExpandInto {
  self =>
  private final val CACHE_SIZE = 100000

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    //cache of known connected nodes
    val relCache = new PrimitiveRelationshipsCache(CACHE_SIZE)

    input.flatMap {
      (inputRow: ExecutionContext) =>
        val fromNode = inputRow.getLongAt(fromOffset)
        val toNode = inputRow.getLongAt(toOffset)

        if (nodeIsNull(fromNode) || nodeIsNull(toNode)) {
          Iterator(withNulls(inputRow))
        } else {
          val relationships: PrimitiveLongIterator = relCache.get(fromNode, toNode, dir)
            .getOrElse(findRelationships(state.query, fromNode, toNode, relCache, dir, lazyTypes.types(state.query)))

          val matchIterator = PrimitiveLongHelper.map(relationships, relId => {
            val outputRow = PrimitiveExecutionContext(pipelineInformation)
            outputRow.copyFrom(inputRow, pipelineInformation.initialNumberOfLongs, pipelineInformation.initialNumberOfReferences)
            outputRow.setLongAt(relOffset, relId)
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
    outputRow
  }

}
