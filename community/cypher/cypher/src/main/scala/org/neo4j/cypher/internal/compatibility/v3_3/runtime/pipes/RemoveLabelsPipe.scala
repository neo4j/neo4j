/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.CastSupport
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.mutation.GraphElementPropertyFunctions
import org.neo4j.cypher.internal.v3_3.logical.plans.LogicalPlanId
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue

case class RemoveLabelsPipe(src: Pipe, variable: String, labels: Seq[LazyLabel])
                           (val id: LogicalPlanId = LogicalPlanId.DEFAULT)
  extends PipeWithSource(src) with GraphElementPropertyFunctions {

  override protected def internalCreateResults(input: Iterator[ExecutionContext],
                                               state: QueryState): Iterator[ExecutionContext] = {
    input.map { row =>
      val item = row.get(variable).get
      if (item != Values.NO_VALUE) removeLabels(row, state, CastSupport.castOrFail[NodeValue](item).id)
      row
    }
  }

  private def removeLabels(context: ExecutionContext, state: QueryState, nodeId: Long) = {
    val labelIds = labels.flatMap(_.getOptId(state.query)).map(_.id)
    state.query.removeLabelsFromNode(nodeId, labelIds.iterator)
  }
}
