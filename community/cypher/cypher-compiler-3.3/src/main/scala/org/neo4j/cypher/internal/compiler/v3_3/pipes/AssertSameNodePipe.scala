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
package org.neo4j.cypher.internal.compiler.v3_3.pipes

import org.neo4j.cypher.internal.compiler.v3_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_3.helpers.CastSupport
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.Id
import org.neo4j.cypher.internal.frontend.v3_3.MergeConstraintConflictException
import org.neo4j.graphdb.Node

case class AssertSameNodePipe(source: Pipe, inner: Pipe, node: String)
                             (val id: Id = new Id)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) {

  protected def internalCreateResults(lhsResult: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    val rhsResults = inner.createResults(state)
    if (lhsResult.isEmpty != rhsResults.isEmpty) {
      throw new MergeConstraintConflictException(
        s"Merge did not find a matching node $node and can not create a new node due to conflicts with existing unique nodes")
    }

    lhsResult.map { leftRow =>
      val lhsNode = CastSupport.castOrFail[Node](leftRow.get(node).get)
      rhsResults.foreach { rightRow =>
        val rhsNode = CastSupport.castOrFail[Node](rightRow.get(node).get)
        if (lhsNode.getId != rhsNode.getId) {
          throw new MergeConstraintConflictException(
            s"Merge did not find a matching node $node and can not create a new node due to conflicts with existing unique nodes")
        }
      }

      leftRow
    }
  }
}
