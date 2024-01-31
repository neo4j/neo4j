/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.CastSupport
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.MergeConstraintConflictException.nodeConflict
import org.neo4j.values.virtual.VirtualNodeValue

case class AssertSameNodePipe(source: Pipe, inner: Pipe, node: String)(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(source) {

  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val rhsResults = inner.createResults(state)
    if (input.isEmpty != rhsResults.isEmpty) {
      nodeConflict(node)
    }

    input.map { leftRow =>
      val lhsNode = CastSupport.castOrFail[VirtualNodeValue](leftRow.getByName(node))
      rhsResults.foreach { rightRow =>
        val rhsNode = CastSupport.castOrFail[VirtualNodeValue](rightRow.getByName(node))
        if (lhsNode.id != rhsNode.id) {
          nodeConflict(node)
        }
      }

      leftRow
    }
  }
}
