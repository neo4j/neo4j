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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DirectedUnionRelationshipTypesScanPipe.unionTypeIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyType
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id

case class DirectedUnionRelationshipTypesScanSlottedPipe(
  relOffset: Int,
  fromOffset: Int,
  types: Seq[LazyType],
  toOffset: Int,
  indexOrder: IndexOrder
)(val id: Id = Id.INVALID_ID) extends Pipe {

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val relIterator = unionTypeIterator(state, types, indexOrder, state.relTypeTokenReadSession.get)
    PrimitiveLongHelper.map(
      relIterator,
      { relId =>
        val context = state.newRowWithArgument(rowFactory)
        context.setLongAt(relOffset, relId)
        context.setLongAt(fromOffset, relIterator.startNodeId())
        context.setLongAt(toOffset, relIterator.endNodeId())
        context
      }
    )
  }
}
