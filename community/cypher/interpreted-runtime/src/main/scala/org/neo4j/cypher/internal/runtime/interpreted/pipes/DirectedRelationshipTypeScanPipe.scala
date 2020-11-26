/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.util.attribution.Id

case class DirectedRelationshipTypeScanPipe(ident: String, fromNode: String, typ: LazyType, toNode: String)
                                           (val id: Id = Id.INVALID_ID) extends Pipe {

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val ctx = state.newRowWithArgument(rowFactory)
    val query = state.query
    val typeId = typ.getId(query)
    if (typeId == LazyType.UNKNOWN) ClosingIterator.empty
    else {
      PrimitiveLongHelper.map(query.getRelationshipsByType(typeId), relationshipId => {
        val relationship = state.query.relationshipById(relationshipId)
        rowFactory.copyWith(ctx, ident, relationship, fromNode, relationship.startNode(), toNode, relationship.endNode())
      })
    }
  }
}
