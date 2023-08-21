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

import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualValues

case class ExpandAllPipe(
  source: Pipe,
  fromName: String,
  relName: String,
  toName: String,
  dir: SemanticDirection,
  types: RelationshipTypes
)(val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    input.flatMap {
      row =>
        row.getByName(fromName) match {
          case n: VirtualNodeValue =>
            val relationships = state.query.getRelationshipsForIds(n.id(), dir, types.types(state.query))
            PrimitiveLongHelper.map(
              relationships,
              relId => {
                val other = relationships.otherNodeId(n.id())
                rowFactory.copyWith(
                  row,
                  relName,
                  VirtualValues.relationship(
                    relId,
                    relationships.startNodeId(),
                    relationships.endNodeId(),
                    relationships.typeId()
                  ),
                  toName,
                  VirtualValues.node(other)
                )

              }
            )
          case IsNoValue() => ClosingIterator.empty

          case value =>
            throw new ParameterWrongTypeException(s"Expected to find a node at '$fromName' but found $value instead")
        }
    }
  }
}
