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
import org.neo4j.cypher.internal.runtime.ClosingLongIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.RelationshipIterator
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualValues

abstract class OptionalExpandAllPipe(
  source: Pipe,
  fromName: String,
  relName: String,
  toName: String,
  dir: SemanticDirection,
  types: RelationshipTypes
) extends PipeWithSource(source) {

  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    input.flatMap {
      row =>
        val fromNode = getFromNode(row)
        fromNode match {
          case n: VirtualNodeValue =>
            val relationships = state.query.getRelationshipsForIds(n.id(), dir, types.types(state.query))
            val matchIterator = findMatchIterator(row, state, relationships, n)
            if (matchIterator.isEmpty) {
              ClosingIterator.single(withNulls(row))
            } else {
              matchIterator
            }

          case value if value eq Values.NO_VALUE =>
            ClosingIterator.single(withNulls(row))

          case value =>
            throw new ParameterWrongTypeException(s"Expected to find a node at '$fromName' but found $value instead")
        }
    }
  }

  def findMatchIterator(
    row: CypherRow,
    state: QueryState,
    relationships: ClosingLongIterator with RelationshipIterator,
    n: VirtualNodeValue
  ): ClosingIterator[CypherRow]

  private def withNulls(row: CypherRow) = {
    row.set(relName, Values.NO_VALUE, toName, Values.NO_VALUE)
    row
  }

  def getFromNode(row: CypherRow): AnyValue = row.getByName(fromName)
}

object OptionalExpandAllPipe {

  def apply(
    source: Pipe,
    fromName: String,
    relName: String,
    toName: String,
    dir: SemanticDirection,
    types: RelationshipTypes,
    maybePredicate: Option[Expression]
  )(id: Id = Id.INVALID_ID): OptionalExpandAllPipe = maybePredicate match {
    case Some(predicate) => FilteringOptionalExpandAllPipe(source, fromName, relName, toName, dir, types, predicate)(id)
    case None            => NonFilteringOptionalExpandAllPipe(source, fromName, relName, toName, dir, types)(id)
  }
}

case class NonFilteringOptionalExpandAllPipe(
  source: Pipe,
  fromName: String,
  relName: String,
  toName: String,
  dir: SemanticDirection,
  types: RelationshipTypes
)(val id: Id = Id.INVALID_ID)
    extends OptionalExpandAllPipe(source, fromName, relName, toName, dir, types) {

  override def findMatchIterator(
    row: CypherRow,
    ignore: QueryState,
    relationships: ClosingLongIterator with RelationshipIterator,
    n: VirtualNodeValue
  ): ClosingIterator[CypherRow] = {
    PrimitiveLongHelper.map(
      relationships,
      r => {
        val other = relationships.otherNodeId(n.id())
        rowFactory.copyWith(
          row,
          relName,
          VirtualValues.relationship(r, relationships.startNodeId(), relationships.endNodeId(), relationships.typeId()),
          toName,
          VirtualValues.node(other)
        )
      }
    )
  }
}

case class FilteringOptionalExpandAllPipe(
  source: Pipe,
  fromName: String,
  relName: String,
  toName: String,
  dir: SemanticDirection,
  types: RelationshipTypes,
  predicate: Expression
)(val id: Id = Id.INVALID_ID)
    extends OptionalExpandAllPipe(source, fromName, relName, toName, dir, types) {

  override def findMatchIterator(
    row: CypherRow,
    state: QueryState,
    relationships: ClosingLongIterator with RelationshipIterator,
    n: VirtualNodeValue
  ): ClosingIterator[CypherRow] = {

    PrimitiveLongHelper.map(
      relationships,
      r => {
        val other = relationships.otherNodeId(n.id())
        rowFactory.copyWith(
          row,
          relName,
          VirtualValues.relationship(r, relationships.startNodeId(), relationships.endNodeId(), relationships.typeId()),
          toName,
          VirtualValues.node(other)
        )
      }
    ).filter(ctx => predicate(ctx, state) eq Values.TRUE)
  }
}
