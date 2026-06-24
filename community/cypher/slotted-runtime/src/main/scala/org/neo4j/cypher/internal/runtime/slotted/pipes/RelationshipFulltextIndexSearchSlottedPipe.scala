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

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipFulltextIndexSearchPipe
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor
import org.neo4j.values.storable.Values

abstract class BaseRelationshipFulltextIndexSearchSlottedPipe(
  relOffset: Option[Int],
  fromOffset: Option[Int],
  toOffset: Option[Int],
  score: Option[Int],
  queryStringExpression: Expression,
  analyzerExpression: Option[Expression],
  skipExpression: Option[Expression],
  limitExpression: Expression,
  queryIndexId: Int
) extends RelationshipFulltextIndexSearchPipe(
      queryStringExpression,
      analyzerExpression,
      skipExpression,
      limitExpression,
      queryIndexId
    ) {

  private val relationshipWriter = Relationships.compileRelationshipWriter(relOffset, fromOffset, toOffset)

  private[this] val _newRow: (CypherRow, RelationshipFulltextSearchIterator) => CypherRow = {
    score match {
      case Some(value) =>
        (incomingRow: CypherRow, iterator: RelationshipFulltextSearchIterator) =>
          relationshipWriter.writeRow(incomingRow, iterator.reference, iterator)
          incomingRow.setRefAt(value, Values.floatValue(iterator.score))
          incomingRow
      case None => (incomingRow: CypherRow, iterator: RelationshipFulltextSearchIterator) =>
          relationshipWriter.writeRow(incomingRow, iterator.reference, iterator)
          incomingRow
    }
  }

  override protected def newRow(
    row: CypherRow,
    iterator: RelationshipFulltextSearchIterator
  ): CypherRow = _newRow(row, iterator)
}

case class DirectedRelationshipFulltextIndexSearchSlottedPipe(
  relOffset: Option[Int],
  fromOffset: Option[Int],
  toOffset: Option[Int],
  score: Option[Int],
  queryStringExpression: Expression,
  analyzerExpression: Option[Expression],
  skipExpression: Option[Expression],
  limitExpression: Expression,
  queryIndexId: Int
)(val id: Id = Id.INVALID_ID) extends BaseRelationshipFulltextIndexSearchSlottedPipe(
      relOffset,
      fromOffset,
      toOffset,
      score,
      queryStringExpression,
      analyzerExpression,
      skipExpression,
      limitExpression,
      queryIndexId
    ) {

  override protected def iteratorFrom(cursor: RelationshipValueIndexCursor): RelationshipFulltextSearchIterator = {
    if (fromOffset.nonEmpty || toOffset.nonEmpty) {
      new RelationshipFulltextSearchIterator(cursor)
    } else {
      new NonStoreAccessingFulltextSearchIterator(cursor)
    }
  }
}

case class UndirectedRelationshipFulltextIndexSearchSlottedPipe(
  relOffset: Option[Int],
  fromOffset: Option[Int],
  toOffset: Option[Int],
  score: Option[Int],
  queryStringExpression: Expression,
  analyzerExpression: Option[Expression],
  skipExpression: Option[Expression],
  limitExpression: Expression,
  queryIndexId: Int
)(val id: Id = Id.INVALID_ID) extends BaseRelationshipFulltextIndexSearchSlottedPipe(
      relOffset,
      fromOffset,
      toOffset,
      score,
      queryStringExpression,
      analyzerExpression,
      skipExpression,
      limitExpression,
      queryIndexId
    ) {

  override protected def iteratorFrom(cursor: RelationshipValueIndexCursor): RelationshipFulltextSearchIterator =
    new UndirectedRelationshipFulltextSearchIterator(cursor)
}
