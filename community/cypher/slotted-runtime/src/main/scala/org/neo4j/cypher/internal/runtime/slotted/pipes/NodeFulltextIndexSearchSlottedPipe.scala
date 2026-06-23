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
import org.neo4j.cypher.internal.runtime.interpreted.pipes.BaseNodeFulltextIndexSearchPipe
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.values.storable.Values

case class NodeFulltextIndexSearchSlottedPipe(
  offset: Int,
  score: Option[Int],
  queryStringExpression: Expression,
  analyzerExpression: Option[Expression],
  skipExpression: Option[Expression],
  limitExpression: Expression,
  queryIndexId: Int
)(val id: Id = Id.INVALID_ID)
    extends BaseNodeFulltextIndexSearchPipe(
      queryStringExpression,
      analyzerExpression,
      skipExpression,
      limitExpression,
      queryIndexId
    ) {

  private[this] val withScore: Boolean = score.isDefined
  private[this] val scoreOffset: Int = score.getOrElse(-1)

  override protected def newRow(row: CypherRow, cursor: NodeValueIndexCursor): CypherRow = {
    row.setLongAt(offset, cursor.nodeReference())
    if (withScore) row.setRefAt(scoreOffset, Values.floatValue(cursor.score()))
    row
  }
}
