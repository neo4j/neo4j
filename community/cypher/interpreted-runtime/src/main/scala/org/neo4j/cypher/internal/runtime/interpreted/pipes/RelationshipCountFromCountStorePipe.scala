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

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.util.NameId
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.storable.Values

case class RelationshipCountFromCountStorePipe(
  ident: String,
  startLabel: Option[LazyLabel],
  types: RelationshipTypes,
  endLabel: Option[LazyLabel]
)(val id: Id = Id.INVALID_ID) extends Pipe {

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val maybeStartLabelId = getLabelId(startLabel, state)
    val maybeEndLabelId = getLabelId(endLabel, state)

    val count = (maybeStartLabelId, maybeEndLabelId) match {
      case (Some(startLabelId), Some(endLabelId)) =>
        countOneDirection(state, startLabelId, endLabelId)

      // If any of the specified labels does not exist the count is zero
      case _ =>
        0
    }

    val baseContext = state.newRowWithArgument(rowFactory)
    baseContext.set(ident, Values.longValue(count))
    ClosingIterator.single(baseContext)
  }

  private def getLabelId(lazyLabel: Option[LazyLabel], state: QueryState): Option[Int] = lazyLabel match {
    case Some(label) =>
      val id = label.getId(state.query)
      if (id == LazyLabel.UNKNOWN) None
      else Some(id)
    case _ => Some(NameId.WILDCARD)
  }

  private def countOneDirection(state: QueryState, startLabelId: Int, endLabelId: Int) = {
    val ts = types.types(state.query)
    if (ts == null) state.query.relationshipCountByCountStore(startLabelId, NameId.WILDCARD, endLabelId)
    else {
      var i = 0
      var count = 0L
      while (i < ts.length) {
        count += state.query.relationshipCountByCountStore(startLabelId, ts(i), endLabelId)
        i += 1
      }
      count
    }
  }
}
