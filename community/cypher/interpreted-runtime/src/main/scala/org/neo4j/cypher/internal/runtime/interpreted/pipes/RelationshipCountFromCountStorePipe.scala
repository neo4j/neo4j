/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.opencypher.v9_0.util.NameId
import org.opencypher.v9_0.util.attribution.Id
import org.neo4j.values.storable.Values

case class RelationshipCountFromCountStorePipe(ident: String, startLabel: Option[LazyLabel],
                                               typeNames: LazyTypes, endLabel: Option[LazyLabel])
                                              (val id: Id = Id.INVALID_ID) extends Pipe {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val maybeStartLabelId = getLabelId(startLabel, state)
    val maybeEndLabelId = getLabelId(endLabel, state)

    val count = (maybeStartLabelId, maybeEndLabelId) match {
      case (Some(startLabelId), Some(endLabelId)) =>
        countOneDirection(state, typeNames, startLabelId, endLabelId)

      // If any of the specified labels does not exist the count is zero
      case _ =>
        0
    }

    val baseContext = state.createOrGetInitialContext(executionContextFactory)
    Seq(baseContext.set(ident, Values.longValue(count))).iterator
  }

  private def getLabelId(lazyLabel: Option[LazyLabel], state: QueryState): Option[Int] = lazyLabel match {
      case Some(label) => label.getOptId(state.query).map(_.id)
      case _ => Some(NameId.WILDCARD)
    }

  private def countOneDirection(state: QueryState, typeNames: LazyTypes, startLabelId: Int, endLabelId: Int) =
    typeNames.types(state.query) match {
      case None => state.query.relationshipCountByCountStore(startLabelId, NameId.WILDCARD, endLabelId)
      case Some(types) => types.foldLeft(0L) { (count, typeId) =>
        count + state.query.relationshipCountByCountStore(startLabelId, typeId, endLabelId)
      }
    }
}
